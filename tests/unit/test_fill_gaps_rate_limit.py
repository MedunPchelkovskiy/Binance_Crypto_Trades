from unittest.mock import MagicMock

import requests

from airflow.dags import backfill_logic
from airflow.dags.backfill_logic import fill_gaps
from ingestion.validation import Trade


def ok_response(first_id, last_id):
    """Фалшив 200 отговор на Binance с трейдове first_id..last_id."""
    resp = MagicMock()
    resp.status_code = 200
    resp.headers = {}
    resp.json.return_value = [
        {"a": i, "p": "1.0", "q": "1.0", "f": i, "l": i,
         "T": 1000, "m": False, "M": True}
        for i in range(first_id, last_id + 1)
    ]
    return resp


def rate_limited_response():
    """Фалшив 429 отговор с Retry-After."""
    resp = MagicMock()
    resp.status_code = 429
    resp.headers = {"Retry-After": "30"}
    resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
    return resp


def make_http_get(responses):
    """http_get, който връща отговорите един по един."""
    it = iter(responses)
    return lambda url, params, timeout: next(it)


def make_producer():
    producer = MagicMock()
    producer.flush.return_value = 0  # 0 = няма неизпратени съобщения
    return producer


def run_fill_gaps(client, producer, gaps, http_get, chunk_size):
    return fill_gaps(
        client, producer, lambda t: b"x", "topic", Trade, gaps, "run-1",
        http_get=http_get, sleep=lambda s: None, chunk_size=chunk_size,
    )


# --- 429 още на първата заявка: нищо не се публикува, watermark не се пише ---
def test_fill_gaps_stops_on_429_before_any_progress():
    client, producer = MagicMock(), make_producer()
    gaps = [{"symbol": "BNBUSDT", "from_id": 1, "to_id": 100}]

    stats = run_fill_gaps(client, producer, gaps,
                          make_http_get([rate_limited_response()]), chunk_size=100)

    assert stats["rate_limited"] is True
    assert stats["retry_after_sec"] == "30"
    assert stats["chunks_done"] == 0
    producer.produce.assert_not_called()
    client.insert.assert_not_called()


# --- 429 на втория chunk: първият е записан, вторият не мести watermark ---
def test_fill_gaps_keeps_first_chunk_progress_when_second_is_rate_limited(monkeypatch):
    monkeypatch.setattr(backfill_logic, "wait_for_silver", lambda *a, **k: [])
    client, producer = MagicMock(), make_producer()
    gaps = [{"symbol": "BNBUSDT", "from_id": 1, "to_id": 20}]

    stats = run_fill_gaps(
        client, producer, gaps,
        make_http_get([ok_response(1, 10), rate_limited_response()]),
        chunk_size=10,
    )

    assert stats["chunks_done"] == 1
    assert stats["published"] == 10
    assert stats["rate_limited"] is True
    client.insert.assert_called_once()  # watermark само за първия chunk


# --- Без 429 флагът остава False ---
def test_fill_gaps_not_rate_limited_on_success(monkeypatch):
    monkeypatch.setattr(backfill_logic, "wait_for_silver", lambda *a, **k: [])
    client, producer = MagicMock(), make_producer()
    gaps = [{"symbol": "BNBUSDT", "from_id": 1, "to_id": 10}]

    stats = run_fill_gaps(client, producer, gaps,
                          make_http_get([ok_response(1, 10)]), chunk_size=10)

    assert stats["rate_limited"] is False
    assert stats["chunks_done"] == 1