from unittest.mock import MagicMock

import pytest
import requests

from airflow.dags import backfill_logic
from airflow.dags.backfill_logic import process_ranges, split_into_chunks
from ingestion.validation import Trade


# ---------- helpers ----------

def raw_trade(i, **overrides):
    t = {"a": i, "p": "1.0", "q": "1.0", "f": i, "l": i, "T": 1000, "m": False, "M": True}
    t.update(overrides)
    return t


def page_response(trades):
    resp = MagicMock()
    resp.status_code = 200
    resp.headers = {}
    resp.json.return_value = trades
    return resp


def ok_response(first_id, last_id):
    return page_response([raw_trade(i) for i in range(first_id, last_id + 1)])


def error_response(status, headers=None):
    resp = MagicMock()
    resp.status_code = status
    resp.headers = headers or {}
    resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
    return resp


def make_http_get(*responses):
    return MagicMock(side_effect=list(responses))


def make_producer():
    p = MagicMock()
    p.flush.return_value = 0
    return p


def client_with_existing(*ids):
    c = MagicMock()
    c.query.return_value = MagicMock(result_rows=[(i,) for i in ids])
    return c


def rng(symbol="BNBUSDT", f=1, t=10):
    return {"symbol": symbol, "from_id": f, "to_id": t}


def run(client, producer, ranges, http_get, chunk_size=10, **kw):
    return process_ranges(
        client, producer, lambda t: b"x", "topic", Trade, ranges, "run-1",
        http_get=http_get, sleep=lambda s: None, chunk_size=chunk_size, **kw,
    )


@pytest.fixture
def silver_complete(monkeypatch):
    """wait_for_silver reports that everything has arrived."""
    monkeypatch.setattr(backfill_logic, "wait_for_silver", lambda *a, **k: [])


# ---------- split_into_chunks ----------

def test_split_exact_multiple():
    assert split_into_chunks([rng(f=1, t=20)], 10) == [rng(f=1, t=10), rng(f=11, t=20)]


def test_split_last_chunk_is_shorter():
    assert split_into_chunks([rng(f=1, t=25)], 10) == [
        rng(f=1, t=10), rng(f=11, t=20), rng(f=21, t=25)
    ]


def test_split_range_smaller_than_chunk_stays_whole():
    assert split_into_chunks([rng(f=5, t=8)], 100) == [rng(f=5, t=8)]


def test_split_single_id():
    assert split_into_chunks([rng(f=7, t=7)], 10) == [rng(f=7, t=7)]


def test_split_orders_by_symbol_then_from_id():
    ranges = [rng("B", 1, 5), rng("A", 10, 12), rng("A", 1, 3)]
    assert split_into_chunks(ranges, 100) == [rng("A", 1, 3), rng("A", 10, 12), rng("B", 1, 5)]


def test_split_empty():
    assert split_into_chunks([], 10) == []


# ---------- process_ranges: happy paths ----------

def test_chunk_without_gaps_moves_watermark_and_makes_no_http_calls():
    client, producer, http = client_with_existing(*range(1, 11)), make_producer(), make_http_get()

    stats = run(client, producer, [rng(f=1, t=10)], http)

    http.assert_not_called()
    producer.produce.assert_not_called()
    assert stats["chunks_done"] == 1 and stats["gaps_found"] == 0
    assert client.insert.call_args.args[1] == [["BNBUSDT", 10, "run-1", 0, 0]]


def test_gap_is_fetched_published_and_watermark_moved(silver_complete):
    client, producer = MagicMock(), make_producer()  # MagicMock client: no ids exist

    stats = run(client, producer, [rng(f=1, t=10)], make_http_get(ok_response(1, 10)))

    assert stats["published"] == 10 and stats["remaining_gaps"] == 0
    assert producer.produce.call_count == 10
    assert client.insert.call_args.args[1] == [["BNBUSDT", 10, "run-1", 10, 10]]


def test_multiple_chunks_processed_in_ascending_order(silver_complete):
    client, producer = MagicMock(), make_producer()
    http = make_http_get(ok_response(1, 10), ok_response(11, 20))

    stats = run(client, producer, [rng(f=1, t=20)], http)

    assert stats["chunks_done"] == 2 and stats["published"] == 20
    assert http.call_args_list[0].kwargs["params"]["fromId"] == 1
    assert http.call_args_list[1].kwargs["params"]["fromId"] == 11


# ---------- process_ranges: unfilled gaps block the symbol ----------

def test_unfilled_gap_blocks_later_chunks_of_same_symbol_only(monkeypatch):
    def wait(client, window, *a, **k):
        # BNB chunk 1 stays incomplete, everything else arrives
        if window[0]["symbol"] == "BNBUSDT":
            return [{"symbol": "BNBUSDT", "from_id": 5, "to_id": 10}]
        return []

    monkeypatch.setattr(backfill_logic, "wait_for_silver", wait)
    client, producer = MagicMock(), make_producer()
    http = make_http_get(ok_response(1, 10), ok_response(1, 10))

    stats = run(client, producer, [rng("BNBUSDT", 1, 20), rng("BTCUSDT", 1, 10)], http)

    # BNB chunk 1 + BTC chunk 1; BNB chunk 2 is skipped
    assert stats["chunks_done"] == 2
    assert http.call_count == 2
    rows = [r for call in client.insert.call_args_list for r in call.args[1]]
    symbols_written = {r[0]: r[1] for r in rows}
    assert symbols_written == {"BNBUSDT": 4, "BTCUSDT": 10}  # BNB stops before its gap


# ---------- process_ranges: validation ----------

def test_invalid_trade_is_counted_rejected_and_not_published(monkeypatch):
    monkeypatch.setattr(backfill_logic, "wait_for_silver",
                        lambda *a, **k: [{"symbol": "BNBUSDT", "from_id": 2, "to_id": 2}])
    client, producer = MagicMock(), make_producer()
    page = page_response([raw_trade(1), raw_trade(2, m="false"), raw_trade(3)])  # StrictBool

    stats = run(client, producer, [rng(f=1, t=3)], make_http_get(page))

    assert stats["published"] == 2 and stats["rejected"] == 1
    assert producer.produce.call_count == 2
    assert stats["remaining_gaps"] == 1
    assert client.insert.call_args.args[1][0][1] == 1  # watermark stops before id 2


def test_all_trades_invalid_never_moves_watermark(monkeypatch):
    def must_not_wait(*a, **k):
        raise AssertionError("wait_for_silver must be skipped when nothing was published")

    monkeypatch.setattr(backfill_logic, "wait_for_silver", must_not_wait)
    client, producer = MagicMock(), make_producer()
    page = page_response([raw_trade(1, m="false")])

    stats = run(client, producer, [rng(f=1, t=1)], make_http_get(page))

    assert stats["rejected"] == 1 and stats["published"] == 0
    assert stats["remaining_gaps"] == 1
    client.insert.assert_not_called()


# ---------- process_ranges: time limit ----------

def test_stops_between_chunks_when_runtime_exceeded(silver_complete):
    client, producer = MagicMock(), make_producer()
    clock = MagicMock(side_effect=[0, 0, 10_000])  # start, chunk 1 check, chunk 2 check

    stats = run(client, producer, [rng(f=1, t=20)], make_http_get(ok_response(1, 10)),
                clock=clock, max_runtime_sec=100)

    assert stats["stopped_by_time"] is True
    assert stats["chunks_done"] == 1


# ---------- process_ranges: rate limit / errors ----------

def test_429_on_first_request_makes_no_progress():
    client, producer = MagicMock(), make_producer()

    stats = run(client, producer, [rng(f=1, t=100)],
                make_http_get(error_response(429, {"Retry-After": "30"})), chunk_size=100)

    assert stats["rate_limited"] is True
    assert stats["retry_after_sec"] == "30"
    assert stats["chunks_done"] == 0
    producer.produce.assert_not_called()
    client.insert.assert_not_called()


def test_429_on_second_chunk_keeps_first_chunk_progress(silver_complete):
    client, producer = MagicMock(), make_producer()
    http = make_http_get(ok_response(1, 10), error_response(429))

    stats = run(client, producer, [rng(f=1, t=20)], http)

    assert stats["chunks_done"] == 1 and stats["published"] == 10
    assert stats["rate_limited"] is True
    client.insert.assert_called_once()  # watermark only for chunk 1


def test_no_rate_limit_flag_on_success(silver_complete):
    stats = run(MagicMock(), make_producer(), [rng(f=1, t=10)], make_http_get(ok_response(1, 10)))
    assert stats["rate_limited"] is False and stats["stopped_by_time"] is False


def test_non_rate_limit_http_error_propagates():
    client = MagicMock()
    with pytest.raises(requests.HTTPError):
        run(client, make_producer(), [rng(f=1, t=10)], make_http_get(error_response(500)))
    client.insert.assert_not_called()