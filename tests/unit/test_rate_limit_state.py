from unittest.mock import MagicMock

from airflow.dags import backfill_logic


def test_sync_preserves_pending_weight_in_same_window(monkeypatch):
    state = {
        "server_offset": 0.0,
        "last_sync": None,
        "window": 100,
        "reserved_weight": 20,
        "reported_weight": 30,
        "pending_weight": 12,
    }

    monkeypatch.setattr(backfill_logic, "rate_limit_state", state)

    response = MagicMock()
    response.json.return_value = {"serverTime": 100 * 60 * 1000 + 1000}
    response.headers = {"X-MBX-USED-WEIGHT-1M": "35"}

    backfill_logic.sync_binance_server_time(
        http_get=lambda *a, **k: response,
        clock=lambda: 6001.0,
    )

    assert state["pending_weight"] == 12
    assert state["reported_weight"] == 35
    assert state["reserved_weight"] == 20


def test_sync_resets_pending_weight_on_new_window(monkeypatch):
    state = {
        "server_offset": 0.0,
        "last_sync": None,
        "window": 100,
        "reserved_weight": 20,
        "reported_weight": 30,
        "pending_weight": 12,
    }

    monkeypatch.setattr(backfill_logic, "rate_limit_state", state)

    response = MagicMock()
    response.json.return_value = {"serverTime": 101 * 60 * 1000 + 1000}
    response.headers = {"X-MBX-USED-WEIGHT-1M": "5"}

    backfill_logic.sync_binance_server_time(
        http_get=lambda *a, **k: response,
        clock=lambda: 6061.0,
    )

    assert state["pending_weight"] == 0
    assert state["reserved_weight"] == 0
    assert state["reported_weight"] == 5
