from unittest.mock import MagicMock

import pytest

from airflow.dags import backfill_logic


def fresh_state(**overrides):
    state = {
        "server_offset": 0.0,
        "last_sync": None,
        "window": 100,
        "reserved_weight": 20,
        "reported_weight": 30,
        "pending_weight": 12,
    }
    state.update(overrides)
    return state


def time_response(server_time_ms, headers):
    response = MagicMock()
    response.json.return_value = {"serverTime": server_time_ms}
    response.headers = headers
    return response


def test_sync_preserves_pending_weight_in_same_window(monkeypatch):
    state = fresh_state()
    monkeypatch.setattr(backfill_logic, "rate_limit_state", state)
    response = time_response(100 * 60 * 1000 + 1000, {"X-MBX-USED-WEIGHT-1M": "35"})

    backfill_logic.sync_binance_server_time(
        http_get=lambda *a, **k: response,
        clock=lambda: 6001.0,
    )

    assert state["pending_weight"] == 12
    assert state["reported_weight"] == 35
    assert state["reserved_weight"] == 20


def test_sync_resets_pending_weight_on_new_window(monkeypatch):
    state = fresh_state()
    monkeypatch.setattr(backfill_logic, "rate_limit_state", state)
    response = time_response(101 * 60 * 1000 + 1000, {"X-MBX-USED-WEIGHT-1M": "5"})

    backfill_logic.sync_binance_server_time(
        http_get=lambda *a, **k: response,
        clock=lambda: 6061.0,
    )

    assert state["pending_weight"] == 0
    assert state["reserved_weight"] == 0
    assert state["reported_weight"] == 5


def test_sync_reads_lowercase_weight_header(monkeypatch):
    state = fresh_state(reported_weight=0)
    monkeypatch.setattr(backfill_logic, "rate_limit_state", state)
    response = time_response(100 * 60 * 1000 + 1000, {"x-mbx-used-weight-1m": "7"})

    backfill_logic.sync_binance_server_time(http_get=lambda *a, **k: response, clock=lambda: 6001.0)

    assert state["reported_weight"] == 7


def test_sync_stores_offset_to_server_time(monkeypatch):
    state = fresh_state()
    monkeypatch.setattr(backfill_logic, "rate_limit_state", state)
    response = time_response(10_000_000, {})  # server says 10 000 s

    backfill_logic.sync_binance_server_time(http_get=lambda *a, **k: response, clock=lambda: 9_990.0)

    assert state["server_offset"] == pytest.approx(10.0)


def test_update_with_header_replaces_reservation_with_reported_weight(monkeypatch):
    state = fresh_state(reported_weight=10, pending_weight=4)
    monkeypatch.setattr(backfill_logic, "rate_limit_state", state)

    backfill_logic.update_request_weight({"x-mbx-used-weight-1m": "50"})

    assert state["reported_weight"] == 50
    assert state["pending_weight"] == 0


def test_update_without_header_estimates_cost(monkeypatch):
    state = fresh_state(reported_weight=10, pending_weight=4)
    monkeypatch.setattr(backfill_logic, "rate_limit_state", state)

    backfill_logic.update_request_weight({})

    assert state["reported_weight"] == 10 + backfill_logic.AGG_TRADES_WEIGHT
    assert state["pending_weight"] == 0


def test_release_never_goes_below_zero(monkeypatch):
    state = fresh_state(pending_weight=1)
    monkeypatch.setattr(backfill_logic, "rate_limit_state", state)

    backfill_logic.release_request_weight(4)

    assert state["pending_weight"] == 0


def test_acquire_reserves_weight_inside_budget():
    backfill_logic.acquire_request_weight(
        4,
        sleep=lambda s: None,
        clock=lambda: 6001.0,
        http_get=lambda *a, **k: time_response(6_001_000, {"X-MBX-USED-WEIGHT-1M": "10"}),
    )
    state = backfill_logic.rate_limit_state
    assert state["reported_weight"] == 10
    assert state["pending_weight"] == 4
    assert state["reserved_weight"] == 4


def test_acquire_raises_timeout_if_next_window_is_beyond_deadline():
    full = time_response(6_001_000, {"X-MBX-USED-WEIGHT-1M": str(backfill_logic.REQUEST_WEIGHT_BUDGET)})
    with pytest.raises(TimeoutError):
        backfill_logic.acquire_request_weight(
            4,
            sleep=lambda s: None,
            clock=lambda: 6001.0,
            http_get=lambda *a, **k: full,
            deadline=10,
            mono=lambda: 0,
        )
