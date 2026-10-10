from unittest.mock import MagicMock

import airflow.dags.backfill_logic
from airflow.dags.backfill_logic import (
    compute_new_watermarks, count_ids, save_watermarks, verify_backfill, wait_for_silver,
)


def rng(symbol="BTCUSDT", f=1, t=10):
    return {"symbol": symbol, "from_id": f, "to_id": t}


def gap(f, t, symbol="BTCUSDT"):
    return {"symbol": symbol, "from_id": f, "to_id": t}


def test_count_ids():
    assert count_ids([gap(3, 5), gap(9, 9), gap(1, 2, "ETHUSDT")], "BTCUSDT") == 4


def test_watermark_moves_to_range_end_when_no_gaps():
    assert compute_new_watermarks([rng()], []) == {"BTCUSDT": 10}


def test_watermark_stops_before_first_remaining_gap():
    assert compute_new_watermarks([rng()], [gap(7, 8), gap(4, 5)]) == {"BTCUSDT": 3}


def test_no_progress_when_gap_at_range_start():
    assert compute_new_watermarks([rng(f=5, t=10)], [gap(5, 6)]) == {}


def test_symbols_are_independent():
    r = compute_new_watermarks([rng("A"), rng("B")], [gap(4, 4, "B")])
    assert r == {"A": 10, "B": 3}


def test_wait_stops_as_soon_as_gaps_are_gone(monkeypatch):
    monkeypatch.setattr(airflow.dags.backfill_logic, "find_gaps",
                        MagicMock(side_effect=[[gap(1, 2)], [gap(1, 1)], []]))
    sleep = MagicMock()
    assert wait_for_silver(MagicMock(), [rng()], sleep=sleep) == []
    assert sleep.call_count == 2


def test_wait_gives_up_after_max_attempts(monkeypatch):
    find = MagicMock(return_value=[gap(1, 2)])
    monkeypatch.setattr(airflow.dags.backfill_logic, "find_gaps", find)
    sleep = MagicMock()
    assert wait_for_silver(MagicMock(), [rng()], sleep=sleep, max_attempts=3) == [gap(1, 2)]
    assert find.call_count == 3 and sleep.call_count == 2


def test_wait_does_not_sleep_when_already_complete(monkeypatch):
    monkeypatch.setattr(airflow.dags.backfill_logic, "find_gaps", MagicMock(return_value=[]))
    sleep = MagicMock()
    wait_for_silver(MagicMock(), [rng()], sleep=sleep)
    sleep.assert_not_called()


def test_save_watermarks_computes_found_and_filled():
    c = MagicMock()
    rows = save_watermarks(c, {"BTCUSDT": 3}, [gap(3, 6)], [gap(5, 6)], "run1")
    assert rows == [["BTCUSDT", 3, "run1", 4, 2]]
    assert c.insert.call_args.args[0] == "trades.backfill_state"


def test_save_watermarks_empty_does_not_insert():
    c = MagicMock()
    save_watermarks(c, {}, [], [], "run1")
    c.insert.assert_not_called()


def test_verify_waits_only_when_something_was_published(monkeypatch):
    monkeypatch.setattr(airflow.dags.backfill_logic, "find_gaps", MagicMock(return_value=[gap(7, 8)]))
    sleep = MagicMock()
    c = MagicMock()
    res = verify_backfill(c, [rng()], [gap(7, 8)], {"published": 0}, "r", sleep=sleep)
    sleep.assert_not_called()
    assert res == {"watermarks": {"BTCUSDT": 6}, "remaining_gaps": 1}


def test_verify_full_success_moves_watermark_and_writes_state(monkeypatch):
    monkeypatch.setattr(airflow.dags.backfill_logic, "find_gaps", MagicMock(return_value=[]))
    c = MagicMock()
    res = verify_backfill(c, [rng()], [gap(4, 5)], {"published": 2}, "r", sleep=MagicMock())
    assert res == {"watermarks": {"BTCUSDT": 10}, "remaining_gaps": 0}
    assert c.insert.call_args.args[1] == [["BTCUSDT", 10, "r", 2, 2]]