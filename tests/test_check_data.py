from unittest.mock import MagicMock

from airflow.dags.backfill_logic import check_data, compute_check_ranges


def test_new_range_after_watermark():
    r = compute_check_ranges(["BTCUSDT"], {"BTCUSDT": (100, 500)}, {"BTCUSDT": 300})
    assert r == [{"symbol": "BTCUSDT", "from_id": 301, "to_id": 500}]


def test_first_run_starts_from_min_id():
    r = compute_check_ranges(["BTCUSDT"], {"BTCUSDT": (100, 500)}, {})
    assert r == [{"symbol": "BTCUSDT", "from_id": 100, "to_id": 500}]


def test_nothing_new_is_skipped():
    r = compute_check_ranges(["BTCUSDT"], {"BTCUSDT": (100, 300)}, {"BTCUSDT": 300})
    assert r == []


def test_symbol_without_silver_data_is_skipped():
    assert compute_check_ranges(["ETHUSDT"], {}, {}) == []


def test_symbols_are_independent():
    r = compute_check_ranges(
        ["A", "B"], {"A": (1, 10), "B": (1, 5)}, {"A": 10, "B": 2}
    )
    assert r == [{"symbol": "B", "from_id": 3, "to_id": 5}]


def test_check_data_uppercases_symbols_and_wires_queries():
    client = MagicMock()
    client.query.side_effect = [
        MagicMock(result_rows=[("BTCUSDT", 1, 50)]),  # bounds
        MagicMock(result_rows=[]),                     # watermarks
    ]
    r = check_data(client, ["btcusdt"])
    assert r == [{"symbol": "BTCUSDT", "from_id": 1, "to_id": 50}]
    assert client.query.call_args_list[0].kwargs["parameters"]["symbols"] == ["BTCUSDT"]