from unittest.mock import MagicMock

from airflow.dags.backfill_logic import find_gaps, find_missing_ranges


def test_no_gaps():
    assert find_missing_ranges([1, 2, 3, 4], 1, 4) == []


def test_inner_gap():
    assert find_missing_ranges([1, 2, 6, 7], 1, 7) == [(3, 5)]


def test_leading_gap():
    assert find_missing_ranges([5, 6], 3, 6) == [(3, 4)]


def test_trailing_gap():
    assert find_missing_ranges([1, 2], 1, 5) == [(3, 5)]


def test_multiple_gaps():
    assert find_missing_ranges([2, 5, 6, 9], 1, 10) == [(1, 1), (3, 4), (7, 8), (10, 10)]


def test_empty_ids_whole_range_is_gap():
    assert find_missing_ranges([], 10, 20) == [(10, 20)]


def test_duplicates_and_unsorted_input():
    assert find_missing_ranges([4, 1, 1, 2, 4], 1, 4) == [(3, 3)]


def test_ids_outside_range_are_ignored():
    assert find_missing_ranges([0, 1, 2, 99], 1, 3) == [(3, 3)]


def test_single_id_range_present_and_missing():
    assert find_missing_ranges([7], 7, 7) == []
    assert find_missing_ranges([], 7, 7) == [(7, 7)]


def test_find_gaps_wires_query_and_builds_dicts():
    client = MagicMock()
    client.query.return_value = MagicMock(result_rows=[(1,), (2,), (5,)])
    ranges = [{"symbol": "BTCUSDT", "from_id": 1, "to_id": 5}]

    assert find_gaps(client, ranges) == [{"symbol": "BTCUSDT", "from_id": 3, "to_id": 4}]
    params = client.query.call_args.kwargs["parameters"]
    assert params == {"symbol": "BTCUSDT", "from_id": 1, "to_id": 5}


def test_find_gaps_empty_ranges_makes_no_queries():
    client = MagicMock()
    assert find_gaps(client, []) == []
    client.query.assert_not_called()