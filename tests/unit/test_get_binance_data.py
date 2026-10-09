from unittest.mock import MagicMock

import pytest

from airflow.dags.backfill_logic import fetch_gap_trades, get_binance_data, to_trade


def raw(a):
    return {"a": a, "p": "1.0", "q": "2.0", "f": a, "l": a, "T": 1000 + a,
            "m": False, "M": True}


def fake_get(pages):
    """Returns an http_get that serves the given pages one by one."""
    responses = []
    for page in pages:
        r = MagicMock()
        r.json.return_value = page
        responses.append(r)
    return MagicMock(side_effect=responses)


def test_to_trade_adds_stream_fields():
    t = to_trade(raw(7), "BTCUSDT")
    assert t["e"] == "aggTrade"
    assert t["s"] == "BTCUSDT"
    assert t["E"] == t["T"] == 1007
    assert t["a"] == 7


def test_single_page_within_gap():
    get = fake_get([[raw(1), raw(2), raw(3)]])
    trades = fetch_gap_trades("BTCUSDT", 1, 3, http_get=get)
    assert [t["a"] for t in trades] == [1, 2, 3]


def test_pagination_uses_last_id_plus_one():
    get = fake_get([[raw(1), raw(2)], [raw(3), raw(4)]])
    trades = fetch_gap_trades("BTCUSDT", 1, 4, http_get=get)
    assert [t["a"] for t in trades] == [1, 2, 3, 4]
    assert get.call_args_list[1].kwargs["params"]["fromId"] == 3


def test_first_request_params():
    get = fake_get([[raw(5)]])
    fetch_gap_trades("ETHUSDT", 5, 5, http_get=get)
    params = get.call_args.kwargs["params"]
    assert params == {"symbol": "ETHUSDT", "fromId": 5, "limit": 1000}


def test_page_overshooting_to_id_is_truncated():
    get = fake_get([[raw(1), raw(2), raw(3), raw(4)]])
    trades = fetch_gap_trades("BTCUSDT", 1, 2, http_get=get)
    assert [t["a"] for t in trades] == [1, 2]
    assert get.call_count == 1


def test_empty_response_stops_loop():
    get = fake_get([[]])
    assert fetch_gap_trades("BTCUSDT", 1, 100, http_get=get) == []
    assert get.call_count == 1


def test_http_error_propagates():
    r = MagicMock()
    r.raise_for_status.side_effect = RuntimeError("429")
    with pytest.raises(RuntimeError, match="429"):
        fetch_gap_trades("BTCUSDT", 1, 10, http_get=MagicMock(return_value=r))


def test_max_trades_stops_between_pages():
    get = fake_get([[raw(1), raw(2)], [raw(3), raw(4)]])
    trades = fetch_gap_trades("BTCUSDT", 1, 100, http_get=get, max_trades=2)
    assert [t["a"] for t in trades] == [1, 2]
    assert get.call_count == 1


def test_get_binance_data_multiple_gaps_and_symbols():
    get = fake_get([[raw(1), raw(2)], [raw(10)]])
    gaps = [
        {"symbol": "BTCUSDT", "from_id": 1, "to_id": 2},
        {"symbol": "ETHUSDT", "from_id": 10, "to_id": 10},
    ]
    trades = get_binance_data(gaps, http_get=get)
    assert [(t["s"], t["a"]) for t in trades] == [
        ("BTCUSDT", 1), ("BTCUSDT", 2), ("ETHUSDT", 10)
    ]


def test_get_binance_data_budget_is_shared_across_gaps():
    get = fake_get([[raw(1), raw(2)]])
    gaps = [
        {"symbol": "A", "from_id": 1, "to_id": 100},
        {"symbol": "B", "from_id": 1, "to_id": 100},
    ]
    trades = get_binance_data(gaps, http_get=get, max_trades=2)
    assert len(trades) == 2
    assert get.call_count == 1  # second gap never requested


def test_get_binance_data_no_gaps_makes_no_requests():
    get = MagicMock()
    assert get_binance_data([], http_get=get) == []
    get.assert_not_called()