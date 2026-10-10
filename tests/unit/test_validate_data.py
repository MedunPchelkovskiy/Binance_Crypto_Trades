from airflow.dags.backfill_logic import validate_trades

from ingestion.validation import Trade


def good(a=1):
    return {"e": "aggTrade", "E": 1000, "s": "BTCUSDT", "a": a, "p": "1.0",
            "q": "2.0", "f": a, "l": a, "T": 1000, "m": False, "M": True}


def test_all_valid():
    valid, invalid = validate_trades([good(1), good(2)], Trade)
    assert [t["a"] for t in valid] == [1, 2]
    assert invalid == []


def test_invalid_trade_is_separated_with_error():
    bad = good(2)
    bad["a"] = "not-an-int"
    valid, invalid = validate_trades([good(1), bad], Trade)
    assert [t["a"] for t in valid] == [1]
    assert invalid[0]["trade"] is bad
    assert "a" in invalid[0]["error"]


def test_missing_field_is_invalid():
    bad = good(3)
    del bad["p"]
    valid, invalid = validate_trades([bad], Trade)
    assert valid == [] and len(invalid) == 1


def test_string_boolean_is_rejected():
    bad = good(4)
    bad["m"] = "false"  # StrictBool must not coerce
    valid, invalid = validate_trades([bad], Trade)
    assert valid == [] and len(invalid) == 1


def test_empty_input():
    assert validate_trades([], Trade) == ([], [])


def test_output_is_plain_dict_for_xcom():
    valid, _ = validate_trades([good(1)], Trade)
    assert type(valid[0]) is dict
