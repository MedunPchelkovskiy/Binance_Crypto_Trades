# tests/test_clickhouse_consumer_batching.py
from decimal import Decimal
from datetime import datetime, timezone

from consumers.click_house_silver_consumer import record_to_row, COLUMN_NAMES


def make_record(**overrides):
    """Builds a valid deserialized Avro record dict (raw Binance field names),
    with optional overrides."""
    base = {
        "e": "aggTrade",
        "E": 1789215829640,
        "s": "BNBUSDT",
        "a": 857488061,
        "p": "736.01000000",
        "q": "0.52400000",
        "f": 1584058807,
        "l": 1584058807,
        "T": 1789215829639,
        "m": False,
        "M": True,
    }
    base.update(overrides)
    return base


# --- Normal case: record maps to a transformed Silver row ---
def test_record_to_row_normal_case():
    record = make_record()
    row = record_to_row(record)

    assert row[0] == "aggTrade"
    assert row[1] == datetime.fromtimestamp(1789215829640 / 1000, tz=timezone.utc)
    assert row[2] == "BNBUSDT"
    assert row[3] == 857488061
    assert row[4] == Decimal("736.01000000")
    assert row[5] == Decimal("0.52400000")
    assert row[6] == 1584058807
    assert row[7] == 1584058807
    assert row[8] == datetime.fromtimestamp(1789215829639 / 1000, tz=timezone.utc)
    assert row[9] is False
    assert row[10] is True
    assert len(row) == len(COLUMN_NAMES)


# --- Edge case: missing raw field raises KeyError ---
def test_record_to_row_missing_field_raises():
    record = make_record()
    del record["p"]

    try:
        record_to_row(record)
        assert False, "Expected KeyError but none was raised"
    except KeyError:
        pass


# --- Edge case: zero and empty-string decimal values are preserved, not skipped ---
def test_record_to_row_zero_and_empty_values():
    record = make_record(a=0, q="0.00000000")
    row = record_to_row(record)

    assert row[COLUMN_NAMES.index("agg_trade_id")] == 0
    assert row[COLUMN_NAMES.index("quantity")] == Decimal("0.00000000")


# --- Edge case: price/quantity with unusual precision are not rounded or truncated ---
def test_record_to_row_preserves_decimal_precision():
    record = make_record(p="0.00000001", q="123456789.12345678")
    row = record_to_row(record)

    assert row[COLUMN_NAMES.index("price")] == Decimal("0.00000001")
    assert row[COLUMN_NAMES.index("quantity")] == Decimal("123456789.12345678")


# --- Edge case: boolean fields stored as 0/1 (int) instead of True/False ---
def test_record_to_row_boolean_as_int():
    record = make_record(m=1, M=0)
    row = record_to_row(record)

    assert row[COLUMN_NAMES.index("is_buyer_maker")] is True
    assert row[COLUMN_NAMES.index("is_best_match")] is False


# --- Edge case: invalid decimal string raises decimal.InvalidOperation ---
def test_record_to_row_invalid_price_raises():
    from decimal import InvalidOperation
    record = make_record(p="not-a-number")

    try:
        record_to_row(record)
        assert False, "Expected InvalidOperation but none was raised"
    except InvalidOperation:
        pass