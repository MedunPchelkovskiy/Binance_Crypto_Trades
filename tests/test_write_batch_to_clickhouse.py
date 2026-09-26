# tests/test_write_batch_to_clickhouse.py
from unittest.mock import patch
import pytest

from consumers import click_house_silver_consumer as consumer_module


def make_valid_record():
    return {
        "e": "aggTrade", "E": 1789215829640, "s": "BNBUSDT", "a": 1,
        "p": "736.01000000", "q": "0.52400000", "f": 1, "l": 1,
        "T": 1789215829639, "m": False, "M": True,
    }


# --- Normal case: batch is inserted with correct table and column names ---
def test_write_batch_success_calls_insert_with_correct_args():
    with patch.object(consumer_module, "clickhouse_client") as mock_client, \
         patch.object(consumer_module, "clickhouse_records_counter") as mock_counter, \
         patch.object(consumer_module, "batch_clickhouse_insert_duration") as mock_duration:

        records = [(make_valid_record(), 1789215829700)]

        consumer_module.write_batch_to_clickhouse(records)

        mock_client.insert.assert_called_once()
        call_args = mock_client.insert.call_args
        assert call_args.args[0] == consumer_module.CLICKHOUSE_TABLE
        assert call_args.kwargs["column_names"] == consumer_module.COLUMN_NAMES
        mock_counter.inc.assert_called_once_with(1)
        mock_duration.observe.assert_called_once()


# --- Edge case: empty batch — insert is still called with an empty rows list ---
# NOTE: this documents CURRENT behavior. clickhouse_client.insert([]) with an
# empty list may be a harmless no-op, or may raise depending on the client —
# worth verifying manually if an empty batch can ever reach this function.
def test_write_batch_empty_list_calls_insert_with_empty_rows():
    with patch.object(consumer_module, "clickhouse_client") as mock_client, \
         patch.object(consumer_module, "clickhouse_records_counter") as mock_counter, \
         patch.object(consumer_module, "batch_clickhouse_insert_duration"):

        consumer_module.write_batch_to_clickhouse([])

        mock_client.insert.assert_called_once()
        assert mock_client.insert.call_args.args[1] == []
        mock_counter.inc.assert_called_once_with(0)


# --- Edge case: insert raises an exception — propagates, metrics not recorded ---
def test_write_batch_insert_failure_propagates_and_skips_metrics():
    with patch.object(consumer_module, "clickhouse_client") as mock_client, \
         patch.object(consumer_module, "clickhouse_records_counter") as mock_counter, \
         patch.object(consumer_module, "batch_clickhouse_insert_duration") as mock_duration:

        mock_client.insert.side_effect = Exception("ClickHouse insert failed")
        records = [(make_valid_record(), 1789215829700)]

        with pytest.raises(Exception, match="ClickHouse insert failed"):
            consumer_module.write_batch_to_clickhouse(records)

        # NOTE: documents current behavior — since insert() raises before
        # reaching the counter/duration lines, neither metric is recorded
        # on a failed batch.
        mock_counter.inc.assert_not_called()
        mock_duration.observe.assert_not_called()


# --- Edge case: one malformed record fails the entire batch before insert is reached ---
def test_write_batch_one_malformed_record_fails_before_insert():
    with patch.object(consumer_module, "clickhouse_client") as mock_client, \
         patch.object(consumer_module, "clickhouse_records_counter") as mock_counter:

        good = (make_valid_record(), 1789215829700)
        bad = ({"e": "aggTrade"}, 1789215829700)  # missing most fields

        with pytest.raises(KeyError):
            consumer_module.write_batch_to_clickhouse([good, bad])

        mock_client.insert.assert_not_called()
        mock_counter.inc.assert_not_called()


# --- Edge case: multiple valid records — counter reflects correct batch size ---
def test_write_batch_multiple_records_counter_matches_batch_size():
    with patch.object(consumer_module, "clickhouse_client"), \
         patch.object(consumer_module, "clickhouse_records_counter") as mock_counter, \
         patch.object(consumer_module, "batch_clickhouse_insert_duration"):

        records = [
            (make_valid_record(), 1789215829700),
            (make_valid_record(), 1789215829800),
            (make_valid_record(), 1789215829900),
        ]

        consumer_module.write_batch_to_clickhouse(records)

        mock_counter.inc.assert_called_once_with(3)