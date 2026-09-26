# tests/test_on_trade_message.py
from unittest.mock import MagicMock, patch
import pytest

from ingestion import producer as producer_module


class FakeTradeData:
    """Mimics the raw data object passed into on_trade_message (has .to_dict())."""
    def __init__(self, data_dict):
        self._data = data_dict

    def to_dict(self):
        return self._data


def valid_trade_dict():
    return {
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


# --- Normal case: valid trade goes through validation + produce ---
def test_on_trade_message_valid_trade_calls_produce():
    with patch.object(producer_module, "producer") as mock_producer, \
         patch.object(producer_module, "avro_serializer", return_value=b"fake-bytes"):

        data = FakeTradeData(valid_trade_dict())
        producer_module.on_trade_message(data)

        mock_producer.produce.assert_called_once()
        call_kwargs = mock_producer.produce.call_args.kwargs
        assert call_kwargs["topic"] == "trade_streams_avro_dev"
        assert call_kwargs["key"] == "857488061"


# --- Edge case: invalid trade data increments validation_errors_total, no produce call ---
def test_on_trade_message_invalid_trade_skips_produce():
    with patch.object(producer_module, "producer") as mock_producer, \
         patch.object(producer_module, "validation_errors_total") as mock_counter:

        bad_data = FakeTradeData({"s": "BNBUSDT"})  # missing required fields
        producer_module.on_trade_message(bad_data)

        mock_producer.produce.assert_not_called()
        mock_counter.inc.assert_called_once()


# --- Edge case: serialization failure increments serialization_errors_total, no produce call ---
def test_on_trade_message_serialization_error_skips_produce():
    with patch.object(producer_module, "producer") as mock_producer, \
         patch.object(producer_module, "avro_serializer", side_effect=Exception("boom")), \
         patch.object(producer_module, "serialization_errors_total") as mock_counter:

        data = FakeTradeData(valid_trade_dict())
        producer_module.on_trade_message(data)

        mock_producer.produce.assert_not_called()
        mock_counter.inc.assert_called_once()


# --- Edge case: pending_trades gets the correct key registered on success ---
def test_on_trade_message_registers_pending_trade():
    with patch.object(producer_module, "producer"), \
         patch.object(producer_module, "avro_serializer", return_value=b"fake-bytes"):

        producer_module.pending_trades.clear()
        data = FakeTradeData(valid_trade_dict())
        producer_module.on_trade_message(data)

        assert 857488061 in producer_module.pending_trades


# --- Edge case: pending_trades does NOT leak an entry when serialization fails ---
# NOTE: this test currently documents the KNOWN BUG discussed earlier
# (pending_trades is set before avro_serializer is called).
# Once fixed, this test should assert the entry is absent.
def test_on_trade_message_no_pending_leak_on_serialization_error():
    with patch.object(producer_module, "producer"), \
         patch.object(producer_module, "avro_serializer", side_effect=Exception("boom")):

        producer_module.pending_trades.clear()
        data = FakeTradeData(valid_trade_dict())
        producer_module.on_trade_message(data)

        # This assertion will currently FAIL until the ordering bug is fixed —
        # keep it as a regression test once the fix is applied.
        assert 857488061 not in producer_module.pending_trades