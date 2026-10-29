# tests/unit/test_on_trade_message.py
from unittest.mock import MagicMock, patch

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
    mock_producer = MagicMock()
    mock_serializer = MagicMock(return_value=b"fake-bytes")

    producer_module.on_trade_message(
        FakeTradeData(valid_trade_dict()),
        avro_serializer=mock_serializer,
        producer=mock_producer,
    )

    mock_producer.produce.assert_called_once()
    call_kwargs = mock_producer.produce.call_args.kwargs
    assert call_kwargs["topic"] == "trade_streams_avro_dev"
    assert call_kwargs["key"] == "857488061"


# --- Edge case: invalid trade data increments validation_errors_total, no produce call ---
def test_on_trade_message_invalid_trade_skips_produce():
    mock_producer = MagicMock()
    mock_serializer = MagicMock()
    with patch.object(producer_module, "validation_errors_total") as mock_counter:

        producer_module.on_trade_message(
            FakeTradeData({"s": "BNBUSDT"}),  # missing required fields
            avro_serializer=mock_serializer,
            producer=mock_producer,
        )

        mock_producer.produce.assert_not_called()
        mock_serializer.assert_not_called()
        mock_counter.inc.assert_called_once()


# --- Edge case: serialization failure increments serialization_errors_total, no produce call ---
def test_on_trade_message_serialization_error_skips_produce():
    mock_producer = MagicMock()
    mock_serializer = MagicMock(side_effect=Exception("boom"))
    with patch.object(producer_module, "serialization_errors_total") as mock_counter:

        producer_module.on_trade_message(
            FakeTradeData(valid_trade_dict()),
            avro_serializer=mock_serializer,
            producer=mock_producer,
        )

        mock_producer.produce.assert_not_called()
        mock_counter.inc.assert_called_once()


# --- Edge case: pending_trades gets the correct key registered on success ---
def test_on_trade_message_registers_pending_trade():
    producer_module.pending_trades.clear()

    producer_module.on_trade_message(
        FakeTradeData(valid_trade_dict()),
        avro_serializer=MagicMock(return_value=b"fake-bytes"),
        producer=MagicMock(),
    )

    assert 857488061 in producer_module.pending_trades


# --- Regression: pending_trades must NOT leak an entry when serialization fails ---
def test_on_trade_message_no_pending_leak_on_serialization_error():
    producer_module.pending_trades.clear()
    with patch.object(producer_module, "serialization_errors_total"):

        producer_module.on_trade_message(
            FakeTradeData(valid_trade_dict()),
            avro_serializer=MagicMock(side_effect=Exception("boom")),
            producer=MagicMock(),
        )

    assert 857488061 not in producer_module.pending_trades