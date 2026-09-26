# tests/test_delivery_report.py
from unittest.mock import MagicMock, patch
import time

from ingestion import producer as producer_module


def make_fake_msg(key, topic="trade_streams_avro_dev", partition=0, offset=123):
    """Fake Kafka message object, mimicking what confluent_kafka passes
    to the delivery callback."""
    msg = MagicMock()
    msg.key.return_value = key.encode()
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    return msg


# --- Normal case: successful delivery ---
def test_delivery_report_success_increments_delivered_counter():
    with patch.object(producer_module, "kafka_delivered_total") as mock_delivered, \
         patch.object(producer_module, "kafka_delivery_errors_total") as mock_errors, \
         patch.object(producer_module, "kafka_produce_latency_seconds") as mock_latency:

        producer_module.pending_trades[123] = time.monotonic()
        msg = make_fake_msg(key="123")

        producer_module.delivery_report(None, msg)

        mock_delivered.inc.assert_called_once()
        mock_errors.inc.assert_not_called()
        mock_latency.observe.assert_called_once()


# --- Edge case: failed delivery ---
def test_delivery_report_failure_increments_error_counter():
    with patch.object(producer_module, "kafka_delivered_total") as mock_delivered, \
         patch.object(producer_module, "kafka_delivery_errors_total") as mock_errors, \
         patch.object(producer_module, "kafka_produce_latency_seconds") as mock_latency:

        producer_module.pending_trades[456] = time.monotonic()
        msg = make_fake_msg(key="456")

        producer_module.delivery_report("some Kafka error", msg)

        mock_errors.inc.assert_called_once()
        mock_delivered.inc.assert_not_called()
        # latency is still recorded even on failure, per current implementation
        mock_latency.observe.assert_called_once()


# --- Edge case: pending_trades entry is removed after delivery report, success or failure ---
def test_delivery_report_removes_pending_entry():
    with patch.object(producer_module, "kafka_delivered_total"), \
         patch.object(producer_module, "kafka_delivery_errors_total"), \
         patch.object(producer_module, "kafka_produce_latency_seconds"):

        producer_module.pending_trades[789] = time.monotonic()
        msg = make_fake_msg(key="789")

        producer_module.delivery_report(None, msg)

        assert 789 not in producer_module.pending_trades


# --- Edge case: KeyError if agg_trade_id was never registered in pending_trades ---
# This documents current behavior — delivery_report assumes the key always
# exists via pending_trades.pop(agg_trade_id) without a default.
def test_delivery_report_missing_pending_entry_raises_keyerror():
    with patch.object(producer_module, "kafka_delivered_total"), \
         patch.object(producer_module, "kafka_delivery_errors_total"), \
         patch.object(producer_module, "kafka_produce_latency_seconds"):

        producer_module.pending_trades.clear()
        msg = make_fake_msg(key="999")  # 999 was never added to pending_trades

        try:
            producer_module.delivery_report(None, msg)
            assert False, "Expected KeyError but none was raised"
        except KeyError:
            pass  # this is the current (fragile) behavior