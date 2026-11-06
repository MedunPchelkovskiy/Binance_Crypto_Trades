import json
from datetime import datetime
from unittest.mock import MagicMock

import pytest

from consumers import dlq


def make_msg(value=b"\x01\xab", key=b"BTCUSDT",
             topic="trade_streams_avro", partition=2, offset=42):
    msg = MagicMock()
    msg.value.return_value = value
    msg.key.return_value = key
    msg.topic.return_value = topic
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    return msg


@pytest.fixture
def producer(monkeypatch):
    p = MagicMock()
    monkeypatch.setattr(dlq, "_get_dlq_producer", lambda: p)
    return p


@pytest.fixture
def counter(monkeypatch):
    c = MagicMock()
    monkeypatch.setattr(dlq, "dlq_messages_total", c)
    return c


def published_payload(producer):
    return json.loads(producer.produce.call_args.kwargs["value"])


def test_publishes_to_dlq_topic_with_original_key(producer, counter):
    dlq.send_to_dlq(make_msg(), "boom", "SchemaError", "silver-group")

    kwargs = producer.produce.call_args.kwargs
    assert kwargs["topic"] == dlq.DLQ_TOPIC
    assert kwargs["key"] == b"BTCUSDT"


def test_payload_contains_source_and_error_metadata(producer, counter):
    dlq.send_to_dlq(make_msg(), ValueError("bad price"), "ValidationError", "bronze-minio")

    payload = published_payload(producer)
    assert payload["source_topic"] == "trade_streams_avro"
    assert payload["source_partition"] == 2
    assert payload["source_offset"] == 42
    assert payload["consumer_group"] == "bronze-minio"
    assert payload["error_type"] == "ValidationError"
    assert payload["error_message"] == "bad price"


def test_original_message_is_hex_encoded_for_replay(producer, counter):
    dlq.send_to_dlq(make_msg(value=b"\x01\xab"), "err", "T", "g")

    assert published_payload(producer)["original_message"] == "01ab"


def test_none_value_is_handled(producer, counter):
    dlq.send_to_dlq(make_msg(value=None), "err", "T", "g")

    assert published_payload(producer)["original_message"] is None


def test_failed_at_is_timezone_aware_iso_timestamp(producer, counter):
    dlq.send_to_dlq(make_msg(), "err", "T", "g")

    ts = datetime.fromisoformat(published_payload(producer)["failed_at"])
    assert ts.tzinfo is not None


def test_produce_then_flush_then_metric(producer, counter):
    dlq.send_to_dlq(make_msg(), "err", "T", "g")

    assert [c[0] for c in producer.mock_calls] == ["produce", "flush"]
    counter.inc.assert_called_once()


def test_producer_is_created_lazily_and_only_once(monkeypatch):
    fake = MagicMock()
    factory = MagicMock(return_value=fake)
    monkeypatch.setattr(dlq, "get_producer", factory)
    monkeypatch.setattr(dlq, "_dlq_producer", None)

    assert dlq._get_dlq_producer() is fake
    assert dlq._get_dlq_producer() is fake
    factory.assert_called_once()