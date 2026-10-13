# consumers/dlq.py
"""
Shared Dead Letter Queue publisher for all consumers.

One module so every layer (bronze MinIO, bronze ClickHouse, silver) writes
the same payload format to the same topic.
"""

import json
import logging
from datetime import datetime, timezone

from confluent_kafka import Producer
from decouple import config

from monitoring.prometheus_metrics import dlq_messages_total

logger = logging.getLogger(__name__)

DLQ_TOPIC = config("DLQ_TOPIC", default="trade_streams_avro_dlq")

_dlq_producer = None


def _get_dlq_producer():
    """Lazy init, so importing this module never opens a Kafka connection."""
    global _dlq_producer
    if _dlq_producer is None:
        _dlq_producer = Producer({
            "bootstrap.servers": config("KAFKA_BROKER_ADDRESS"),
            "acks": "all",
        })
    return _dlq_producer


def send_to_dlq(msg, error, error_type, consumer_group):
    """Publishes a failed Kafka message to the DLQ.

    consumer_group is in the payload because all three consumers read the
    same topic: one bad message lands in the DLQ once per group, and
    without it there is no way to tell which consumer rejected it.
    """
    value = msg.value()
    payload = {
        "source_topic": msg.topic(),
        "source_partition": msg.partition(),
        "source_offset": msg.offset(),
        "consumer_group": consumer_group,
        "error_type": error_type,
        "error_message": str(error),
        "failed_at": datetime.now(timezone.utc).isoformat(),
        # Hex preserves the original bytes for replay.
        "original_message": value.hex() if value is not None else None,
    }

    producer = _get_dlq_producer()
    producer.produce(
        topic=DLQ_TOPIC,
        key=msg.key(),
        value=json.dumps(payload).encode("utf-8"),
    )
    producer.flush()
    dlq_messages_total.inc()

    logger.warning(
        "Message sent to DLQ: group=%s topic=%s partition=%s offset=%s",
        consumer_group,
        msg.topic(),
        msg.partition(),
        msg.offset(),
    )