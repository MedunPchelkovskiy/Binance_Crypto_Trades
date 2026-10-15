"""
ClickHouse Silver consumer.

Reads Avro trade messages from Kafka, applies small canonical
transformations, and inserts them into the Silver ClickHouse table.

Runs independently from the Bronze consumer using a separate
consumer group.

Manual offset commit — only after a successful ClickHouse insert.
"""

import logging
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

from confluent_kafka import KafkaException, TopicPartition
from confluent_kafka.serialization import SerializationContext, MessageField
from prometheus_client import start_http_server

from consumers.clients import get_clickhouse_client, get_consumer, get_avro_deserializer
from consumers.dlq import send_to_dlq
from monitoring.metrics_measurement import measure_batch
from monitoring.metrics_to_clickhouse import write_pipeline_metrics
from monitoring.prometheus_metrics import batch_clickhouse_insert_duration, clickhouse_records_counter, \
    deserialization_errors_total, silver_buffer_size

# --- Configuration ---

logger = logging.getLogger(__name__)

TOPIC = "trade_streams_avro_dev"
CONSUMER_GROUP = "clickhouse-silver-consumer-group"

BATCH_SIZE = 500
BATCH_TIMEOUT_SEC = 30

CLICKHOUSE_TABLE = "binance_agg_trades_silver"

COLUMN_NAMES = [
    "event_type",
    "event_time",
    "symbol",
    "agg_trade_id",
    "price",
    "quantity",
    "first_trade_id",
    "last_trade_id",
    "trade_time",
    "is_buyer_maker",
    "is_best_match",
]


def ms_to_datetime(value):
    """Converts Unix milliseconds to UTC datetime."""
    return datetime.fromtimestamp(
        value / 1000,
        tz=timezone.utc,
    )


def record_to_row(record):
    """Transforms an Avro trade record into a Silver row."""

    return (
        record["e"],
        ms_to_datetime(record["E"]),
        record["s"],
        int(record["a"]),
        Decimal(record["p"]),
        Decimal(record["q"]),
        int(record["f"]),
        int(record["l"]),
        ms_to_datetime(record["T"]),
        bool(record["m"]),
        bool(record["M"]),
    )


def write_batch_to_clickhouse(clickhouse_client, records):
    """Inserts a batch of transformed records into Silver."""

    batch_insert_start = time.monotonic()
    rows = [record_to_row(r) for r, _ in records]

    clickhouse_client.insert(
        CLICKHOUSE_TABLE,
        rows,
        column_names=COLUMN_NAMES,
    )
    clickhouse_records_counter.inc(len(rows))
    batch_clickhouse_insert_duration.observe(time.monotonic() - batch_insert_start)
    print(
        f"Inserted Silver batch into ClickHouse: {len(rows)} rows",
        flush=True,
    )


def main():
    consumer = get_consumer(CONSUMER_GROUP)
    consumer.subscribe([TOPIC])
    avro_deserializer = get_avro_deserializer()
    clickhouse_client = get_clickhouse_client()

    buffer_records = []
    last_offsets = {}
    last_flush_time = time.monotonic()

    print(
        f"ClickHouse Silver consumer started, "
        f"listening on topic '{TOPIC}'...",
        flush=True,
    )

    try:
        while True:

            msg = consumer.poll(timeout=1.0)

            timed_out = (time.monotonic() - last_flush_time >= BATCH_TIMEOUT_SEC)

            if msg is None:

                if buffer_records and timed_out:
                    batch_id = str(uuid.uuid4())
                    result, metrics = measure_batch(
                        clickhouse_client,
                        batch_id,
                        write_batch_to_clickhouse,
                        buffer_records,
                        extract_event_time_ms=lambda r: r["E"]  # bronze event time от Binance
                    )
                    try:
                        write_pipeline_metrics(
                            clickhouse_client,
                            layer="silver",
                            batch_id=batch_id,
                            batch_timestamp=datetime.now(timezone.utc),
                            metrics=metrics,
                        )
                    except Exception as e:
                        logger.warning("Write pipeline metrics failed: %s", e, exc_info=True)

                    consumer.commit(asynchronous=False)

                    buffer_records.clear()
                    last_offsets.clear()
                    silver_buffer_size.set(len(buffer_records))

                    last_flush_time = time.monotonic()

                continue

            if msg.error():
                raise KafkaException(msg.error())

            try:

                record = avro_deserializer(
                    msg.value(),
                    SerializationContext(
                        TOPIC,
                        MessageField.VALUE,
                    ),
                )


            except Exception as e:

                print(
                    f"Deserialization ERROR: {e}",
                    flush=True,
                )

                deserialization_errors_total.inc()
                send_to_dlq(
                    msg,
                    e,
                    "avro_deserialization",
                    CONSUMER_GROUP
                )

                continue

            if record is not None:
                receipt_time_ms = int(time.time() * 1000)
                buffer_records.append((record, receipt_time_ms))
                last_offsets[msg.partition()] = TopicPartition(
                    msg.topic(), msg.partition(), msg.offset() + 1
                )
                silver_buffer_size.set(len(buffer_records))

            if len(buffer_records) >= BATCH_SIZE or timed_out:

                if buffer_records:
                    batch_id = str(uuid.uuid4())
                    result, metrics = measure_batch(clickhouse_client,
                                                    batch_id,
                                                    write_batch_to_clickhouse,
                                                    buffer_records,
                                                    extract_event_time_ms=lambda r: r["E"],
                                                    # bronze event time от Binance
                                                    )
                    try:
                        write_pipeline_metrics(
                            clickhouse_client,
                            layer="silver",
                            batch_id=batch_id,
                            batch_timestamp=datetime.now(timezone.utc),
                            metrics=metrics,
                        )
                    except Exception as e:
                        logger.warning("Write pipeline metrics failed: %s", e, exc_info=True)

                    consumer.commit(asynchronous=False)

                    buffer_records.clear()
                    last_offsets.clear()
                    silver_buffer_size.set(len(buffer_records))

                    last_flush_time = time.monotonic()

    except KeyboardInterrupt:

        print(
            "\nClickHouse Silver consumer stopped by user.",
            flush=True,
        )

    finally:

        try:

            if buffer_records:
                batch_id = str(uuid.uuid4())
                result, metrics = measure_batch(
                    clickhouse_client,
                    batch_id,
                    write_batch_to_clickhouse,
                    buffer_records,
                    extract_event_time_ms=lambda r: r["E"]  # bronze event time от Binance
                )

                try:
                    write_pipeline_metrics(
                        clickhouse_client,
                        layer="silver",
                        batch_id=batch_id,
                        batch_timestamp=datetime.now(timezone.utc),
                        metrics=metrics,
                    )

                except Exception as e:

                    logger.warning("Write pipeline metrics failed: %s", e, exc_info=True)
                # Commit only up to the last buffered message, not the consumer
                # position, which may already be past a message that failed DLQ.
                consumer.commit(offsets=list(last_offsets.values()), asynchronous=False)

        finally:
            consumer.close()


if __name__ == "__main__":
    start_http_server(8005, addr="0.0.0.0")
    logging.basicConfig(level=logging.INFO)
    main()
