"""
ClickHouse Silver consumer.

Reads Avro trade messages from Kafka, applies small canonical
transformations, and inserts them into the Silver ClickHouse table.

Runs independently from the Bronze consumer using a separate
consumer group.

Manual offset commit — only after a successful ClickHouse insert.
"""

import json
import time
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import clickhouse_connect
import fastavro
from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from decouple import config

from monitoring.metrics_measurement import measure_batch
from monitoring.metrics_to_clickhouse import write_pipeline_metrics
from schemas.trade_schema import AVRO_TRADE_SCHEMA

# --- Configuration ---

TOPIC = "trade_streams_avro_dev"
CONSUMER_GROUP = "clickhouse-silver-consumer-group"
DLQ_TOPIC = "trade_streams_avro_dlq"

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

# --- Schema Registry + Avro deserializer ---

schema_registry_conf = {
    "url": config(
        "SCHEMA_REGISTRY_URL",
        default="http://localhost:8081",
    )
}

schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_deserializer = AvroDeserializer(
    schema_registry_client=schema_registry_client,
    schema_str=AVRO_TRADE_SCHEMA,
)

_raw_schema = json.loads(AVRO_TRADE_SCHEMA)
parsed_schema = fastavro.parse_schema(_raw_schema)

# --- Kafka consumer ---

consumer_conf = {
    "bootstrap.servers": config("KAFKA_BROKER_ADDRESS"),
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

dlq_producer = Producer({
    "bootstrap.servers": config("KAFKA_BROKER_ADDRESS_DEV"),
    "acks": "all",
})

consumer = Consumer(consumer_conf)

# --- ClickHouse client ---

clickhouse_client = clickhouse_connect.get_client(
    host=config("CLICKHOUSE_HOST", default="localhost"),
    port=config("CLICKHOUSE_PORT", default=8123, cast=int),
    username=config("CLICKHOUSE_USER", default="default"),
    password=config("CLICKHOUSE_PASSWORD", default=""),
    database=config("CLICKHOUSE_DATABASE", default="trades"),
)


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


def write_batch_to_clickhouse(records):
    """Inserts a batch of transformed records into Silver."""

    rows = [record_to_row(r) for r, _ in records]

    clickhouse_client.insert(
        CLICKHOUSE_TABLE,
        rows,
        column_names=COLUMN_NAMES,
    )

    print(
        f"Inserted Silver batch into ClickHouse: {len(rows)} rows",
        flush=True,
    )


def send_to_dlq(msg, error, error_type):
    """Publishes failed Kafka messages to the DLQ."""

    dlq_payload = {
        "source_topic": msg.topic(),
        "source_partition": msg.partition(),
        "source_offset": msg.offset(),
        "error_type": error_type,
        "error_message": str(error),
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "original_message": (
            msg.value().hex()  # value is binary data, hex allowed to preserve original bites for replay
            if msg.value() is not None
            else None
        ),
    }

    dlq_producer.produce(
        topic=DLQ_TOPIC,
        key=msg.key(),
        value=json.dumps(dlq_payload).encode("utf-8"),
    )

    dlq_producer.flush()

    print(
        f"Message sent to DLQ: "
        f"topic={msg.topic()}, "
        f"partition={msg.partition()}, "
        f"offset={msg.offset()}",
        flush=True,
    )


def main():
    consumer.subscribe([TOPIC])

    buffer_records = []
    last_flush_time = time.monotonic()

    print(
        f"ClickHouse Silver consumer started, "
        f"listening on topic '{TOPIC}'...",
        flush=True,
    )

    try:
        while True:

            msg = consumer.poll(timeout=1.0)

            timed_out = (
                    time.monotonic() - last_flush_time
                    >= BATCH_TIMEOUT_SEC
            )

            if msg is None:

                if buffer_records and timed_out:
                    batch_id = str(uuid.uuid4())
                    result, metrics = measure_batch(
                        batch_id,
                        write_batch_to_clickhouse,
                        buffer_records,
                        extract_event_time_ms=lambda r: r["E"]  # bronze event time от Binance
                    )
                    write_pipeline_metrics(
                        clickhouse_client,
                        layer="silver",
                        batch_id=batch_id,
                        batch_timestamp=datetime.now(timezone.utc),
                        metrics=metrics,
                    )

                    consumer.commit(asynchronous=False)

                    buffer_records.clear()

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

                send_to_dlq(
                    msg,
                    e,
                    "avro_deserialization",
                )

                consumer.commit(
                    message=msg,
                    asynchronous=False,
                )

                continue

            if record is not None:
                receipt_time_ms = int(time.time() * 1000)
                buffer_records.append((record, receipt_time_ms))

            if len(buffer_records) >= BATCH_SIZE or timed_out:

                if buffer_records:
                    batch_id = str(uuid.uuid4())
                    result, metrics = measure_batch(batch_id,
                                                    write_batch_to_clickhouse,
                                                    buffer_records,
                                                    extract_event_time_ms=lambda r: r["E"],
                                                    # bronze event time от Binance
                                                    )
                    write_pipeline_metrics(
                        clickhouse_client,
                        layer="silver",
                        batch_id=batch_id,
                        batch_timestamp=datetime.now(timezone.utc),
                        metrics=metrics,
                    )

                    consumer.commit(asynchronous=False)

                    buffer_records.clear()

                    last_flush_time = time.monotonic()

    except KeyboardInterrupt:

        print(
            "\nClickHouse Silver consumer stopped by user.",
            flush=True,
        )

    finally:

        if buffer_records:
            batch_id = str(uuid.uuid4())
            result, metrics = measure_batch(
                batch_id,
                write_batch_to_clickhouse,
                buffer_records,
                extract_event_time_ms=lambda r: r["E"]  # bronze event time от Binance
            )
            write_pipeline_metrics(
                clickhouse_client,
                layer="silver",
                batch_id=batch_id,
                batch_timestamp=datetime.now(timezone.utc),
                metrics=metrics,
            )

            consumer.commit(asynchronous=False)

        consumer.close()


if __name__ == "__main__":
    main()

    # changes for rebuild initial

