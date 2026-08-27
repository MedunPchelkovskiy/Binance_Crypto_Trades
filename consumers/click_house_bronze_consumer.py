# ingestion/clickhouse_consumer.py
"""
ClickHouse consumer.

Reads trade messages from Kafka (Avro, wire format with Schema Registry),
buffers them into batches, and inserts them into ClickHouse for fast
analytical queries and dashboarding (e.g. Grafana).

This consumer runs independently from bronze_consumer.py (fan-out from
the same Kafka topic, separate consumer group) — MinIO remains the
source-of-truth raw archive, ClickHouse is a query-optimized sink.

Manual offset commit — only after a successful insert into ClickHouse.
If the insert fails, messages are re-consumed on next start (at-least-once).
"""

import json
import time

import clickhouse_connect
import fastavro
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from decouple import config

from schemas.trade_schema import AVRO_TRADE_SCHEMA

# --- Configuration ---

TOPIC = "trade_streams_avro_dev"
CONSUMER_GROUP = "clickhouse-consumer-group"

BATCH_SIZE = 500          # number of records before a forced flush
BATCH_TIMEOUT_SEC = 30    # max wait before flush, even if batch is not full

CLICKHOUSE_TABLE = "binance_agg_trades_bronze"

# Column order must match the INSERT below and the table schema
COLUMN_NAMES = ["e", "E", "s", "a", "p", "q", "f", "l", "T", "m", "M"]

# --- Schema Registry + Avro deserializer ---

schema_registry_conf = {"url": config("SCHEMA_REGISTRY_URL", default="http://localhost:8081")}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_deserializer = AvroDeserializer(
    schema_registry_client=schema_registry_client,
    schema_str=AVRO_TRADE_SCHEMA,
)

# Kept for parity with bronze_consumer.py, in case a fastavro-parsed schema
# is needed elsewhere in this process (not required for ClickHouse insert).
_raw_schema = json.loads(AVRO_TRADE_SCHEMA)
parsed_schema = fastavro.parse_schema(_raw_schema)

# --- Kafka consumer ---

consumer_conf = {
    "bootstrap.servers": config("KAFKA_BROKER_ADDRESS_DEV"),
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # manual commit, only after a successful insert
}
consumer = Consumer(consumer_conf)

# --- ClickHouse client ---

clickhouse_client = clickhouse_connect.get_client(
    host=config("CLICKHOUSE_HOST", default="localhost"),
    port=config("CLICKHOUSE_PORT", default=8123, cast=int),
    username=config("CLICKHOUSE_USER", default="default"),
    password=config("CLICKHOUSE_PASSWORD", default=""),
    database=config("CLICKHOUSE_DATABASE", default="trades"),
)


def record_to_row(record):
    """Maps a deserialized Avro record (dict) to a row tuple matching COLUMN_NAMES."""
    return tuple(record[col] for col in COLUMN_NAMES)


def write_batch_to_clickhouse(records):
    """Inserts a batch of records into ClickHouse."""
    rows = [record_to_row(r) for r in records]
    clickhouse_client.insert(CLICKHOUSE_TABLE, rows, column_names=COLUMN_NAMES)
    print(f"Inserted batch into ClickHouse: {len(rows)} rows", flush=True)


def main():
    consumer.subscribe([TOPIC])

    buffer_records = []
    last_flush_time = time.monotonic()

    print(f"ClickHouse consumer started, listening on topic '{TOPIC}'...", flush=True)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            timed_out = time.monotonic() - last_flush_time >= BATCH_TIMEOUT_SEC

            if msg is None:
                if buffer_records and timed_out:
                    write_batch_to_clickhouse(buffer_records)
                    consumer.commit(asynchronous=False)
                    buffer_records.clear()
                    last_flush_time = time.monotonic()
                continue

            if msg.error():
                raise KafkaException(msg.error())

            try:
                record = avro_deserializer(
                    msg.value(),
                    SerializationContext(TOPIC, MessageField.VALUE),
                )
            except Exception as e:
                print(f"Deserialization ERROR (skip record): {e}", flush=True)
                continue

            if record is not None:
                buffer_records.append(record)

            if len(buffer_records) >= BATCH_SIZE or timed_out:
                if buffer_records:
                    write_batch_to_clickhouse(buffer_records)
                    consumer.commit(asynchronous=False)
                    buffer_records.clear()
                    last_flush_time = time.monotonic()

    except KeyboardInterrupt:
        print("\nClickHouse consumer stopped by user.", flush=True)
    finally:
        if buffer_records:
            write_batch_to_clickhouse(buffer_records)
            consumer.commit(asynchronous=False)
        consumer.close()


if __name__ == "__main__":
    main()