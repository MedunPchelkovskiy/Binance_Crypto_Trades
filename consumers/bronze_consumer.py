# ingestion/bronze_consumer.py
"""
Bronze layer consumer.

Чете trade съобщения от Kafka (Avro, wire format с Schema Registry),
buffer-ва ги на batch-ове и ги пише в MinIO като стандартни
Avro Object Container Files (self-describing — схемата е вградена
във файла, downstream readers не се нуждаят от Schema Registry).

Partitioning по дата/час: bronze/dt=YYYY-MM-DD/hour=HH/<uuid>.avro

Manual offset commit — само след успешен write в MinIO. Ако write-ът
гръмне, съобщенията се преконсумират при следващ старт (at-least-once).
"""

import io
import json
import time
import uuid
from datetime import datetime, timezone

import boto3
import fastavro
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from decouple import config

from schemas.trade_schema import AVRO_TRADE_SCHEMA

# --- Конфигурация ---

TOPIC = "trade_streams_avro_dev"
CONSUMER_GROUP = "bronze-consumer-group"

BATCH_SIZE = 500          # брой записи преди принудителен flush
BATCH_TIMEOUT_SEC = 30    # максимално чакане преди flush, дори при непълен batch

BUCKET_NAME = "trades-bronze-avro"

# --- Schema Registry + Avro deserializer ---

schema_registry_conf = {"url": config("SCHEMA_REGISTRY_URL", default="http://localhost:8081")}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_deserializer = AvroDeserializer(
    schema_registry_client=schema_registry_client,
    schema_str=AVRO_TRADE_SCHEMA,
)

# Fastavro очаква parsed schema (dict) — AVRO_TRADE_SCHEMA може да е dict или JSON string,
# в зависимост от това как е дефинирана в schemas/trade_schema.py
_raw_schema = json.loads(AVRO_TRADE_SCHEMA)
parsed_schema = fastavro.parse_schema(_raw_schema)

# --- Kafka consumer ---

consumer_conf = {
    "bootstrap.servers": config("KAFKA_BROKER_ADDRESS"),
    "group.id": CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # ръчен commit, само след успешен write
}
consumer = Consumer(consumer_conf)

# --- MinIO / S3 client ---

s3_client = boto3.client(
    "s3",
    endpoint_url=config("MINIO_ENDPOINT"),
    aws_access_key_id=config("MINIO_ACCESS_KEY"),
    aws_secret_access_key=config("MINIO_SECRET_KEY"),
)


def write_batch_to_minio(records):
    """Сериализира batch-а като Avro OCF в паметта и го качва в MinIO."""
    buffer = io.BytesIO()
    fastavro.writer(buffer, parsed_schema, records)
    buffer.seek(0)

    now = datetime.now(timezone.utc)
    key = (
        f"bronze/dt={now.strftime('%Y-%m-%d')}/hour={now.strftime('%H')}/"
        f"{uuid.uuid4()}.avro"
    )

    s3_client.put_object(Bucket=BUCKET_NAME, Key=key, Body=buffer.getvalue())
    print(f"Записан bronze batch: {key} ({len(records)} записа)", flush=True)


def main():
    consumer.subscribe([TOPIC])

    buffer_records = []
    last_flush_time = time.monotonic()

    print(f"Bronze consumer стартиран, слуша топик '{TOPIC}'...", flush=True)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            timed_out = time.monotonic() - last_flush_time >= BATCH_TIMEOUT_SEC

            if msg is None:
                if buffer_records and timed_out:
                    write_batch_to_minio(buffer_records)
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
                    write_batch_to_minio(buffer_records)
                    consumer.commit(asynchronous=False)
                    buffer_records.clear()
                    last_flush_time = time.monotonic()

    except KeyboardInterrupt:
        print("\nBronze consumer stopped from customer.", flush=True)
    finally:
        if buffer_records:
            write_batch_to_minio(buffer_records)
            consumer.commit(asynchronous=False)
        consumer.close()


if __name__ == "__main__":
    main()