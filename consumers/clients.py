"""
Factory functions for external clients.

Kept separate so that importing consumer modules for their transformation
logic (e.g. record_to_row) never triggers a real connection attempt.
"""
import boto3
import clickhouse_connect
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from decouple import config

from schemas.trade_schema import AVRO_TRADE_SCHEMA


def get_clickhouse_client():
    """Creates and returns a new ClickHouse client, configured from env vars."""
    return clickhouse_connect.get_client(
        host=config("CLICKHOUSE_HOST", default="localhost"),
        port=config("CLICKHOUSE_PORT", default=8123, cast=int),
        username=config("CLICKHOUSE_USER", default="default"),
        password=config("CLICKHOUSE_PASSWORD", default=""),
        database=config("CLICKHOUSE_DATABASE", default="trades"),
    )


def get_avro_deserializer(registry_url=None):
    schema_registry_url = registry_url or config("SCHEMA_REGISTRY_URL", default="http://localhost:8081")
    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})

    avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client,
        schema_str=AVRO_TRADE_SCHEMA,
    )

    return avro_deserializer


def get_consumer(group_id, bootstrap_servers=None):
    servers = bootstrap_servers or config("KAFKA_BROKER_ADDRESS")
    consumer_conf = {
        "bootstrap.servers": servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # manual commit, only after a successful write
    }
    consumer = Consumer(consumer_conf)
    return consumer


def get_producer(bootstrap_servers=None):
    """Creates and returns a new Kafka Producer, configured from env vars."""
    producer_conf = {"bootstrap.servers": bootstrap_servers or config("KAFKA_BROKER_ADDRESS"),
                     "acks": "all",
                     }
    return Producer(producer_conf)


def get_s3_client(endpoint_url=None, access_key=None, secret_key=None):
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url or config("MINIO_ENDPOINT"),
        aws_access_key_id=access_key or config("MINIO_ACCESS_KEY"),
        aws_secret_access_key=secret_key or config("MINIO_SECRET_KEY"),
    )
    return s3_client
