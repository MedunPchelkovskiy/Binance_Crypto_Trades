"""
Factory function for creating a ClickHouse client.

Kept separate so that importing consumer modules for their transformation
logic (e.g. record_to_row) never triggers a real connection attempt.
"""

import clickhouse_connect
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry._sync.avro import AvroDeserializer
from decouple import config

from schemas.trade_schema import AVRO_TRADE_SCHEMA

CONSUMER_GROUP = "clickhouse-consumer-group"


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
    schema_registry_conf = registry_url or config("SCHEMA_REGISTRY_URL", default="http://localhost:8081")
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_deserializer = AvroDeserializer(
        schema_registry_client=schema_registry_client,
        schema_str=AVRO_TRADE_SCHEMA,
    )

    return avro_deserializer


def get_consumer(group_id, bootstrap_servers):
    schema_registry_client = SchemaRegistryClient()
    consumer_conf = {
        "bootstrap.servers": config("KAFKA_BROKER_ADDRESS"),
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # manual commit, only after a successful insert
    }
