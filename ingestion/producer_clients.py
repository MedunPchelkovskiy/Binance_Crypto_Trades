# ingestion/producer_clients.py
"""
Factory functions for creating Kafka client objects.

Kept separate from producer.py so that importing producer.py for its
functions/logic never triggers a real connection attempt — the Producer
is only created when explicitly requested via get_producer().
"""

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from decouple import config

from schemas.trade_schema import AVRO_TRADE_SCHEMA


def get_producer(bootstrap_servers=None):
    """Creates and returns a new Kafka Producer, configured from env vars."""
    producer_conf = {
        "bootstrap.servers": bootstrap_servers or config("KAFKA_BROKER_ADDRESS"),
        "acks": "all",
    }
    return Producer(producer_conf)


def get_avro_serializer(registry_url=None):
    schema_registry_url = registry_url or config("SCHEMA_REGISTRY_URL", default="http://localhost:8081")
    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=AVRO_TRADE_SCHEMA,
    )

    return avro_serializer
