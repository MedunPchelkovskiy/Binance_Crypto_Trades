from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from decouple import config

from schemas.trade_schema import AVRO_TRADE_SCHEMA


def get_avro_serializer(registry_url=None):
    schema_registry_url = registry_url or config("SCHEMA_REGISTRY_URL", default="http://localhost:8081")
    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})

    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=AVRO_TRADE_SCHEMA,
    )

    return avro_serializer


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
