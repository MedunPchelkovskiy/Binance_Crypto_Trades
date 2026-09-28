# ingestion/kafka_client.py
"""
Factory functions for creating Kafka client objects.

Kept separate from producer.py so that importing producer.py for its
functions/logic never triggers a real connection attempt — the Producer
is only created when explicitly requested via get_producer().
"""

from confluent_kafka import Producer
from decouple import config


def get_producer():
    """Creates and returns a new Kafka Producer, configured from env vars."""
    producer_conf = {
        "bootstrap.servers": config("KAFKA_BROKER_ADDRESS"),
        "acks": "all",
    }
    return Producer(producer_conf)