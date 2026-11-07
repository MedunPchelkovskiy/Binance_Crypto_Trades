from confluent_kafka.serialization import SerializationContext, MessageField

from airflow.dags.backfill_logic import fill_gaps
from clients.clickhouse_clients import get_clickhouse_client
from clients.kafka_clients import get_avro_serializer, get_producer
from ingestion.validation import Trade

TOPIC = "trade_streams_avro_dev"
ser = get_avro_serializer()
serialize = lambda t: ser(t, SerializationContext(TOPIC, MessageField.VALUE))

gaps = [{"symbol": "BNBUSDT", "from_id": 853876320, "to_id": 853876337}]
print(fill_gaps(get_clickhouse_client(), get_producer(), serialize, TOPIC,
                Trade, gaps, "manual-1"))
