from confluent_kafka.serialization import SerializationContext, MessageField

from airflow.dags.backfill_logic import fill_gaps
from consumers.clients import get_clickhouse_client, get_producer
from ingestion.producer_clients import get_avro_serializer
from ingestion.validation import Trade

TOPIC = "trade_streams_avro_dev"
ser = get_avro_serializer()
serialize = lambda t: ser(t, SerializationContext(TOPIC, MessageField.VALUE))

gaps = [{"symbol": "BNBUSDT", "from_id": 853876320, "to_id": 853876337}]
print(fill_gaps(get_clickhouse_client(), get_producer(), serialize, TOPIC,
                Trade, gaps, "manual-1"))
