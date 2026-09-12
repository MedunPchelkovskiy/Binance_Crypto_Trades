### original file comented for testing,

# ingestion/producer.py
import asyncio
import time

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from decouple import config
from prometheus_client import start_http_server

from ingestion.binance_client import stream_agg_trades
from ingestion.validation import Trade
from monitoring.prometheus_metrics import validation_errors_total, serialization_errors_total, \
    kafka_delivery_errors_total, kafka_delivered_total, kafka_produce_latency_seconds
# ДИРЕКТЕН ИМПОРТ НА СХЕМАТА КАТО ПРОМЕНЛИВА
from schemas.trade_schema import AVRO_TRADE_SCHEMA

# 1. Инициализация на Schema Registry Клиента
schema_registry_conf = {'url': config('SCHEMA_REGISTRY_URL', default='http://localhost:8081')}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# 2. Подаваме импортирания стринг директно на сериализатора
avro_serializer = AvroSerializer(
    schema_registry_client=schema_registry_client,
    schema_str=AVRO_TRADE_SCHEMA
)

# 3. Настройка на Confluent Kafka Producer
producer_conf = {
    'bootstrap.servers': config('KAFKA_BROKER_ADDRESS'),
    'acks': 'all'
}
producer = Producer(producer_conf)
pending_trades = {}


def delivery_report(err, msg):
    agg_trade_id = int(msg.key().decode())
    start_time = pending_trades.pop(agg_trade_id)
    if err is not None:
        kafka_delivery_errors_total.inc()
        print(f"Delivery failed for record: {err}")
    else:
        kafka_delivered_total.inc()
        print(f"Успешен запис! topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}")
    latency = time.monotonic() - start_time
    kafka_produce_latency_seconds.observe(latency)


def on_trade_message(data):
    print(data, flush=True)

    # НИВО 1: МИТНИЧАРЯТ (Pydantic Валидация)
    try:
        trade = Trade.model_validate(data.to_dict())
    except Exception as e:
        validation_errors_total.inc()
        print(f"Validation ERROR: {e}")
        return

    # НИВО 2: АВТОМАТИЧНА СЕРИАЛИЗАЦИЯ И ТРАНСПОРТ
    try:
        # Дефинираме контекста: за кой топик и че сериализираме СТОЙНОСТТА (Value) на съобщението
        context = SerializationContext('trade_streams_avro', MessageField.VALUE)
        agg_trade_id = trade.a

        serialized_value = avro_serializer(trade.model_dump(), context)
        pending_trades[agg_trade_id] = time.monotonic()

        producer.produce(
            topic='trade_streams_avro',
            key=str(agg_trade_id),
            value=serialized_value,  # Подаваме контекста тук!
            callback=delivery_report,
        )
        producer.poll(0)

    except Exception as e:
        serialization_errors_total.inc()
        print(f"Serialisation/Produce ERROR: {e}")


async def main():
    symbols = ["bnbusdt", "btcusdt", "ethusdt"]
    try:
        await stream_agg_trades(symbols, on_trade_message)
    finally:
        print("\nИзчистване на опашката и затваряне на продюсера...")
        producer.flush()


if __name__ == "__main__":
    start_http_server(8003)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nСкриптът е спрян от потребителя.")
