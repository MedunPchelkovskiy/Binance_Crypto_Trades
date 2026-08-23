# ingestion/producer.py
import asyncio
import inspect
from decouple import config

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField


from ingestion.binance_client import stream_agg_trades
from ingestion.validation import Trade
# ДИРЕКТЕН ИМПОРТ НА СХЕМАТА КАТО ПРОМЕНЛИВА
from schemas.trade_schema import AVRO_TRADE_SCHEMA

# 1. Инициализация на Schema Registry Клиента
schema_registry_conf = {'url': config('SCHEMA_REGISTRY_URL_DEV', default='http://localhost:8081')}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# 2. Подаваме импортирания стринг директно на сериализатора
avro_serializer = AvroSerializer(
    schema_registry_client=schema_registry_client,
    schema_str=AVRO_TRADE_SCHEMA
)

# 3. Настройка на Confluent Kafka Producer
producer_conf = {
    'bootstrap.servers': config('KAFKA_BROKER_ADDRESS_DEV'),
    'acks': 'all'
}
producer = Producer(producer_conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record: {err}")
    else:
        print(f"Успешен запис! topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}")


def on_trade_message(data):
    print(data, flush=True)

    # НИВО 1: МИТНИЧАРЯТ (Pydantic Валидация)
    try:
        trade = Trade.model_validate(data.to_dict())
    except Exception as e:
        print(f"Validation ERROR: {e}")
        return

    # НИВО 2: АВТОМАТИЧНА СЕРИАЛИЗАЦИЯ И ТРАНСПОРТ
    # НИВО 2: АВТОМАТИЧНА СЕРИАЛИЗАЦИЯ И ТРАНСПОРТ
    try:
        # Дефинираме контекста: за кой топик и че сериализираме СТОЙНОСТТА (Value) на съобщението
        context = SerializationContext('trade_streams_avro_dev', MessageField.VALUE)

        producer.produce(
            topic='trade_streams_avro_dev',
            value=avro_serializer(trade.model_dump(), context),  # Подаваме контекста тук!
            callback=delivery_report
        )
        producer.poll(0)

    except Exception as e:
        print(f"Serialisation/Produce ERROR: {e}")


async def main():
    symbols = ["bnbusdt", "btcusdt", "ethusdt"]
    try:
        await stream_agg_trades(symbols, on_trade_message)
    finally:
        print("\nИзчистване на опашката и затваряне на продюсера...")
        producer.flush()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nСкриптът е спрян от потребителя.")


# # ingestion/producer.py
# import asyncio
# import inspect
#
# from decouple import config
# from kafka import KafkaProducer
#
# from ingestion.avro_ser_deser import trade_serialize
# from ingestion.binance_client import stream_agg_trades
# from ingestion.validation import Trade
#
# producer = KafkaProducer(
#     bootstrap_servers=[config('KAFKA_BROKER_ADDRESS_DEV')]
# )
#
#
# def on_trade_message(data):
#     print(type(data))
#     print(type(data).__module__)
#     print(inspect.getfile(type(data)))
#
#     print(data, flush=True)
#
#     try:
#         trade = Trade.model_validate(data.to_dict())
#
#     except Exception as e:
#         print(f"Validation ERROR: {e}")
#
#     try:
#         trade_bytes = trade_serialize(trade.model_dump())
#         future = producer.send(
#             'trade_streams_dev',
#             value=trade_bytes
#         )
#
#         metadata = future.get(timeout=10)
#         print(
#             f"topic={metadata.topic}, "
#             f"partition={metadata.partition}, "
#             f"offset={metadata.offset}"
#         )
#     except Exception as e:
#         print(f"Serialisation ERROR: {e}")
#
#
# async def main():
#     symbols = ["bnbusdt", "btcusdt", "ethusdt"]
#     try:
#         await stream_agg_trades(symbols, on_trade_message)
#     finally:
#         producer.flush()
#         producer.close()
#
#
# if __name__ == "__main__":
#     try:
#         asyncio.run(main())
#     except KeyboardInterrupt:
#         print("\nСкриптът е спрян от потребителя.")
