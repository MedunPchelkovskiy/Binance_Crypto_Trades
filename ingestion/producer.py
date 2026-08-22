# ingestion/producer.py
import asyncio
import inspect
import json

from decouple import config
from kafka import KafkaProducer

from ingestion.binance_client import stream_agg_trades
from ingestion.validation import Trade

producer = KafkaProducer(
    bootstrap_servers=[config('KAFKA_BROKER_ADDRESS_DEV')],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)


def on_trade_message(data):
    print(type(data))
    print(type(data).__module__)
    print(inspect.getfile(type(data)))

    print(data, flush=True)

    try:
        trade = Trade.model_validate(data.to_dict())

        future = producer.send(
            'trade_streams_dev',
            value=trade.model_dump()
        )

        metadata = future.get(timeout=10)
        print(
            f"topic={metadata.topic}, "
            f"partition={metadata.partition}, "
            f"offset={metadata.offset}"
        )

    except Exception as e:
        print(f"Validation ERROR: {e}")



# def on_trade_message(data):
#     print(data, flush=True)
#     future = producer.send('trade_streams', value=data.model_dump())
#     try:
#         metadata = future.get(timeout=10)
#         print(f"topic={metadata.topic}, partition={metadata.partition}, offset={metadata.offset}")
#     except Exception as e:
#         print(f"Kafka ERROR: {e}")


async def main():
    symbols = ["bnbusdt", "btcusdt", "ethusdt"]
    try:
        await stream_agg_trades(symbols, on_trade_message)
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nСкриптът е спрян от потребителя.")
