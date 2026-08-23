# ingestion/producer.py
import asyncio
import inspect
import json

from decouple import config
from kafka import KafkaProducer

from ingestion.avro_ser_deser import trade_serialize
from ingestion.binance_client import stream_agg_trades
from ingestion.validation import Trade

producer = KafkaProducer(
    bootstrap_servers=[config('KAFKA_BROKER_ADDRESS_DEV')]
)


def on_trade_message(data):
    print(type(data))
    print(type(data).__module__)
    print(inspect.getfile(type(data)))

    print(data, flush=True)

    try:
        trade = Trade.model_validate(data.to_dict())

    except Exception as e:
        print(f"Validation ERROR: {e}")

    try:
        trade_bytes = trade_serialize(trade.model_dump())
        future = producer.send(
            'trade_streams_dev',
            value=trade_bytes
        )

        metadata = future.get(timeout=10)
        print(
            f"topic={metadata.topic}, "
            f"partition={metadata.partition}, "
            f"offset={metadata.offset}"
        )
    except Exception as e:
        print(f"Serialisation ERROR: {e}")


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
