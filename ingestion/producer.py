# ingestion/producer.py
import asyncio
import json
from decouple import config
from kafka import KafkaProducer

from ingestion.binance_client import stream_agg_trades

producer = KafkaProducer(
    bootstrap_servers=[config('KAFKA_BROKER_ADDRESS')],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def on_trade_message(data):
    print(">>> CALLBACK ВЛЕЗЕ")
    print(data, flush=True)
    future = producer.send('trade_streams', value=data.model_dump())
    try:
        metadata = future.get(timeout=10)
        print(f"Kafka OK: topic={metadata.topic}, partition={metadata.partition}, offset={metadata.offset}")
    except Exception as e:
        print(f"Kafka ERROR: {e}")


    # producer.send('trade_streams', value=data)
    # print(f"Изпратено: {data}")


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