import asyncio
import functools
import time

from confluent_kafka.serialization import SerializationContext, MessageField
from prometheus_client import start_http_server

from clients.binance_client import stream_agg_trades
from clients.kafka_clients import get_producer, get_avro_serializer
from ingestion.validation import Trade
from monitoring.prometheus_metrics import validation_errors_total, serialization_errors_total, \
    kafka_delivery_errors_total, kafka_delivered_total, kafka_produce_latency_seconds

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


def on_trade_message(data, avro_serializer, producer):
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
        context = SerializationContext('trade_streams_avro_dev', MessageField.VALUE)
        agg_trade_id = trade.a

        serialized_value = avro_serializer(trade.model_dump(), context)

        producer.produce(
            topic='trade_streams_avro_dev',
            key=str(agg_trade_id),
            value=serialized_value,
            callback=delivery_report,
        )
        pending_trades[agg_trade_id] = time.monotonic()
        producer.poll(0)

    except Exception as e:
        serialization_errors_total.inc()
        print(f"Serialisation/Produce ERROR: {e}")


async def main():
    producer = get_producer()
    avro_serializer = get_avro_serializer()
    symbols = ["bnbusdt", "btcusdt", "ethusdt"]
    callback = functools.partial(on_trade_message, avro_serializer=avro_serializer, producer=producer)
    try:
        await stream_agg_trades(symbols, callback)
    finally:
        print("\nИзчистване на опашката и затваряне на продюсера...")
        producer.flush()


if __name__ == "__main__":
    start_http_server(8003, addr="0.0.0.0")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nСкриптът е спрян от потребителя.")
