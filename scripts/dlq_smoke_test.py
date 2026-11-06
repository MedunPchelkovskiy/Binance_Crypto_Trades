from confluent_kafka import Producer
from decouple import config


TOPIC = "trade_streams_avro_test"


producer = Producer({
    "bootstrap.servers": config("KAFKA_BROKER_ADDRESS_DEV"),
    "acks": "all",
})


def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}", flush=True)
    else:
        print(
            f"Sent invalid message: "
            f"topic={msg.topic()}, "
            f"partition={msg.partition()}, "
            f"offset={msg.offset()}",
            flush=True,
        )


def main():
    for i in range(5):
        invalid_payload = f"THIS_IS_NOT_AVRO_{i}".encode("utf-8")

        producer.produce(
            topic=TOPIC,
            value=invalid_payload,
            callback=delivery_report,
        )
        producer.poll(0)

    producer.flush()


if __name__ == "__main__":
    main()