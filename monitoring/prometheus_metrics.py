from prometheus_client import Counter, Histogram, Gauge

# ingestion metrics

validation_errors_total = Counter(
    "validation_errors_total",
    "Total number of trade validation errors"
)

serialization_errors_total = Counter(
    "serialization_errors_total",
    "Total number of trade serialization errors"
)

kafka_delivery_errors_total = Counter(
    "kafka_delivery_errors_total",
    "Total number of trade delivery errors"
)

kafka_delivered_total = Counter(
    "kafka_delivered_total",
    "Total number of trades delivered"
)

kafka_produce_latency_seconds = Histogram(
    "kafka_produce_latency_seconds",
    "kafka delivery health"
)

binance_messages_received_total = Counter(
    "trades_received_total",
    "Total number of trades received"
)

binance_reconnects_total = Counter(
    "binance_reconnects_total",
    "Total number of Binance WebSocket reconnects"
)

binance_connection_errors = Counter(
    "binance_connection_errors",
    "Total number of Binance WebSocket connection errors"
)

binance_stream_healthy = Gauge(
    "binance_stream_healthy",
    "Whether the Binance stream is currently healthy"
)

binance_message_rate = Gauge(
    "binance_message_rate",
    "Current Binance messages per second"
)

# consumers metrics:

    # bronze minio consumer

batch_to_minio_counter = Counter(
    "batch_to_minio_counter",
    "Total number of Minio batch transactions"
)

records_counter = Counter(
    "records_to_minio_total",
    "Total records written to MinIO"
)

last_batch_timestamp = Gauge(
    "last_batch_to_minio_timestamp_seconds",
    "Unix timestamp of the last batch written to MinIO"
)

batch_wait_seconds = Histogram(
    "batch_wait_seconds",
    "Time a batch waits in the buffer before being flushed"
)

batch_duration_seconds = Histogram(
    "batch_duration_seconds",
    "Time spent writing a batch to MinIO"
)

    # clickhouse silver consumer

batch_clickhouse_insert_duration = Histogram(
    "batch_clickhouse_insert_duration",
    "Time spent writing a batch to ClickHouse"
)

dlq_messages_total = Counter(
    "dlq_messages_total",
    "Total number of DLQ messages"
)


clickhouse_records_counter = Counter(
    "records_to_clickhouse_total",
    "Total records written to clickhouse"
)

deserialization_errors_total = Counter(
    "deserialization_errors_total",
    "Count of Avro deserialization failures in silver consumer"
)

silver_buffer_size = Gauge(
    "silver_buffer_size",
    "Current number of records waiting in buffer before flush"
)
