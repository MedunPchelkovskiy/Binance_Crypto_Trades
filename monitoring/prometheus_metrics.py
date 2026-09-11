from prometheus_client import Counter, Histogram, Gauge

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

