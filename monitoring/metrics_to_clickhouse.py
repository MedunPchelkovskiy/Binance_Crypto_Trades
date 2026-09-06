"""
ClickHouse persistence for pipeline monitoring metrics.
"""


METRICS_TABLE = "silver_metrics"

METRICS_COLUMNS = [
    "layer",
    "batch_id",
    "batch_timestamp",
    "batch_duration_ms",
    "records_processed",
    "avg_latency_ms",
    "max_latency_ms",
    "p95_latency_ms",
]


def write_pipeline_metrics(
    clickhouse_client,
    layer,
    batch_id,
    batch_timestamp,
    metrics,
):
    """Writes one batch's metrics to ClickHouse."""

    row = [
        layer,
        batch_id,
        batch_timestamp,
        metrics["batch_duration_ms"],
        metrics["records_processed"],
        metrics["avg_latency_ms"],
        metrics["max_latency_ms"],
        metrics["p95_latency_ms"],
    ]

    clickhouse_client.insert(
        METRICS_TABLE,
        [row],
        column_names=METRICS_COLUMNS,
    )
