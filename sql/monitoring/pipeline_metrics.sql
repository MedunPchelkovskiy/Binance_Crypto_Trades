
CREATE TABLE pipeline_metrics
(
    layer             LowCardinality(String),
    batch_id          UInt64,
    batch_timestamp   DateTime('UTC'),
    batch_duration    UInt64,
    records_processed UInt64,
    avg_latency_ms    UInt64,
    max_latency_ms    UInt64,
    p95_latency       UInt64
    )
ENGINE = MergeTree()
PARTITION BY toYYYYMM(batch_timestamp)
ORDER BY (layer, batch_timestamp);