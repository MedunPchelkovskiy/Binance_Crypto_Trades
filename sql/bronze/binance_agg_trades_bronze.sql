CREATE TABLE binance_agg_trades_bronze
(
    e String,
    E Int64,
    s LowCardinality(String),
    a Int64,
    p String,
    q String,
    f Int64,
    l Int64,
    T Int64,
    m Bool,
    M Bool,
    trade_time DateTime64(3) MATERIALIZED toDateTime64(T / 1000, 3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(trade_time)
ORDER BY (s, T);