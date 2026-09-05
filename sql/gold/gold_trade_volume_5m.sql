
CREATE TABLE trades.gold_trade_volume_5m
(
    symbol        LowCardinality(String),
    window_start  DateTime('UTC'),
    volume        Decimal64(8),
    trade_count   UInt64
)
ENGINE = SummingMergeTree((volume, trade_count))
PARTITION BY toYYYYMM(window_start)
ORDER BY (symbol, window_start);



CREATE MATERIALIZED VIEW trades.gold_trade_volume_5m_mv
TO trades.gold_trade_volume_5m
AS
SELECT
    symbol,
    toStartOfFiveMinutes(trade_time) AS window_start,
    sum(quantity)          AS volume,
    count()                AS trade_count
FROM binance_agg_trades_silver
GROUP BY symbol, window_start;