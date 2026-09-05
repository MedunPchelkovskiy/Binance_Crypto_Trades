CREATE TABLE gold_buyer_seller_pressure_1m
(
    symbol        LowCardinality(String),
    window_start  DateTime('UTC'),
    buy_volume Decimal64(8),
    sell_volume Decimal64(8),
    buy_count UInt64,
    sell_count UInt64
)
ENGINE = SummingMergeTree((buy_volume, sell_volume, buy_count, sell_count))
PARTITION BY toYYYYMM(window_start)
ORDER BY (symbol, window_start);



CREATE MATERIALIZED VIEW trades.mv_gold_buyer_seller_pressure_1m
TO trades.gold_buyer_seller_pressure_1m
AS
SELECT
    symbol,
    toStartOfMinute(trade_time)                     AS window_start,
	sumIf(quantity, is_buyer_maker = false) AS buy_volume,
	sumIf(quantity, is_buyer_maker = true)  AS sell_volume,
	countIf(is_buyer_maker = false)                  AS buy_count,
	countIf(is_buyer_maker = true)                   AS sell_count
FROM binance_agg_trades_silver
GROUP BY symbol, window_start;