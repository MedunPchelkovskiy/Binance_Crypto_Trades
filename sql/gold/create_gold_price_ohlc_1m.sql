
CREATE TABLE trades.gold_price_ohlc_1m
(
    symbol LowCardinality(String),
    minute DateTime64(3, 'UTC'),

    open  AggregateFunction(argMin, Decimal64(8), DateTime64(3, 'UTC')),
    high  AggregateFunction(max, Decimal64(8)),
    low   AggregateFunction(min, Decimal64(8)),
    close AggregateFunction(argMax, Decimal64(8), DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMMDD(minute)
ORDER BY (symbol, trades.gold_price_ohlc_1m);





CREATE MATERIALIZED VIEW trades.mv_gold_price_ohlc_1m
TO trades.gold_price_ohlc_1m
AS
SELECT
    symbol,
    toStartOfMinute(trade_time) AS minute,

    argMinState(price, trade_time) AS open,
    maxState(price)                AS high,
    minState(price)                AS low,
    argMaxState(price, trade_time) AS close

FROM trades.binance_agg_trades_silver

GROUP BY
    symbol,
    minute;




SELECT
    symbol,
    minute,
    argMinMerge(open)  AS open,
    maxMerge(high)     AS high,
    minMerge(low)      AS low,
    argMaxMerge(close) AS close
FROM trades.gold_price_ohlc_1m
GROUP BY
    symbol,
    minute
ORDER BY
    symbol,
    minute;
