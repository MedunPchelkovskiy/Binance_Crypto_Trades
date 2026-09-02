CREATE TABLE binance_agg_trades_silver
(
    event_type       LowCardinality(String),
    event_time        DateTime64(3, 'UTC'),
    symbol            LowCardinality(String),
    agg_trade_id      Int64,
    price             Decimal64(8),
    quantity          Decimal64(8),
    first_trade_id    Int64,
    last_trade_id     Int64,
    trade_time        DateTime64(3, 'UTC'),
    is_buyer_maker    Bool,
    is_best_match     Bool,
    ingested_at       DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(trade_time)
ORDER BY (symbol, trade_time);