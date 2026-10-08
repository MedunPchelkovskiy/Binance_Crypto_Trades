# Trades newer than this may still be in Kafka/consumer buffers,
# so a "missing" id there is lag, not a real gap.
SAFETY_MARGIN_SEC = 300


def fetch_silver_bounds(client, symbols, margin_sec=SAFETY_MARGIN_SEC):
    """Returns {symbol: (min_id, upper_id)} where upper_id ignores the newest trades."""
    result = client.query(
        """
        SELECT symbol, min(agg_trade_id), maxIf(agg_trade_id, trade_time <= now64(3) - toIntervalSecond({margin:UInt32}))
        FROM trades.binance_agg_trades_silver
        WHERE symbol IN {symbols:Array(String)}
        GROUP BY symbol
        """,
        parameters={"margin": margin_sec, "symbols": symbols},
    )
    return {s: (mn, up) for s, mn, up in result.result_rows}


def fetch_watermarks(client, symbols):
    """Returns {symbol: checked_up_to_id}; symbols without state are absent."""
    result = client.query(
        """
        SELECT symbol, argMax(checked_up_to_id, updated_at)
        FROM trades.backfill_state
        WHERE symbol IN {symbols:Array(String)}
        GROUP BY symbol
        """,
        parameters={"symbols": symbols},
    )
    return {s: wm for s, wm in result.result_rows}


def compute_check_ranges(symbols, bounds, watermarks):
    """Pure function: decides which id range to scan for gaps per symbol."""
    ranges = []
    for symbol in symbols:
        if symbol not in bounds:
            continue  # no data in silver yet
        min_id, upper_id = bounds[symbol]
        # First run: everything below the oldest id in silver is out of scope.
        watermark = watermarks.get(symbol, min_id - 1)
        if upper_id <= watermark:
            continue  # nothing new beyond the safety margin
        ranges.append({"symbol": symbol, "from_id": watermark + 1, "to_id": upper_id})
    return ranges


def check_data(client, symbols):
    symbols = [s.upper() for s in symbols]  # silver stores BNBUSDT, the stream uses bnbusdt
    return compute_check_ranges(
        symbols,
        fetch_silver_bounds(client, symbols),
        fetch_watermarks(client, symbols),
    )