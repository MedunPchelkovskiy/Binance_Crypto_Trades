# Trades newer than this may still be in Kafka/consumer buffers,
# so a "missing" id there is lag, not a real gap.
import requests

SAFETY_MARGIN_SEC = 300
AGG_TRADES_URL = "https://api.binance.com/api/v3/aggTrades"
REST_PAGE_LIMIT = 1000  # Binance maximum per request
# Soft cap per DAG run (checked between pages). Results travel through XCom,
# so a huge gap must be filled over several runs, not in one giant payload.
MAX_TRADES_PER_RUN = 20_000


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

def fetch_existing_ids(client, symbol, from_id, to_id):
    """Returns the agg_trade_ids that already exist in silver for the range."""
    result = client.query(
        """
        SELECT DISTINCT agg_trade_id
        FROM trades.binance_agg_trades_silver
        WHERE symbol = {symbol:String}
          AND agg_trade_id BETWEEN {from_id:Int64} AND {to_id:Int64}
        ORDER BY agg_trade_id
        """,
        parameters={"symbol": symbol, "from_id": from_id, "to_id": to_id},
    )
    return [row[0] for row in result.result_rows]


def find_missing_ranges(ids, from_id, to_id):
    """Pure function: returns [(gap_from, gap_to)] of ids absent from `ids`.

    Walks the sorted ids once and tracks the next expected id, so the
    leading gap, inner gaps and trailing gap are all handled the same way.
    """
    gaps = []
    expected = from_id
    for i in sorted(set(ids)):
        if i < from_id or i > to_id:
            continue
        if i > expected:
            gaps.append((expected, i - 1))
        expected = i + 1
    if expected <= to_id:
        gaps.append((expected, to_id))
    return gaps


def find_gaps(client, ranges):
    gaps = []
    for r in ranges:
        ids = fetch_existing_ids(client, r["symbol"], r["from_id"], r["to_id"])
        for gap_from, gap_to in find_missing_ranges(ids, r["from_id"], r["to_id"]):
            gaps.append({"symbol": r["symbol"], "from_id": gap_from, "to_id": gap_to})
    return gaps

def to_trade(raw, symbol):
    """Pure function: maps a REST aggTrades item to the stream message shape.

    REST has no event time, so E = T (backfill latency metrics must be
    filtered or marked later, as they are meaningless for these records).
    """
    return {
        "e": "aggTrade",
        "E": raw["T"],
        "s": symbol,
        "a": raw["a"],
        "p": raw["p"],
        "q": raw["q"],
        "f": raw["f"],
        "l": raw["l"],
        "T": raw["T"],
        "m": raw["m"],
        "M": raw["M"],
    }


def fetch_gap_trades(symbol, from_id, to_id, http_get=requests.get,
                     max_trades=MAX_TRADES_PER_RUN):
    """Pages through REST aggTrades with fromId until to_id is reached."""
    trades = []
    cursor = from_id
    while cursor <= to_id and len(trades) < max_trades:
        # fromId cannot be combined with startTime/endTime in this endpoint,
        # which is fine: our gaps are defined by id, not by time.
        resp = http_get(
            AGG_TRADES_URL,
            params={"symbol": symbol, "fromId": cursor, "limit": REST_PAGE_LIMIT},
            timeout=10,
        )
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break  # nothing newer on the exchange
        for raw in batch:
            if raw["a"] > to_id:
                return trades  # page overshoots the gap
            trades.append(to_trade(raw, symbol))
        cursor = batch[-1]["a"] + 1
    return trades


def get_binance_data(gaps, http_get=requests.get, max_trades=MAX_TRADES_PER_RUN):
    """Fetches trades for gaps in order, sharing one budget across all gaps."""
    trades = []
    for gap in gaps:
        remaining = max_trades - len(trades)
        if remaining <= 0:
            break  # leftover gaps are picked up by the next run
        trades.extend(
            fetch_gap_trades(gap["symbol"], gap["from_id"], gap["to_id"],
                             http_get=http_get, max_trades=remaining)
        )
    return trades


def validate_trades(trades, model):
    """Pure function: splits trades into (valid, invalid) using the stream's model.

    Invalid trades are not fatal: their ids stay missing in silver, so
    verify_backfill will see them as unfilled gaps instead of hiding them.
    """
    valid, invalid = [], []
    for trade in trades:
        try:
            valid.append(model.model_validate(trade).model_dump())
        except Exception as e:
            invalid.append({"trade": trade, "error": str(e)})
    return valid, invalid

def filter_new_trades(client, trades):
    """Drops trades whose id already exists in silver.

    Runs right before publishing: gold MVs use sum/count, so a duplicate
    would be counted twice and cannot be fixed by ReplacingMergeTree.
    """
    by_symbol = {}
    for t in trades:
        by_symbol.setdefault(t["s"], []).append(t)

    new = []
    for symbol, items in by_symbol.items():
        ids = [t["a"] for t in items]
        existing = set(fetch_existing_ids(client, symbol, min(ids), max(ids)))
        new.extend(t for t in items if t["a"] not in existing)
    return new


def publish_trades(trades, producer, serialize, topic, flush_timeout=30):
    """Produces trades to Kafka and fails loudly if any delivery fails."""
    failures = []

    def on_delivery(err, msg):
        if err is not None:
            failures.append(str(err))

    for t in trades:
        kwargs = dict(
            topic=topic,
            key=str(t["a"]),  # same key as the stream producer
            value=serialize(t),
            headers=[("source", b"backfill")],  # lets us tell backfill from stream later
            callback=on_delivery,
        )
        try:
            producer.produce(**kwargs)
        except BufferError:
            producer.flush()  # local queue full: drain it, then retry once
            producer.produce(**kwargs)
        producer.poll(0)

    remaining = producer.flush(flush_timeout)
    if remaining or failures:
        raise RuntimeError(
            f"Backfill publish failed: {remaining} undelivered, "
            f"{len(failures)} errors, first: {failures[:1]}"
        )


def write_backfill(client, trades, producer, serialize, topic):
    new = filter_new_trades(client, trades)
    publish_trades(new, producer, serialize, topic)
    return {"published": len(new), "skipped_existing": len(trades) - len(new)}