# Trades newer than this may still be in Kafka/consumer buffers,
# so a "missing" id there is lag, not a real gap.
import time

import requests

SAFETY_MARGIN_SEC = 300
AGG_TRADES_URL = "https://api.binance.com/api/v3/aggTrades"
REST_PAGE_LIMIT = 1000  # Binance maximum per request
# Soft cap per DAG run (checked between pages). Results travel through XCom,
# so a huge gap must be filled over several runs, not in one giant payload.
MAX_TRADES_PER_RUN = 20_000
STATE_TABLE = "trades.backfill_state"
STATE_COLUMNS = ["symbol", "checked_up_to_id", "last_run_id", "gaps_found", "gaps_filled"]


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


def count_ids(gaps, symbol):
    return sum(g["to_id"] - g["from_id"] + 1 for g in gaps if g["symbol"] == symbol)


def wait_for_silver(client, ranges, sleep=time.sleep, interval_sec=15, max_attempts=6):
    """Polls find_gaps until silver has everything or attempts run out.

    The silver consumer flushes every BATCH_TIMEOUT_SEC (30s), so checking
    right after publish would report gaps that are merely in flight.
    Total wait (6 x 15s) is deliberately longer than that flush interval.
    """
    gaps = find_gaps(client, ranges)
    for _ in range(max_attempts - 1):
        if not gaps:
            break
        sleep(interval_sec)
        gaps = find_gaps(client, ranges)
    return gaps


def compute_new_watermarks(ranges, remaining):
    """Pure function: {symbol: new_watermark}, only for ranges that made progress.

    No gaps left -> watermark moves to the end of the range.
    Gaps left -> it stops right before the first one, so it is retried next run.
    """
    result = {}
    for r in ranges:
        left = [g for g in remaining if g["symbol"] == r["symbol"]]
        wm = min(g["from_id"] for g in left) - 1 if left else r["to_id"]
        if wm >= r["from_id"]:  # otherwise no progress, nothing to write
            result[r["symbol"]] = wm
    return result


def save_watermarks(client, watermarks, gaps, remaining, run_id):
    rows = []
    for symbol, wm in watermarks.items():
        found = count_ids(gaps, symbol)
        rows.append([symbol, wm, run_id, found, found - count_ids(remaining, symbol)])
    if rows:
        client.insert(STATE_TABLE, rows, column_names=STATE_COLUMNS)
    return rows


def verify_backfill(client, ranges, gaps, written, run_id,
                    sleep=time.sleep, interval_sec=15, max_attempts=6):
    if written["published"] > 0:
        remaining = wait_for_silver(client, ranges, sleep, interval_sec, max_attempts)
    else:
        # Nothing was sent, so waiting would not change the result.
        remaining = find_gaps(client, ranges)
    watermarks = compute_new_watermarks(ranges, remaining)
    save_watermarks(client, watermarks, gaps, remaining, run_id)
    return {"watermarks": watermarks, "remaining_gaps": len(remaining)}


CHUNK_SIZE = 50_000  # max id span of one chunk
MAX_RUNTIME_SEC = 2400  # keep below the DAG interval


def split_into_chunks(gaps, chunk_size):
    """Pure function: ascending, per-symbol chunks whose id span <= chunk_size.

    Small gaps of one symbol are packed into one chunk; a gap larger than
    chunk_size is cut. A chunk is the unit of publish -> verify -> watermark.
    """
    chunks = []
    current = None
    for gap in sorted(gaps, key=lambda g: (g["symbol"], g["from_id"])):
        symbol, start = gap["symbol"], gap["from_id"]
        while start <= gap["to_id"]:
            if (current is None or current["symbol"] != symbol
                    or start > current["from_id"] + chunk_size - 1):
                current = {"symbol": symbol, "from_id": start,
                           "to_id": start, "gaps": []}
                chunks.append(current)
            end = min(gap["to_id"], current["from_id"] + chunk_size - 1)
            current["gaps"].append({"symbol": symbol, "from_id": start, "to_id": end})
            current["to_id"] = end
            start = end + 1
    return chunks


def fill_gaps(client, producer, serialize, topic, model, gaps, run_id, *,
              http_get=requests.get, sleep=time.sleep, clock=time.monotonic,
              chunk_size=CHUNK_SIZE, max_runtime_sec=MAX_RUNTIME_SEC,
              verify_interval_sec=15, verify_attempts=6):
    """Fetch -> validate -> publish -> verify -> move watermark, chunk by chunk.

    Only counters are returned (small XCom payload); trades never leave
    this function.
    """
    started = clock()
    stats = {"chunks_done": 0, "published": 0, "rejected": 0,
             "remaining_gaps": 0, "stopped_by_time": False}
    blocked = set()  # symbol with an unfilled gap: later chunks must not move its watermark

    for chunk in split_into_chunks(gaps, chunk_size):
        symbol = chunk["symbol"]
        if symbol in blocked:
            continue
        # Checked between chunks, never inside one.
        if clock() - started >= max_runtime_sec:
            stats["stopped_by_time"] = True
            break

        trades = get_binance_data(chunk["gaps"], http_get=http_get,
                                  max_trades=chunk_size)
        valid, invalid = validate_trades(trades, model)
        publish_trades(valid, producer, serialize, topic)

        window = [{"symbol": symbol, "from_id": chunk["from_id"], "to_id": chunk["to_id"]}]
        if valid:
            remaining = wait_for_silver(client, window, sleep,
                                        verify_interval_sec, verify_attempts)
        else:
            remaining = find_gaps(client, window)

        watermarks = compute_new_watermarks(window, remaining)
        save_watermarks(client, watermarks, chunk["gaps"], remaining, run_id)

        stats["chunks_done"] += 1
        stats["published"] += len(valid)
        stats["rejected"] += len(invalid)
        stats["remaining_gaps"] += len(remaining)
        if remaining:
            blocked.add(symbol)

    return stats


