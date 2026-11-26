import logging
import time

import requests

# ---------------------------------------------------------------------------
# Binance rate limits
# ---------------------------------------------------------------------------
REQUEST_WEIGHT_LIMIT = 6000
REQUEST_WEIGHT_SAFETY_RATIO = 0.80
REQUEST_WEIGHT_BUDGET = int(REQUEST_WEIGHT_LIMIT * REQUEST_WEIGHT_SAFETY_RATIO)
AGG_TRADES_WEIGHT = 4
SERVER_TIME_SYNC_INTERVAL_SEC = 60
USED_WEIGHT_HEADER = "X-MBX-USED-WEIGHT-1M"
TIME_URL = "https://api.binance.com/api/v3/time"

# One threshold for both 429 and 418: if Binance asks to wait longer than
# this, the run stops cleanly and the next run continues from the watermark.
MAX_RATE_LIMIT_WAIT_SEC = 120
MAX_418_WAIT_SEC = MAX_RATE_LIMIT_WAIT_SEC  # backward-compatible alias
MAX_429_RETRIES = 5  # consecutive 429/418 answers for one request
MAX_TRANSIENT_RETRIES = 3  # 5xx and network errors for one request
TRANSIENT_BACKOFF_BASE_SEC = 2
MIN_DELAY_SEC = 0.1
RATE_LIMIT_CODES = (418, 429)

# ---------------------------------------------------------------------------
# Backfill settings
# ---------------------------------------------------------------------------
CHUNK_SIZE = 50_000  # max id span of one chunk
MAX_RUNTIME_SEC = 2400  # keep below the DAG interval

# Trades newer than this may still be in Kafka/consumer buffers,
# so a "missing" id there is lag, not a real gap.
SAFETY_MARGIN_SEC = 300
AGG_TRADES_URL = "https://api.binance.com/api/v3/aggTrades"
REST_PAGE_LIMIT = 1000  # Binance maximum per request
STATE_TABLE = "trades.backfill_state"
STATE_COLUMNS = ["symbol", "checked_up_to_id", "last_run_id", "gaps_found", "gaps_filled"]

rate_limit_state = {
    "server_offset": 0.0,
    "last_sync": None,
    "window": None,
    "reserved_weight": 0,
    "reported_weight": 0,
    "pending_weight": 0,
}


def fetch_bronze_bounds(client, symbols, margin_sec=SAFETY_MARGIN_SEC):
    """Returns {symbol: (min_id, upper_id)} where upper_id ignores the newest trades."""
    result = client.query(
        """
        SELECT s, min(a), maxIf(a, T <= toUnixTimestamp64Milli(now64(3)) - {margin:UInt32} * 1000)
        FROM trades.binance_agg_trades_bronze
        WHERE s IN {symbols:Array(String)}
        GROUP BY s
        """,
        parameters={"margin": margin_sec, "symbols": symbols},
    )
    return {s: (min_id, max_id) for s, min_id, max_id in result.result_rows}


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
            continue  # no data in bronze yet
        min_id, upper_id = bounds[symbol]
        # First run: everything below the oldest id in bronze is out of scope.
        watermark = watermarks.get(symbol, min_id - 1)
        if upper_id <= watermark:
            continue  # nothing new beyond the safety margin
        ranges.append({"symbol": symbol, "from_id": watermark + 1, "to_id": upper_id})
    return ranges


def check_data(client, symbols):
    symbols = [s.upper() for s in symbols]  # bronze stores BNBUSDT, the stream uses bnbusdt
    return compute_check_ranges(
        symbols,
        fetch_bronze_bounds(client, symbols),
        fetch_watermarks(client, symbols),
    )


def fetch_existing_ids(client, symbol, from_id, to_id):
    """Returns the agg_trade_ids that already exist in bronze for the range."""
    result = client.query(
        """
        SELECT DISTINCT a
        FROM trades.binance_agg_trades_bronze
        WHERE s = {symbol:String}
          AND a BETWEEN {from_id:Int64} AND {to_id:Int64}
        ORDER BY a
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


# ---------------------------------------------------------------------------
# Header helpers and rate-limit state
# ---------------------------------------------------------------------------
def get_header(headers, name):
    """Case-insensitive header lookup that also works for plain dicts."""
    if headers is None:
        return None
    wanted = name.lower()
    for key, value in headers.items():
        if str(key).lower() == wanted:
            return value
    return None


def _to_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def sync_binance_server_time(http_get=requests.get, clock=time.time):
    """Measures the offset to Binance server time and re-aligns the weight window."""
    started = clock()

    response = http_get(TIME_URL, timeout=10)
    response.raise_for_status()

    finished = clock()

    server_timestamp = float(response.json()["serverTime"]) / 1000
    midpoint = (started + finished) / 2

    rate_limit_state["server_offset"] = server_timestamp - midpoint
    rate_limit_state["last_sync"] = finished

    reset_weight_window(server_timestamp)

    used = _to_float(get_header(response.headers, USED_WEIGHT_HEADER))
    if used is not None:
        rate_limit_state["reported_weight"] = max(
            rate_limit_state["reported_weight"], int(used)
        )


def reset_weight_window(server_timestamp):
    """A new server minute starts a fresh weight window."""
    current_window = int(server_timestamp // 60)

    if current_window != rate_limit_state["window"]:
        rate_limit_state["window"] = current_window
        rate_limit_state["reserved_weight"] = 0
        rate_limit_state["reported_weight"] = 0
        rate_limit_state["pending_weight"] = 0


def seconds_to_window_end(clock=time.time):
    """Seconds until the next server minute starts (plus a small margin)."""
    server_now = clock() + rate_limit_state["server_offset"]
    return 60 - (server_now % 60) + 0.25


def acquire_request_weight(
    weight,
    sleep=time.sleep,
    clock=time.time,
    http_get=requests.get,
    deadline=None,
    mono=time.monotonic,
):
    """Reserves `weight` in the current minute, or waits for the next window.

    Server time is re-synced at most once per minute. Raises TimeoutError if
    waiting for the next window would run past the runtime deadline.
    """
    while True:
        now = clock()
        last_sync = rate_limit_state["last_sync"]

        if last_sync is None or now - last_sync >= SERVER_TIME_SYNC_INTERVAL_SEC:
            try:
                sync_binance_server_time(http_get=http_get, clock=clock)
            except requests.RequestException as exc:
                if is_rate_limited(exc):
                    raise  # even /time is limited: stop, do not make it worse
                logging.warning("Binance time sync failed: %s. Using previous offset.", exc)
                rate_limit_state["last_sync"] = clock()  # retry in a minute, not per request
            now = clock()

        server_now = now + rate_limit_state["server_offset"]
        reset_weight_window(server_now)

        effective_weight = (
            rate_limit_state["reported_weight"] + rate_limit_state["pending_weight"]
        )

        if effective_weight + weight <= REQUEST_WEIGHT_BUDGET:
            rate_limit_state["reserved_weight"] += weight
            rate_limit_state["pending_weight"] += weight
            return

        wait = 60 - (server_now % 60) + 0.25

        if deadline is not None and mono() + wait >= deadline:
            raise TimeoutError("Not enough runtime left to wait for the next weight window")

        logging.info(
            "Binance REQUEST_WEIGHT budget reached: %s/%s. Waiting %.2fs.",
            effective_weight,
            REQUEST_WEIGHT_BUDGET,
            wait,
        )
        sleep(wait)


def release_request_weight(weight=AGG_TRADES_WEIGHT):
    """Gives back a reservation for a request that never got an answer."""
    rate_limit_state["pending_weight"] = max(
        0, rate_limit_state["pending_weight"] - weight
    )


def update_request_weight(headers, weight=AGG_TRADES_WEIGHT):
    """Replaces the reservation with the real value Binance reports."""
    release_request_weight(weight)

    used = _to_float(get_header(headers, USED_WEIGHT_HEADER))
    if used is not None:
        rate_limit_state["reported_weight"] = max(
            rate_limit_state["reported_weight"], int(used)
        )
    else:
        # No header: assume the request cost what we reserved.
        rate_limit_state["reported_weight"] += weight


def is_rate_limited(error):
    """True, if HTTPError is 429/418 (we need to stop, if not, ban!)."""
    response = getattr(error, "response", None)
    return response is not None and response.status_code in RATE_LIMIT_CODES


def _check_deadline(deadline, clock):
    if deadline is not None and clock() >= deadline:
        raise TimeoutError("Backfill runtime deadline exceeded")


def _sleep_within_deadline(delay, sleep, deadline, clock):
    if deadline is not None and clock() + delay >= deadline:
        raise TimeoutError("Insufficient runtime remaining for Binance retry")
    sleep(delay)


def guarded_get(
    url,
    params,
    http_get=requests.get,
    sleep=time.sleep,
    deadline=None,
    clock=time.monotonic,
    wall_clock=time.time,
):
    """GET with weight budgeting and reaction to Binance errors.

    - Before each request: reserve weight, wait for the next server minute if
      the budget is used up.
    - 429/418: wait Retry-After (429 without it: until the next minute) and
      repeat the same request. If the wait is longer than
      MAX_RATE_LIMIT_WAIT_SEC the HTTPError is raised (418 without
      Retry-After too); if it does not fit in the runtime, TimeoutError.
    - 5xx / network errors: a few retries with growing pauses.
    """
    rate_retries = 0
    transient_retries = 0

    while True:
        _check_deadline(deadline, clock)

        acquire_request_weight(
            AGG_TRADES_WEIGHT,
            sleep=sleep,
            clock=wall_clock,
            http_get=http_get,
            deadline=deadline,
            mono=clock,
        )

        try:
            resp = http_get(url, params=params, timeout=10)
        except requests.RequestException as exc:
            release_request_weight()
            transient_retries += 1
            if transient_retries > MAX_TRANSIENT_RETRIES:
                raise
            delay = TRANSIENT_BACKOFF_BASE_SEC ** transient_retries
            logging.warning("Binance request failed: %s. Retry in %ss.", exc, delay)
            _sleep_within_deadline(delay, sleep, deadline, clock)
            continue

        update_request_weight(resp.headers)

        try:
            resp.raise_for_status()
        except requests.HTTPError as exc:
            status = getattr(exc.response, "status_code", None)

            if status in RATE_LIMIT_CODES:
                delay = _to_float(get_header(resp.headers, "Retry-After"))
                if delay is None or delay <= 0:
                    if status == 418:
                        raise  # a ban without a known duration: stop
                    delay = seconds_to_window_end(wall_clock)

                rate_retries += 1
                if rate_retries > MAX_429_RETRIES or delay > MAX_RATE_LIMIT_WAIT_SEC:
                    raise

                logging.warning("Binance HTTP %s. Waiting %.2fs before retry.", status, delay)
                _sleep_within_deadline(delay, sleep, deadline, clock)
                continue

            if isinstance(status, int) and 500 <= status < 600:
                transient_retries += 1
                if transient_retries > MAX_TRANSIENT_RETRIES:
                    raise
                delay = TRANSIENT_BACKOFF_BASE_SEC ** transient_retries
                logging.warning("Binance HTTP %s. Retry in %ss.", status, delay)
                _sleep_within_deadline(delay, sleep, deadline, clock)
                continue

            raise

        sleep(MIN_DELAY_SEC)
        return resp


def fetch_gap_trades(
        symbol,
        from_id,
        to_id,
        http_get=requests.get,
        max_trades=CHUNK_SIZE,
        sleep=time.sleep,
        deadline=None,
        clock=time.monotonic,
):
    """Pages through REST aggTrades with fromId until to_id is reached."""
    trades = []
    cursor = from_id
    while cursor <= to_id and len(trades) < max_trades:
        # fromId cannot be combined with startTime/endTime in this endpoint,
        # which is fine: our gaps are defined by id, not by time.
        resp = guarded_get(
            AGG_TRADES_URL,
            {"symbol": symbol, "fromId": cursor, "limit": REST_PAGE_LIMIT},
            http_get=http_get, sleep=sleep, deadline=deadline, clock=clock,
        )
        batch = resp.json()
        if not batch:
            break  # nothing newer on the exchange
        for raw in batch:
            if raw["a"] > to_id:
                return trades  # page overshoots the gap
            trades.append(to_trade(raw, symbol))
        cursor = batch[-1]["a"] + 1
    return trades


def get_binance_data(gaps, http_get=requests.get, max_trades=CHUNK_SIZE,
                     sleep=time.sleep, deadline=None, clock=time.monotonic):
    """Fetches trades for gaps in order, sharing one budget across all gaps."""
    trades = []
    for gap in gaps:
        remaining = max_trades - len(trades)
        if remaining <= 0:
            break  # leftover gaps are picked up by the next run
        trades.extend(
            fetch_gap_trades(
                gap["symbol"],
                gap["from_id"],
                gap["to_id"],
                http_get=http_get,
                max_trades=remaining,
                sleep=sleep,
                deadline=deadline,
                clock=clock,
            )
        )

    return trades


def validate_trades(trades, model):
    """Pure function: splits trades into (valid, invalid) using the stream's model.

    Invalid trades are not fatal: their ids stay missing in bronze, so the
    next gap check will see them as unfilled gaps instead of hiding them.
    """
    valid, invalid = [], []
    for trade in trades:
        try:
            valid.append(model.model_validate(trade).model_dump())
        except Exception as e:
            invalid.append({"trade": trade, "error": str(e)})
    return valid, invalid


def filter_new_trades(client, trades):
    """Drops trades whose id already exists in bronze.

    Gold MVs use sum/count, so a duplicate would be counted twice and cannot
    be fixed by ReplacingMergeTree.
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


def wait_for_silver(client, ranges, sleep=time.sleep, interval_sec=15, max_attempts=6,
                    deadline=None, clock=time.monotonic):
    """Polls find_gaps (reads bronze) until published trades arrive or attempts run out.

    The consumer flushes every BATCH_TIMEOUT_SEC (30s), so checking right
    after publish would report gaps that are merely in flight. Total wait
    (6 x 15s) is deliberately longer than that flush interval. Stops early
    if the runtime deadline would be exceeded; the watermark then simply
    does not move past the unconfirmed gap.
    """
    gaps = find_gaps(client, ranges)
    for _ in range(max_attempts - 1):
        if not gaps:
            break
        if deadline is not None and clock() + interval_sec >= deadline:
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


def split_into_chunks(ranges, chunk_size):
    """Pure function: cuts each checked range into ascending chunks of <= chunk_size ids.

    A chunk is the unit of find gaps -> fetch -> publish -> verify -> watermark.
    """
    chunks = []
    for r in sorted(ranges, key=lambda r: (r["symbol"], r["from_id"])):
        start = r["from_id"]
        while start <= r["to_id"]:
            end = min(r["to_id"], start + chunk_size - 1)
            chunks.append({"symbol": r["symbol"], "from_id": start, "to_id": end})
            start = end + 1
    return chunks


def process_ranges(client, producer, serialize, topic, model, ranges, run_id, *,
                   http_get=requests.get, sleep=time.sleep, clock=time.monotonic,
                   chunk_size=CHUNK_SIZE, max_runtime_sec=MAX_RUNTIME_SEC,
                   verify_interval_sec=15, verify_attempts=6):
    """Per chunk: find gaps -> fetch -> validate -> publish -> verify -> move watermark.

    A chunk with no gaps still moves the watermark to its end: find_gaps has
    just scanned the whole chunk, so empty result = proven complete.
    The run stops cleanly (watermark stays on the last confirmed chunk) when
    time runs out or Binance asks for a wait longer than the allowed maximum.
    Only counters are returned (small XCom payload).
    """
    started = clock()
    deadline = started + max_runtime_sec
    stats = {"chunks_done": 0, "gaps_found": 0, "published": 0, "rejected": 0,
             "remaining_gaps": 0, "stopped_by_time": False,
             "rate_limited": False, "retry_after_sec": None,
             "rate_limit_status": None}
    blocked = set()  # symbol with an unfilled gap: later chunks must not move its watermark

    for chunk in split_into_chunks(ranges, chunk_size):
        symbol = chunk["symbol"]
        if symbol in blocked:
            continue
        # Checked between chunks, never inside one.
        if clock() - started >= max_runtime_sec:
            stats["stopped_by_time"] = True
            break

        window = [chunk]
        gaps = find_gaps(client, window)  # only this chunk's ids are loaded in memory
        valid, invalid = [], []

        if gaps:
            try:
                trades = get_binance_data(gaps, http_get=http_get,
                                          max_trades=chunk_size, sleep=sleep,
                                          deadline=deadline, clock=clock)
            except requests.HTTPError as e:
                if not is_rate_limited(e):
                    raise
                stats["rate_limited"] = True
                stats["retry_after_sec"] = get_header(e.response.headers, "Retry-After")
                stats["rate_limit_status"] = e.response.status_code
                break
            except TimeoutError:
                stats["stopped_by_time"] = True
                break
            valid, invalid = validate_trades(trades, model)
            publish_trades(valid, producer, serialize, topic)

            if valid:
                remaining = wait_for_silver(client, window, sleep,
                                            verify_interval_sec, verify_attempts,
                                            deadline=deadline, clock=clock)
            else:
                remaining = find_gaps(client, window)
        else:
            remaining = []  # nothing missing: watermark goes to the chunk end

        watermarks = compute_new_watermarks(window, remaining)
        save_watermarks(client, watermarks, gaps, remaining, run_id)

        stats["chunks_done"] += 1
        stats["gaps_found"] += len(gaps)
        stats["published"] += len(valid)
        stats["rejected"] += len(invalid)
        stats["remaining_gaps"] += len(remaining)
        if remaining:
            blocked.add(symbol)

    return stats