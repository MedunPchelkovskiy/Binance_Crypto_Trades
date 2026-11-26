"""End-to-end backfill: gap detection -> REST fetch -> publish -> bronze -> watermark.

Real: ClickHouse (Docker), HTTP calls (to a local fake Binance), check_data,
process_ranges, validation (Trade model), watermark state.
Simulated: Kafka + ingestion consumer (IngestingProducer writes straight to
bronze on flush). Kafka/Avro itself needs a separate docker-compose smoke test.

Run only these:      pytest -m integration
Run all but these:   pytest -m "not integration"
(register the marker: markers = integration: needs Docker, in pytest.ini)

Data: exchange has ids 1..100; bronze is missing 21-30 and 61-65.
With chunk_size=40 the chunks are 1-40 (gap 21-30), 41-80 (gap 61-65), 81-100.
"""
import json
import time
from types import SimpleNamespace

import pytest
import requests

from airflow.dags import backfill_logic
from airflow.dags.backfill_logic import check_data, fetch_existing_ids, fetch_watermarks, process_ranges
from ingestion.validation import Trade

from backfill_fakes import BRONZE_COLUMNS, BRONZE_TABLE, FakeBinance, IngestingProducer, bronze_row, raw_trade

pytestmark = pytest.mark.integration

SYMBOL = "BTCUSDT"
TOTAL_IDS = 100
MISSING = list(range(21, 31)) + list(range(61, 66))
CHUNK = 40


@pytest.fixture
def env(ch, monkeypatch):
    # trades are 1 h old, so they are outside the 5 min safety margin
    base_ms = int(time.time() * 1000) - 3_600_000

    server = FakeBinance([raw_trade(i, base_ms + i) for i in range(1, TOTAL_IDS + 1)])
    server.start()
    monkeypatch.setattr(backfill_logic, "AGG_TRADES_URL", f"{server.url}/api/v3/aggTrades")
    monkeypatch.setattr(backfill_logic, "TIME_URL", f"{server.url}/api/v3/time")

    streamed = [i for i in range(1, TOTAL_IDS + 1) if i not in MISSING]
    ch.insert(BRONZE_TABLE, [bronze_row(SYMBOL, i, base_ms + i) for i in streamed],
              column_names=BRONZE_COLUMNS)

    yield SimpleNamespace(ch=ch, server=server, producer=IngestingProducer(ch), sleeps=[])
    server.stop()


def run_backfill(env, run_id="run-1", **kwargs):
    ranges = check_data(env.ch, [SYMBOL.lower()])
    return process_ranges(
        env.ch, env.producer, lambda t: json.dumps(t).encode(), "trade_streams_avro_dev",
        Trade, ranges, run_id,
        http_get=requests.get, sleep=env.sleeps.append, chunk_size=CHUNK, **kwargs,
    )


def bronze_ids(env):
    return fetch_existing_ids(env.ch, SYMBOL, 0, 10**12)


def bronze_row_count(env):
    result = env.ch.query(
        f"SELECT count() FROM {BRONZE_TABLE} WHERE s = {{symbol:String}}",
        parameters={"symbol": SYMBOL},
    )
    return result.result_rows[0][0]


def test_gaps_are_filled_and_watermark_reaches_the_end(env):
    stats = run_backfill(env)

    assert stats["chunks_done"] == 3
    assert stats["gaps_found"] == 2
    assert stats["published"] == len(MISSING)
    assert stats["rejected"] == 0 and stats["remaining_gaps"] == 0
    assert stats["rate_limited"] is False and stats["stopped_by_time"] is False

    assert bronze_ids(env) == list(range(1, TOTAL_IDS + 1))
    assert bronze_row_count(env) == TOTAL_IDS  # nothing published twice
    assert fetch_watermarks(env.ch, [SYMBOL]) == {SYMBOL: TOTAL_IDS}

    assert sorted(int(m["key"]) for m in env.producer.delivered) == MISSING
    assert all(m["headers"]["source"] == b"backfill" for m in env.producer.delivered)


def test_second_run_has_nothing_to_do(env):
    run_backfill(env, run_id="run-1")
    requests_before = len(env.server.agg_requests)

    stats = run_backfill(env, run_id="run-2")

    assert stats["chunks_done"] == 0 and stats["published"] == 0
    assert len(env.server.agg_requests) == requests_before
    assert bronze_row_count(env) == TOTAL_IDS


def test_interrupted_run_keeps_confirmed_watermark_and_next_run_resumes(env):
    env.server.fail_after = 1  # chunk 1 is served, then Binance answers 429 / Retry-After 600

    first = run_backfill(env, run_id="run-1")

    assert first["rate_limited"] is True
    assert first["rate_limit_status"] == 429 and first["retry_after_sec"] == "600"
    assert first["chunks_done"] == 1 and first["published"] == 10
    assert fetch_watermarks(env.ch, [SYMBOL]) == {SYMBOL: 40}  # only the confirmed chunk
    assert set(MISSING[:10]) <= set(bronze_ids(env))  # 21-30 are in
    assert not set(range(61, 66)) & set(bronze_ids(env))  # 61-65 are still missing

    env.server.fail_after = None
    requests_before = len(env.server.agg_requests)

    second = run_backfill(env, run_id="run-2")

    assert second["rate_limited"] is False and second["published"] == 5
    assert [r["from_id"] for r in env.server.agg_requests[requests_before:]] == [61]  # chunk 1 not refetched
    assert bronze_ids(env) == list(range(1, TOTAL_IDS + 1))
    assert bronze_row_count(env) == TOTAL_IDS  # no duplicates after resuming
    assert fetch_watermarks(env.ch, [SYMBOL]) == {SYMBOL: TOTAL_IDS}


def test_short_429_is_waited_out_and_the_run_completes(env):
    env.server.script = [(429, "5")]

    stats = run_backfill(env)

    assert stats["rate_limited"] is False
    assert stats["chunks_done"] == 3 and stats["published"] == len(MISSING)
    assert 5.0 in env.sleeps
    # the 429'd request was repeated unchanged, then chunk 2
    assert [r["from_id"] for r in env.server.agg_requests] == [21, 21, 61]
    assert bronze_row_count(env) == TOTAL_IDS


def test_ban_418_stops_at_once_and_moves_nothing(env):
    env.server.script = [(418, "3600")]
    rows_before = bronze_row_count(env)

    stats = run_backfill(env)

    assert stats["rate_limited"] is True and stats["rate_limit_status"] == 418
    assert stats["retry_after_sec"] == "3600"
    assert stats["chunks_done"] == 0
    assert len(env.server.agg_requests) == 1  # no hammering during a ban
    assert bronze_row_count(env) == rows_before
    assert fetch_watermarks(env.ch, [SYMBOL]) == {}


def test_wait_longer_than_remaining_runtime_stops_by_time(env):
    env.server.script = [(429, "60")]  # below the 120 s threshold, but run has only 30 s

    stats = run_backfill(env, max_runtime_sec=30)

    assert stats["stopped_by_time"] is True and stats["rate_limited"] is False
    assert stats["chunks_done"] == 0
    assert len(env.server.agg_requests) == 1
    assert fetch_watermarks(env.ch, [SYMBOL]) == {}
