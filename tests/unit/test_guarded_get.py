from unittest.mock import MagicMock

import pytest
import requests
from requests.structures import CaseInsensitiveDict

from airflow.dags import backfill_logic
from airflow.dags.backfill_logic import (
    guarded_get, is_rate_limited,
    MIN_DELAY_SEC, MAX_RATE_LIMIT_WAIT_SEC, MAX_429_RETRIES,
    MAX_TRANSIENT_RETRIES, TRANSIENT_BACKOFF_BASE_SEC, REQUEST_WEIGHT_BUDGET,
)

SERVER_TIME_MS = 1_700_000_000_000
# 1_700_000_000 s % 60 == 20 -> the next server minute starts in 40.25 s (with margin)
SECONDS_TO_NEXT_WINDOW = 40.25


def make_response(status=200, headers=None):
    """Фалшив отговор на Binance."""
    resp = MagicMock()
    resp.status_code = status
    resp.headers = CaseInsensitiveDict(headers or {})
    resp.json.return_value = {"serverTime": SERVER_TIME_MS}
    if status >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
    return resp


class FakeTime:
    """Wall clock that moves forward whenever the code sleeps."""

    def __init__(self, now=SERVER_TIME_MS / 1000):
        self.now = now
        self.sleeps = []

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def make_get(*items, time_response=None, time_error=None):
    """Answers /time with `time_response`; other calls consume `items` in order.

    An item that is an Exception is raised instead of returned.
    """
    queue = iter(items)
    calls = {"time": 0, "trades": 0}

    def get(url, **kwargs):
        if url.endswith("/api/v3/time"):
            calls["time"] += 1
            if time_error is not None:
                raise time_error
            return time_response or make_response(200)
        calls["trades"] += 1
        item = next(queue)
        if isinstance(item, Exception):
            raise item
        return item

    get.calls = calls
    return get


def run(*items, deadline=None, clock=None, **get_kwargs):
    t = FakeTime()
    get = make_get(*items, **get_kwargs)
    kwargs = {"deadline": deadline}
    if clock is not None:
        kwargs["clock"] = clock
    result = guarded_get("http://x/aggTrades", {}, http_get=get,
                         sleep=t.sleep, wall_clock=t.clock, **kwargs)
    return result, t.sleeps, get


# ---------- normal flow ----------

def test_normal_response_returned_with_min_delay():
    resp = make_response(200, {"X-MBX-USED-WEIGHT-1m": "10"})
    result, sleeps, _ = run(resp)
    assert result is resp
    assert sleeps == [MIN_DELAY_SEC]


def test_missing_weight_header_is_ok():
    _, sleeps, _ = run(make_response(200))
    assert sleeps == [MIN_DELAY_SEC]


def test_reported_weight_is_stored_and_reservation_released():
    run(make_response(200, {"X-MBX-USED-WEIGHT-1m": "10"}))
    state = backfill_logic.rate_limit_state
    assert state["reported_weight"] == 10
    assert state["pending_weight"] == 0


# ---------- 429 ----------

def test_429_waits_retry_after_and_repeats_same_request():
    ok = make_response(200)
    result, sleeps, get = run(make_response(429, {"Retry-After": "5"}), ok)
    assert result is ok
    assert sleeps == [5.0, MIN_DELAY_SEC]
    assert get.calls["trades"] == 2


def test_429_without_retry_after_waits_until_next_minute():
    _, sleeps, _ = run(make_response(429), make_response(200))
    assert sleeps[0] == pytest.approx(SECONDS_TO_NEXT_WINDOW)


def test_429_retry_after_over_threshold_raises_without_waiting():
    with pytest.raises(requests.HTTPError) as exc:
        run(make_response(429, {"Retry-After": str(MAX_RATE_LIMIT_WAIT_SEC + 1)}))
    assert is_rate_limited(exc.value)


def test_429_gives_up_after_max_retries():
    responses = [make_response(429, {"Retry-After": "1"}) for _ in range(MAX_429_RETRIES + 1)]
    t = FakeTime()
    get = make_get(*responses)
    with pytest.raises(requests.HTTPError):
        guarded_get("http://x/aggTrades", {}, http_get=get, sleep=t.sleep, wall_clock=t.clock)
    assert t.sleeps == [1.0] * MAX_429_RETRIES


# ---------- 418 ----------

def test_418_within_threshold_waits_and_retries():
    ok = make_response(200)
    result, sleeps, _ = run(make_response(418, {"Retry-After": "30"}), ok)
    assert result is ok
    assert sleeps == [30.0, MIN_DELAY_SEC]


def test_418_over_threshold_raises_and_is_rate_limited():
    with pytest.raises(requests.HTTPError) as exc:
        run(make_response(418, {"Retry-After": "3600"}))
    assert is_rate_limited(exc.value)


def test_418_without_retry_after_raises():
    with pytest.raises(requests.HTTPError) as exc:
        run(make_response(418))
    assert is_rate_limited(exc.value)


# ---------- 5xx / network ----------

def test_500_is_retried_with_backoff_then_succeeds():
    ok = make_response(200)
    result, sleeps, _ = run(make_response(500), ok)
    assert result is ok
    assert sleeps == [TRANSIENT_BACKOFF_BASE_SEC, MIN_DELAY_SEC]


def test_500_after_all_retries_raises_and_is_not_rate_limited():
    t = FakeTime()
    get = make_get(*[make_response(500) for _ in range(MAX_TRANSIENT_RETRIES + 1)])
    with pytest.raises(requests.HTTPError) as exc:
        guarded_get("http://x/aggTrades", {}, http_get=get, sleep=t.sleep, wall_clock=t.clock)
    assert not is_rate_limited(exc.value)
    assert t.sleeps == [TRANSIENT_BACKOFF_BASE_SEC ** n for n in range(1, MAX_TRANSIENT_RETRIES + 1)]


def test_400_raises_immediately_without_retry():
    with pytest.raises(requests.HTTPError):
        run(make_response(400))


def test_network_error_is_retried_and_reservation_released():
    ok = make_response(200)
    result, _, get = run(requests.ConnectionError("down"), ok)
    assert result is ok
    assert get.calls["trades"] == 2
    assert backfill_logic.rate_limit_state["pending_weight"] == 0


def test_network_error_after_all_retries_raises():
    errors = [requests.ConnectionError("down") for _ in range(MAX_TRANSIENT_RETRIES + 1)]
    with pytest.raises(requests.ConnectionError):
        run(*errors)


# ---------- runtime deadline ----------

def test_deadline_already_passed_makes_no_request():
    t = FakeTime()
    get = make_get()
    with pytest.raises(TimeoutError):
        guarded_get("http://x/aggTrades", {}, http_get=get, sleep=t.sleep,
                    wall_clock=t.clock, deadline=0, clock=lambda: 1)
    assert get.calls == {"time": 0, "trades": 0}


def test_retry_wait_that_does_not_fit_in_runtime_raises_timeout():
    with pytest.raises(TimeoutError):
        run(make_response(429, {"Retry-After": "60"}), deadline=10, clock=lambda: 0)


# ---------- weight budget and time sync ----------

def test_budget_reached_waits_for_next_window_before_requesting():
    used = make_response(200, {"X-MBX-USED-WEIGHT-1M": str(REQUEST_WEIGHT_BUDGET)})
    _, sleeps, get = run(make_response(200), time_response=used)
    assert sleeps == [pytest.approx(SECONDS_TO_NEXT_WINDOW), MIN_DELAY_SEC]
    assert get.calls["trades"] == 1


def test_time_is_synced_once_per_minute():
    t = FakeTime()
    get = make_get(make_response(200), make_response(200), make_response(200))

    def call():
        guarded_get("http://x/aggTrades", {}, http_get=get, sleep=t.sleep, wall_clock=t.clock)

    call()
    call()
    assert get.calls["time"] == 1

    t.now += 61
    call()
    assert get.calls["time"] == 2


def test_failed_time_sync_does_not_block_requests():
    ok = make_response(200)
    result, _, get = run(ok, time_error=requests.ConnectionError("no time"))
    assert result is ok
    assert get.calls["time"] == 1


def test_rate_limited_time_sync_stops_before_any_request():
    limited = make_response(429, {"Retry-After": "30"})
    with pytest.raises(requests.HTTPError) as exc:
        run(make_response(200), time_response=limited)
    assert is_rate_limited(exc.value)


def test_is_rate_limited_without_response():
    assert not is_rate_limited(requests.HTTPError("no response"))
