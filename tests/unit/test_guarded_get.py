from unittest.mock import MagicMock

import pytest
import requests
from requests.structures import CaseInsensitiveDict

from airflow.dags.backfill_logic import (
    guarded_get, is_rate_limited,
    WEIGHT_COOLDOWN_SEC, WEIGHT_SOFT_LIMIT, MIN_DELAY_SEC,
)


def make_response(status=200, headers=None):
    """Фалшив отговор на Binance."""
    resp = MagicMock()
    resp.status_code = status
    resp.headers = CaseInsensitiveDict(headers or {})
    if status >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
    return resp


def run(resp):
    sleeps = []
    result = guarded_get("http://x", {}, http_get=lambda *a, **k: resp,
                         sleep=sleeps.append)
    return result, sleeps


# --- Нормален случай: връща отговора и прави само кратка пауза ---
def test_normal_response_returned_with_min_delay():
    resp = make_response(200, {"X-MBX-USED-WEIGHT-1m": "10"})
    result, sleeps = run(resp)
    assert result is resp
    assert sleeps == [MIN_DELAY_SEC]


# --- Липсва weight header: третира се като 0 ---
def test_missing_weight_header_is_ok():
    _, sleeps = run(make_response(200))
    assert sleeps == [MIN_DELAY_SEC]


# --- 429: хвърля HTTPError, разпознава се като rate limit, не чака ---
def test_429_raises_and_is_rate_limited():
    with pytest.raises(requests.HTTPError) as exc:
        run(make_response(429, {"Retry-After": "30"}))
    assert is_rate_limited(exc.value)


# --- 418 (бан) се третира като 429 ---
def test_418_is_rate_limited():
    with pytest.raises(requests.HTTPError) as exc:
        run(make_response(418))
    assert is_rate_limited(exc.value)


# --- Висок weight: дълга пауза вместо кратка ---
def test_high_weight_triggers_cooldown():
    resp = make_response(200, {"X-MBX-USED-WEIGHT-1m": str(WEIGHT_SOFT_LIMIT + 1)})
    _, sleeps = run(resp)
    assert sleeps == [WEIGHT_COOLDOWN_SEC]


# --- 500 не е rate limit: хвърля, но is_rate_limited е False ---
def test_500_raises_but_is_not_rate_limited():
    with pytest.raises(requests.HTTPError) as exc:
        run(make_response(500))
    assert not is_rate_limited(exc.value)


# --- HTTPError без response не чупи проверката ---
def test_is_rate_limited_without_response():
    assert not is_rate_limited(requests.HTTPError("no response"))