import pytest

from airflow.dags import backfill_logic


@pytest.fixture(autouse=True)
def fresh_rate_limit_state(monkeypatch):
    """Isolates the module-level rate limit state between tests."""
    monkeypatch.setattr(backfill_logic, "rate_limit_state", {
        "server_offset": 0.0,
        "last_sync": None,
        "window": None,
        "reserved_weight": 0,
        "reported_weight": 0,
        "pending_weight": 0,
    })
