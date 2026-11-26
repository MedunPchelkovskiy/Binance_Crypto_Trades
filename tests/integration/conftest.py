"""ClickHouse in Docker for the integration tests.

Needs: pip install testcontainers clickhouse-connect  (and a running Docker).
Without Docker the tests are skipped, not failed.

The tables below are a MINIMAL copy of the real schema: they contain only the
columns the backfill code reads or writes. If the real DDL differs (types,
engine, ORDER BY), align it here.
"""
import time

import pytest

IMAGE = "clickhouse/clickhouse-server:24.8"
USER = PASSWORD = "test"

DDL = [
    "CREATE DATABASE IF NOT EXISTS trades",
    """
    CREATE TABLE IF NOT EXISTS trades.binance_agg_trades_bronze (
        e String, E Int64, s String, a Int64, p String, q String,
        f Int64, l Int64, T Int64, m Bool, M Bool
    ) ENGINE = MergeTree ORDER BY (s, a)
    """,
    # microsecond updated_at: two chunks finished in the same millisecond must
    # not tie in argMax(checked_up_to_id, updated_at)
    """
    CREATE TABLE IF NOT EXISTS trades.backfill_state (
        symbol String, checked_up_to_id Int64, last_run_id String,
        gaps_found Int64, gaps_filled Int64,
        updated_at DateTime64(6) DEFAULT now64(6)
    ) ENGINE = MergeTree ORDER BY (symbol, updated_at)
    """,
]


def _connect(host, port, timeout_sec=90):
    # tests/conftest.py replaces clickhouse_connect.get_client with a MagicMock
    # for the unit tests; create_client is the real function behind it and is
    # not patched, so the integration tests get a genuine client.
    from clickhouse_connect.driver import create_client

    deadline = time.monotonic() + timeout_sec
    while True:
        try:
            return create_client(host=host, port=port,
                                 username=USER, password=PASSWORD)
        except Exception:
            if time.monotonic() > deadline:
                raise
            time.sleep(1)


@pytest.fixture(scope="session")
def _clickhouse():
    container_module = pytest.importorskip("testcontainers.core.container")
    pytest.importorskip("clickhouse_connect")

    container = (
        container_module.DockerContainer(IMAGE)
        .with_exposed_ports(8123)
        .with_env("CLICKHOUSE_USER", USER)
        .with_env("CLICKHOUSE_PASSWORD", PASSWORD)
    )
    try:
        container.start()
    except Exception as exc:  # no Docker daemon, image cannot be pulled, ...
        pytest.skip(f"Docker/ClickHouse container is not available: {exc}")

    try:
        client = _connect(container.get_container_host_ip(),
                          int(container.get_exposed_port(8123)))
        for ddl in DDL:
            client.command(ddl)
        yield client
    finally:
        container.stop()


@pytest.fixture
def ch(_clickhouse):
    """ClickHouse client with empty bronze and state tables."""
    _clickhouse.command("TRUNCATE TABLE trades.binance_agg_trades_bronze")
    _clickhouse.command("TRUNCATE TABLE trades.backfill_state")
    return _clickhouse