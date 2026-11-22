# tests/integration/test_pipeline_integration.py
"""
End-to-end integration tests against the real stack (Kafka KRaft, Schema Registry,
ClickHouse, MinIO) started from docker-compose.test.yml, running REAL consumer code.

Prerequisites:
  * consumers' main() accepts `stop_event` (see chat)
  * `pytest.ini` registers the `integration` marker
  * Docker Compose v2 (`docker compose`); the dev stack may keep running (other ports)

Run: pytest tests/integration -m integration
"""

import io
import json
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path

import boto3
import clickhouse_connect
import fastavro
import pytest
import requests
from confluent_kafka.serialization import SerializationContext, MessageField
from testcontainers.compose import DockerCompose

pytestmark = pytest.mark.integration

KAFKA = "localhost:19092"
REGISTRY = "http://localhost:18081"
CH_PORT = 18123
MINIO = "http://localhost:19000"
MINIO_KEY = "testaccesskey"
MINIO_SECRET = "testsecretkey123"

SILVER_DDL = """
CREATE TABLE IF NOT EXISTS trades.binance_agg_trades_silver
(
    event_type       LowCardinality(String),
    event_time       DateTime64(3, 'UTC'),
    symbol           LowCardinality(String),
    agg_trade_id     Int64,
    price            Decimal64(8),
    quantity         Decimal64(8),
    first_trade_id   Int64,
    last_trade_id    Int64,
    trade_time       DateTime64(3, 'UTC'),
    is_buyer_maker   Bool,
    is_best_match    Bool,
    ingested_at      DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(trade_time)
ORDER BY (symbol, trade_time)
"""

# Test stand-in: the project has no DDL for silver_metrics (columns taken from
# metrics_to_clickhouse.METRICS_COLUMNS). Replace with the real DDL when it exists.
SILVER_METRICS_DDL = """
CREATE TABLE IF NOT EXISTS trades.silver_metrics
(
    layer             String,
    batch_id          String,
    batch_timestamp   DateTime64(3, 'UTC'),
    batch_duration_ms Float64,
    records_processed UInt64,
    avg_latency_ms    Float64,
    max_latency_ms    Float64,
    p95_latency_ms    Float64
)
ENGINE = MergeTree
ORDER BY batch_timestamp
"""


# ---------------------------------------------------------------- helpers

def wait_until(predicate, timeout=60, interval=1.0):
    """Polls instead of a fixed sleep: fast when things work, bounded when they don't."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = predicate()
        except Exception:
            result = None
        if result:
            return result
        time.sleep(interval)
    return predicate()


def http_ok(url):
    return requests.get(url, timeout=2).status_code == 200


# ---------------------------------------------------------------- fixtures

@pytest.fixture(scope="session")
def stack():
    """Starts the whole stack ONCE per session (per-test startup would take minutes)."""
    with DockerCompose(
        context=str(Path(__file__).parent),
        compose_file_name="docker-compose.test.yml",
        pull=False,
    ):
        # Schema Registry answers only after it has connected to Kafka,
        # so it is a good readiness signal for both.
        assert wait_until(lambda: http_ok(f"{REGISTRY}/subjects"), timeout=120), "Schema Registry not ready"
        assert wait_until(lambda: http_ok(f"http://localhost:{CH_PORT}/ping"), timeout=120), "ClickHouse not ready"
        assert wait_until(lambda: http_ok(f"{MINIO}/minio/health/live"), timeout=120), "MinIO not ready"
        yield


@pytest.fixture(scope="session", autouse=True)
def env(stack):
    """Points production code (python-decouple reads os.environ first) at the test stack."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("KAFKA_BROKER_ADDRESS", KAFKA)
        mp.setenv("SCHEMA_REGISTRY_URL", REGISTRY)
        mp.setenv("CLICKHOUSE_HOST", "localhost")
        mp.setenv("CLICKHOUSE_PORT", str(CH_PORT))
        mp.setenv("CLICKHOUSE_USER", "default")
        mp.setenv("CLICKHOUSE_PASSWORD", "")
        mp.setenv("CLICKHOUSE_DATABASE", "trades")
        mp.setenv("MINIO_ENDPOINT", MINIO)
        mp.setenv("MINIO_ACCESS_KEY", MINIO_KEY)
        mp.setenv("MINIO_SECRET_KEY", MINIO_SECRET)
        yield


@pytest.fixture(autouse=True)
def real_clickhouse_client(monkeypatch):
    """The root conftest.py replaces clickhouse_connect.get_client with a MagicMock
    (right for unit tests, fatal here), so restore the real factory for these tests."""
    monkeypatch.setattr(clickhouse_connect, "get_client", clickhouse_connect.driver.create_client)


@pytest.fixture(scope="session")
def ch_client(env):
    client = clickhouse_connect.driver.create_client(
        host="localhost", port=CH_PORT, username="default", password="",
    )
    client.command("CREATE DATABASE IF NOT EXISTS trades")
    client.command(SILVER_DDL)
    client.command(SILVER_METRICS_DDL)
    return client


@pytest.fixture
def topic():
    """Unique topic per test, so tests cannot see each other's messages."""
    return f"it_trades_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def symbol():
    """Unique symbol per test: the silver table is shared, so each test counts its own."""
    return f"T{uuid.uuid4().hex[:6].upper()}"


# ---------------------------------------------------------------- test helpers

def make_trade(a, symbol):
    return {
        "e": "aggTrade", "E": 1789215829640, "s": symbol, "a": a,
        "p": "736.01000000", "q": "0.52400000", "f": a, "l": a,
        "T": 1789215829639, "m": False, "M": True,
    }


def produce_trades(topic, trades):
    from clients.kafka_clients import get_avro_serializer, get_producer

    serializer = get_avro_serializer()
    producer = get_producer()
    for t in trades:
        value = serializer(t, SerializationContext(topic, MessageField.VALUE))
        producer.produce(topic=topic, key=str(t["a"]), value=value)
    assert producer.flush(30) == 0


def produce_raw(topic, payload: bytes):
    from clients.kafka_clients import get_producer

    producer = get_producer()
    producer.produce(topic=topic, key="bad", value=payload)
    assert producer.flush(30) == 0


@contextmanager
def running(main_func):
    """Runs a consumer's main(stop_event) in a thread and stops it cleanly afterwards.
    If main() crashes, the error is re-raised in the test instead of a silent timeout."""
    stop = threading.Event()
    errors = []

    def target():
        try:
            main_func(stop)
        except BaseException as e:  # noqa: BLE001 - surfaced below
            errors.append(e)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(timeout=30)
    if errors:
        raise errors[0]


def silver_count(client, symbol):
    return client.query(
        "SELECT count() FROM trades.binance_agg_trades_silver WHERE symbol = {s:String}",
        parameters={"s": symbol},
    ).result_rows[0][0]


# ---------------------------------------------------------------- tests

def test_silver_happy_path(monkeypatch, topic, symbol, ch_client):
    """N valid trades in Kafka -> N rows in silver with correct types."""
    from consumers import click_house_silver_consumer as silver

    monkeypatch.setattr(silver, "TOPIC", topic)
    monkeypatch.setattr(silver, "CONSUMER_GROUP", f"it-silver-{topic}")

    # Exactly BATCH_SIZE messages: the consumer flushes on size, no 30s timeout wait.
    n = silver.BATCH_SIZE
    produce_trades(topic, [make_trade(i, symbol) for i in range(1, n + 1)])

    with running(silver.main):
        assert wait_until(lambda: silver_count(ch_client, symbol) == n), \
            f"expected {n} rows, got {silver_count(ch_client, symbol)}"

    row = ch_client.query(
        "SELECT symbol, price, quantity, is_buyer_maker "
        "FROM trades.binance_agg_trades_silver WHERE symbol = {s:String} LIMIT 1",
        parameters={"s": symbol},
    ).result_rows[0]
    assert row[0] == symbol
    assert str(row[1]) == "736.01000000"
    assert row[3] is False


def test_invalid_message_goes_to_dlq_and_consumer_keeps_running(monkeypatch, topic, symbol, ch_client):
    """A garbage message must land in the DLQ, not kill the consumer."""
    from clients.kafka_clients import get_consumer
    from consumers import click_house_silver_consumer as silver
    from consumers import dlq

    group = f"it-silver-{topic}"
    monkeypatch.setattr(silver, "TOPIC", topic)
    monkeypatch.setattr(silver, "CONSUMER_GROUP", group)

    produce_raw(topic, b"not-an-avro-message")  # no Schema Registry magic byte
    n = silver.BATCH_SIZE
    produce_trades(topic, [make_trade(i, symbol) for i in range(1, n + 1)])

    with running(silver.main):
        # The valid messages AFTER the bad one must still be processed.
        assert wait_until(lambda: silver_count(ch_client, symbol) == n)

    # The DLQ topic is shared by all tests, so filter by this test's unique source topic.
    dlq_consumer = get_consumer(f"it-dlq-reader-{topic}")
    dlq_consumer.subscribe([dlq.DLQ_TOPIC])

    def find_dlq_payload():
        msg = dlq_consumer.poll(1.0)
        if msg is None or msg.error():
            return None
        payload = json.loads(msg.value())
        return payload if payload["source_topic"] == topic else None

    try:
        payload = wait_until(find_dlq_payload, timeout=30, interval=0)
    finally:
        dlq_consumer.close()

    assert payload, "no DLQ message found for this test's topic"
    assert payload["consumer_group"] == group
    assert payload["original_message"] == b"not-an-avro-message".hex()


@pytest.mark.xfail(reason="silver idempotency (dedup before insert) not implemented yet", strict=False)
def test_duplicate_trade_id_is_stored_once(monkeypatch, topic, symbol, ch_client):
    """At-least-once delivery can repeat a message; silver must not double-count it."""
    from consumers import click_house_silver_consumer as silver

    monkeypatch.setattr(silver, "TOPIC", topic)
    monkeypatch.setattr(silver, "CONSUMER_GROUP", f"it-silver-{topic}")

    n = silver.BATCH_SIZE
    unique = [make_trade(i, symbol) for i in range(1, n)]  # n-1 unique trades
    produce_trades(topic, unique + [make_trade(1, symbol)])  # + 1 duplicate = n messages

    with running(silver.main):
        wait_until(lambda: silver_count(ch_client, symbol) >= n - 1)
        time.sleep(3)  # give a (wrongly) duplicated row time to show up
        assert silver_count(ch_client, symbol) == n - 1


def test_minio_bronze_happy_path(monkeypatch, topic, symbol):
    """N valid trades in Kafka -> Avro OCF file(s) in MinIO containing N records."""
    from consumers import minio_bronze_consumer as bronze

    s3 = boto3.client(
        "s3", endpoint_url=MINIO,
        aws_access_key_id=MINIO_KEY, aws_secret_access_key=MINIO_SECRET,
        region_name="us-east-1",
    )
    bucket = f"it-bronze-{uuid.uuid4().hex[:8]}"
    s3.create_bucket(Bucket=bucket)

    monkeypatch.setattr(bronze, "TOPIC", topic)
    monkeypatch.setattr(bronze, "CONSUMER_GROUP", f"it-bronze-{topic}")
    monkeypatch.setattr(bronze, "BUCKET_NAME", bucket)

    n = bronze.BATCH_SIZE
    produce_trades(topic, [make_trade(i, symbol) for i in range(1, n + 1)])

    def read_all_records():
        objects = s3.list_objects_v2(Bucket=bucket).get("Contents", [])
        records = []
        for obj in objects:
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            records.extend(fastavro.reader(io.BytesIO(body)))  # self-describing OCF
        return records

    with running(bronze.main):
        assert wait_until(lambda: len(read_all_records()) == n), \
            f"expected {n} records in MinIO, got {len(read_all_records())}"

    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=bucket)["Contents"]]
    assert all(k.startswith("bronze/dt=") and "/hour=" in k for k in keys)