import logging
from datetime import datetime, timedelta

from confluent_kafka.serialization import SerializationContext, MessageField

from airflow.sdk import dag, task
import backfill_logic
from clients.clickhouse_clients import get_clickhouse_client
from clients.kafka_clients import get_avro_serializer, get_producer
from ingestion.validation import Trade

SYMBOLS = ["bnbusdt", "btcusdt", "ethusdt"]
TOPIC = "trade_streams_avro_dev"


@task
def check_data():
    return backfill_logic.check_data(get_clickhouse_client(), SYMBOLS)


@task
def find_gaps(ranges):
    # Thin wrapper: ranges come from check_data via XCom.
    return backfill_logic.find_gaps(get_clickhouse_client(), ranges)


# Hard stop at 45 min: the logic stops itself after 40 (MAX_RUNTIME_SEC),
# this is the safety net before the next hourly run.
@task(execution_timeout=timedelta(minutes=45))
def fill_gaps(gaps, run_id=None):
    # run_id is injected by Airflow from the task context.
    avro_serializer = get_avro_serializer()

    def serialize(trade):
        return avro_serializer(trade, SerializationContext(TOPIC, MessageField.VALUE))

    stats = backfill_logic.fill_gaps(
        get_clickhouse_client(), get_producer(), serialize, TOPIC, Trade, gaps, run_id
    )
    if stats["rate_limited"]:
        logging.warning(
            "Binance rate limit hit, stopping. Retry-After: %s", stats["retry_after_sec"]
        )
    logging.info("Backfill stats: %s", stats)
    return stats


@dag(
    dag_id="binance_agg_trades_backfill",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=50),
)
def binance_agg_trades_backfill():
    ranges = check_data()
    gaps = find_gaps(ranges)
    fill_gaps(gaps)


binance_agg_trades_backfill()