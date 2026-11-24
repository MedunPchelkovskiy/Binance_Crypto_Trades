import logging
from datetime import datetime, timedelta

from confluent_kafka.serialization import SerializationContext, MessageField
from decouple import config

from airflow.sdk import dag, task
import backfill_logic
from clients.clickhouse_clients import get_clickhouse_client
from clients.kafka_clients import get_avro_serializer, get_producer
from ingestion.validation import Trade

SYMBOLS = ["bnbusdt", "btcusdt", "ethusdt"]
TOPIC = "trade_streams_avro_dev"
# 0 = без лимит. За тест: колко id-та най-много да се гледат на symbol за един run.
MAX_IDS = config("BACKFILL_MAX_IDS", default=0, cast=int)

def cap_ranges(ranges, max_ids):
    if not max_ids:
        return ranges
    return [{**r, "to_id": min(r["to_id"], r["from_id"] + max_ids - 1)} for r in ranges]


@task
def check_data():
    return backfill_logic.check_data(get_clickhouse_client(), SYMBOLS)


# Hard stop at 45 min: the logic stops itself after 40 (MAX_RUNTIME_SEC),
# this is the safety net before the next hourly run.
@task(execution_timeout=timedelta(minutes=45))
def process_ranges(ranges, run_id=None):
    ranges = cap_ranges(ranges, MAX_IDS)
    # Gap detection, filling and watermark moves all happen inside, chunk by chunk.
    avro_serializer = get_avro_serializer()

    def serialize(trade):
        return avro_serializer(trade, SerializationContext(TOPIC, MessageField.VALUE))

    stats = backfill_logic.process_ranges(
        get_clickhouse_client(), get_producer(), serialize, TOPIC, Trade, ranges, run_id
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
    process_ranges(ranges)


binance_agg_trades_backfill()