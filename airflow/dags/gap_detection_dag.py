import logging
from datetime import datetime

from confluent_kafka.serialization import SerializationContext, MessageField

from airflow.dags import backfill_logic
from airflow.sdk import dag, task
from backfill_logic import check_data as check_data_logic
from backfill_logic import find_gaps as find_gaps_logic
from consumers.clients import get_clickhouse_client
from ingestion.producer_clients import get_avro_serializer, get_producer
from ingestion.validation import Trade

SYMBOLS = ["bnbusdt", "btcusdt", "ethusdt"]
TOPIC = "trade_streams_avro_dev"



@task
def check_data():
    return check_data_logic(get_clickhouse_client(), SYMBOLS)


@task
def find_gaps(ranges):
    # Thin wrapper: ranges come from check_data via XCom.
    return find_gaps_logic(get_clickhouse_client(), ranges)


@task
def get_binance_data(gaps):
    return backfill_logic.get_binance_data(gaps)


@task
def validate_data(trades):
    valid, invalid = backfill_logic.validate_trades(trades, Trade)
    if invalid:
        # Log only a sample: full payloads would flood the Airflow logs.
        logging.warning("Rejected %s trades, first: %s", len(invalid), invalid[0])
    return valid


@task
def write_backfill(trades):
    avro_serializer = get_avro_serializer()

    def serialize(trade):
        return avro_serializer(trade, SerializationContext(TOPIC, MessageField.VALUE))

    return backfill_logic.write_backfill(
        get_clickhouse_client(), trades, get_producer(), serialize, TOPIC
    )


@task
def verify_backfill(ranges, gaps, written, run_id=None):
    # run_id is injected by Airflow from the task context.
    return backfill_logic.verify_backfill(
        get_clickhouse_client(), ranges, gaps, written, run_id
    )


@dag(
    dag_id="binance_agg_trades_backfill",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def binance_agg_trades_backfill():
    ranges = check_data()
    gaps = find_gaps(ranges)
    download = get_binance_data(gaps)
    validated = validate_data(download)
    written = write_backfill(validated)
    verify = verify_backfill(ranges, gaps, written)

    # check >> gaps >> download >> validate >> write >> verify


binance_agg_trades_backfill()
