from datetime import datetime

from airflow.dags import backfill_logic
from airflow.sdk import dag, task
from backfill_logic import check_data as check_data_logic
from backfill_logic import find_gaps as find_gaps_logic
from consumers.clients import get_clickhouse_client

SYMBOLS = ["bnbusdt", "btcusdt", "ethusdt"]


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
def validate_data():
    pass


@task
def write_backfill():
    pass


@task
def verify_backfill():
    pass


@dag(
    dag_id="binance_agg_trades_backfill",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
)
def binance_agg_trades_backfill():
    ranges = check_data()
    gaps = find_gaps(ranges)
    download = get_binance_data()
    validate = validate_data()
    write = write_backfill()
    verify = verify_backfill()

    check >> gaps >> download >> validate >> write >> verify


binance_agg_trades_backfill()
