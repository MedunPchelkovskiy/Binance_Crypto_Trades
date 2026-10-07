from datetime import datetime

from airflow.sdk import dag, task


@task
def check_data():
    pass


@task
def find_gaps():
    pass


@task
def get_binance_data():
    pass


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

    check = check_data()
    gaps = find_gaps()
    download = get_binance_data()
    validate = validate_data()
    write = write_backfill()
    verify = verify_backfill()

    check >> gaps >> download >> validate >> write >> verify


binance_agg_trades_backfill()