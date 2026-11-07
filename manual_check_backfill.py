from airflow.dags.backfill_logic import check_data, find_gaps
from clients.clickhouse_clients import get_clickhouse_client

client = get_clickhouse_client()
ranges = check_data(client, ["bnbusdt", "btcusdt", "ethusdt"])
print("RANGES:", ranges)
print("GAPS:", find_gaps(client, ranges))