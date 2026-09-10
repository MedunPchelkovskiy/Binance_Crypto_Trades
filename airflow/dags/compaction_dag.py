from datetime import datetime, timedelta

import boto3
from decouple import config

from airflow.sdk import dag, task

@dag(
    dag_id="compaction_dag",
    schedule="@hourly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["maintenance", "minio"],
)
def compaction_dag():

    @task
    def list_small_files() -> list[str]:
        # свързваш се с MinIO (boto3/minio client), листваш bronze bucket, TODO: this is second place of using this client, eventually move in connections folder/file for DRY.
        s3_client = boto3.client(
            "s3",
            endpoint_url=config("MINIO_ENDPOINT"),
            aws_access_key_id=config("MINIO_ACCESS_KEY"),
            aws_secret_access_key=config("MINIO_SECRET_KEY"),
        )
        # филтрираш файлове под някакъв size threshold (напр. <5MB)


    @task
    def compact_files(file_keys: list[str]) -> dict:
        # чете малките Avro файлове, слива ги в един по-голям
        # записва новия обединен файл, изтрива старите
        # връща {"before": N, "after": M} за метриката от 3.11
        ...

    @task
    def log_compaction_metrics(stats: dict):
        # запис в pipeline_metrics таблица (ClickHouse) — за DoD 3.11
        ...

    files = list_small_files()
    stats = compact_files(files)
    log_compaction_metrics(stats)

compaction_dag()