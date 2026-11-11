import logging

import clickhouse_connect
from decouple import config

logger = logging.getLogger(__name__)


def get_clickhouse_client():
    """Creates and returns a new ClickHouse client, configured from env vars."""
    clickhouse_client = clickhouse_connect.get_client(
        host=config("CLICKHOUSE_HOST", default="localhost"),
        port=config("CLICKHOUSE_PORT", default=8123, cast=int),
        username=config("CLICKHOUSE_USER", default="default"),
        password=config("CLICKHOUSE_PASSWORD", default=""),
        database=config("CLICKHOUSE_DATABASE", default="trades"),
    )
    logger.info(f"ClickHouse client created successfully")

    return clickhouse_client
