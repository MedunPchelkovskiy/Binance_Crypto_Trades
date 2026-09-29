"""
Factory function for creating a ClickHouse client.

Kept separate so that importing consumer modules for their transformation
logic (e.g. record_to_row) never triggers a real connection attempt.
"""

import clickhouse_connect
from decouple import config


def get_clickhouse_client():
    """Creates and returns a new ClickHouse client, configured from env vars."""
    return clickhouse_connect.get_client(
        host=config("CLICKHOUSE_HOST", default="localhost"),
        port=config("CLICKHOUSE_PORT", default=8123, cast=int),
        username=config("CLICKHOUSE_USER", default="default"),
        password=config("CLICKHOUSE_PASSWORD", default=""),
        database=config("CLICKHOUSE_DATABASE", default="default"),
    )





