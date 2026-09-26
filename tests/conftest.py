# tests/conftest.py
"""
Pytest fixtures and test-time patches shared across all test modules.

Patches external clients (Kafka, ClickHouse) at import time, so that
importing production modules during test collection does not attempt
real network connections that only work inside the Docker network.
"""

from unittest.mock import MagicMock
import clickhouse_connect

clickhouse_connect.get_client = MagicMock()