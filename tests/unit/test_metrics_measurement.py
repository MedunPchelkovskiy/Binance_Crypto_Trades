# tests/unit/test_metrics_measurement.py
from unittest.mock import MagicMock

import pytest

from monitoring.metrics_measurement import measure_batch


def dummy_process(client, batch):
    """Fake process_func: returns the batch unchanged."""
    return batch


def make_batch(event_times_ms, receipt_times_ms):
    """Helper: batch = list of (record, receipt_time_ms) tuples."""
    return list(zip(event_times_ms, receipt_times_ms))


def extract_event_time(record):
    return record  # in these tests, record is directly the event_time_ms


# --- Normal case ---
def test_measure_batch_normal_case():
    batch = make_batch(
        event_times_ms=[1000, 1000, 1000],
        receipt_times_ms=[1100, 1150, 1200],  # latency: 100, 150, 200
    )
    _, metrics = measure_batch(MagicMock(), "batch-1", dummy_process, batch, extract_event_time)

    assert metrics["records_processed"] == 3
    assert metrics["avg_latency_ms"] == 150
    assert metrics["max_latency_ms"] == 200


# --- Edge case: late-arriving trade (large latency outlier) ---
def test_measure_batch_late_arriving_trade():
    batch = make_batch(
        event_times_ms=[1000, 1000, 1000],
        receipt_times_ms=[1010, 1020, 60000],  # last one is very delayed
    )
    _, metrics = measure_batch(MagicMock(), "batch-2", dummy_process, batch, extract_event_time)

    assert metrics["max_latency_ms"] == 59000
    assert metrics["p95_latency_ms"] <= metrics["max_latency_ms"]


# --- Edge case: batch with a single record ---
def test_measure_batch_single_record():
    batch = make_batch([1000], [1050])
    _, metrics = measure_batch(MagicMock(), "batch-3", dummy_process, batch, extract_event_time)

    assert metrics["records_processed"] == 1
    assert metrics["avg_latency_ms"] == 50
    assert metrics["p95_latency_ms"] == 50


# --- Edge case: zero latency (receipt time equals event time) ---
def test_measure_batch_zero_latency():
    batch = make_batch([1000, 2000], [1000, 2000])
    _, metrics = measure_batch(MagicMock(), "batch-4", dummy_process, batch, extract_event_time)

    assert metrics["avg_latency_ms"] == 0
    assert metrics["max_latency_ms"] == 0


# --- Edge case: empty batch divides by zero (documents current behavior) ---
def test_measure_batch_empty_batch_raises():
    with pytest.raises(ZeroDivisionError):
        measure_batch(MagicMock(), "batch-5", dummy_process, [], extract_event_time)