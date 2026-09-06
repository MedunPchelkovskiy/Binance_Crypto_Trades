import time


def measure_batch(process_func, batch, extract_event_time_ms):
    start = time.perf_counter()
    result = process_func(batch)
    batch_duration_ms = int((time.perf_counter() - start) * 1000)

    latencies = [
        receipt_time_ms - extract_event_time_ms(record)
        for record, receipt_time_ms in batch
    ]

    metrics = {
        "batch_duration_ms": batch_duration_ms,
        "records_processed": len(batch),
        "avg_latency_ms": sum(latencies) / len(latencies),
        "max_latency_ms": max(latencies),
        "p95_latency": sorted(latencies)[int(0.95 * (len(latencies) - 1))],
    }
    return result, metrics
