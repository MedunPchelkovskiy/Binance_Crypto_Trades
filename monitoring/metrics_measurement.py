import time
import numpy as np

def measure_batch(process_func, records, extract_event_time_ms):
    """
    extract_event_time_ms: функция record -> event timestamp в ms
    (различна за bronze vs silver, виж по-долу защо)
    """
    start = time.perf_counter()
    result = process_func(records)
    batch_duration_ms = int((time.perf_counter() - start) * 1000)

    write_time_ms = int(time.time() * 1000)
    latencies = [write_time_ms - extract_event_time_ms(r) for r in records]

    metrics = {
        "batch_duration_ms": batch_duration_ms,
        "records_processed": len(records),
        "avg_processing_latency_ms": float(np.mean(latencies)),
        "max_processing_latency_ms": int(np.max(latencies)),
        "p95_processing_latency_ms": float(np.percentile(latencies, 95)),
    }
    return result, metrics