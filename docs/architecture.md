# Streaming ETL — Architecture

## Overview

This project is a real-time streaming ETL pipeline for Binance cryptocurrency trades.

The pipeline receives trade events from the Binance WebSocket API, publishes them to Kafka, validates them using Avro and Schema Registry, and processes them through independent consumers into MinIO and ClickHouse.

---

## Data Flow

```text
Binance WebSocket
       |
       v
   Python Producer
       |
       v
      Kafka
       |
       +--------------------+
       |                    |
       v                    v
 Bronze MinIO          Silver ClickHouse
 Consumer              Consumer
       |                    |
       v                    v
   MinIO                ClickHouse
   (raw)              (typed/cleaned)
                            |
                            v
                       Gold Layer
                    (aggregations)
                            |
                            v
                         Grafana
```

---

## Kafka

Kafka is used as the central message broker.

The producer publishes Binance trade events to the streaming topic. Bronze and Silver consumers use separate consumer groups, allowing both pipelines to consume the same events independently.

This fan-out design prevents one consumer from affecting the offsets or availability of the other.

---

## Avro and Schema Registry

Trade events are serialized using Avro.

Schema Registry stores and manages the Avro schema used by the producer and consumers. This provides a shared contract between the components and prevents incompatible message formats.

---

## Bronze Layer

The Bronze pipeline stores the raw Kafka events in MinIO as Avro Object Container Files.

MinIO provides an immutable raw-data layer that can be used for audit, debugging and replay.

Data is partitioned by date and hour.

---

## Silver Layer

The Silver consumer writes cleaned and typed trade records to ClickHouse.

The Silver layer uses readable column names and appropriate ClickHouse types for price, quantity and timestamps.

It is the main query-ready representation of individual trades.

---

## Gold Layer

The Gold layer contains aggregated, dashboard-ready data.

Examples include:

- 1-minute OHLC price aggregates
- 5-minute trade volume
- Buyer/seller pressure
- Daily symbol summaries
- Top symbols by volume

Real-time aggregates are maintained using ClickHouse Materialized Views. Batch recomputation is handled by Airflow where late-arriving data requires it.

---

## Dead Letter Queue

Invalid messages are redirected to a dedicated Kafka DLQ topic instead of stopping the consumer pipeline.

The DLQ allows invalid events to be inspected and monitored independently from the main processing flow.

---

## Monitoring

Operational monitoring uses Prometheus and Grafana.

Important pipeline metrics include:

- Kafka consumer lag
- DLQ message count
- Consumer health
- Processing latency
- Throughput
- Data freshness

Historical pipeline-health metrics are stored in ClickHouse `monitoring_gold`, while short-lived operational alerting remains in Prometheus/Alertmanager.

---

## Main Components

| Component | Responsibility |
|---|---|
| Binance WebSocket | Source of trade events |
| Python Producer | Validation and Kafka publishing |
| Kafka | Message broker |
| Schema Registry | Avro schema management |
| MinIO | Raw/Bronze storage |
| ClickHouse | Silver and Gold analytical storage |
| Airflow | Batch orchestration |
| Prometheus | Operational metrics |
| Grafana | Monitoring and data visualization |
| Docker Compose | Local/single-VM infrastructure |
