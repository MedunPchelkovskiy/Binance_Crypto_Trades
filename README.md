# Streaming ETL Portfolio — Binance Crypto Trades

A production-style streaming ETL pipeline that ingests real-time Binance crypto trades, processes them through Kafka consumer groups, stores raw and analytical data in MinIO and ClickHouse, and exposes operational and business metrics through Grafana and Prometheus.

The project is a portfolio implementation focused on practical streaming data engineering concepts: **Kafka, Avro, Schema Registry, ClickHouse, Airflow, Prometheus/Grafana, Docker and CI/CD**.

---

## Architecture

```text
                         Binance WebSocket
                                │
                                ▼
                    ┌─────────────────────┐
                    │  Python Producer    │
                    │  Avro + Validation  │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │        Kafka        │
                    │  trade_streams_avro │
                    └──────────┬──────────┘
                               │
                  ┌────────────┴────────────┐
                  │                         │
                  ▼                         ▼
       ┌─────────────────────┐   ┌─────────────────────┐
       │ Bronze MinIO        │   │ Silver Consumer     │
       │ Consumer            │   │                     │
       └──────────┬──────────┘   └──────────┬──────────┘
                  │                         │
                  ▼                         ▼
             ┌────────┐             ┌──────────────┐
             │ MinIO  │             │  ClickHouse  │
             │ Bronze │             │    Silver    │
             └────────┘             └──────┬───────┘
                                           │
                                           ▼
                                  ┌─────────────────┐
                                  │ ClickHouse Gold │
                                  │ OHLC / Volume / │
                                  │ Buy/Sell        │
                                  │ Pressure        │
                                  └────────┬────────┘
                                           │
                                           ▼
                                      ┌─────────┐
                                      │ Grafana │
                                      └─────────┘

 Kafka ───────────────► Prometheus ───────► Grafana
   │
   └── consumer lag / operational metrics

 Airflow ─────────────► batch jobs / compaction /
                        health checks / gold refresh
```

## Main Components

| Component | Responsibility |
|---|---|
| Binance WebSocket | Real-time crypto trade source |
| Python Producer | Validates and publishes trades to Kafka |
| Apache Kafka | Streaming message broker |
| Avro + Schema Registry | Message serialization and schema contract |
| Bronze MinIO Consumer | Persists immutable raw Avro data |
| Silver ClickHouse Consumer | Writes typed, query-ready trade records |
| MinIO | Raw/bronze object storage |
| ClickHouse | Analytical bronze/silver/gold storage |
| Airflow | Batch orchestration and operational jobs |
| Prometheus | Operational metrics |
| Grafana | Operational and business dashboards |
| Kafka Exporter | Kafka consumer lag metrics |
| Docker Compose | Local/single-VM infrastructure |
| GitHub Actions | Image build/push and CI checks |

---

## Data Layers

### Bronze

Raw Binance trade data is preserved as close to the source format as possible.

- MinIO stores Avro Object Container Files.
- ClickHouse contains a bronze representation of the source fields.
- Data is retained for replay, debugging and audit purposes.

### Silver

The silver layer contains cleaned and typed trade records.

Examples of normalized fields:

- `symbol`
- `price`
- `quantity`
- `trade_time`
- `is_buyer_maker`
- `ingested_at`

ClickHouse is used for analytical querying and high-throughput inserts.

### Gold

Gold contains dashboard-ready aggregations.

Current planned/implemented aggregations:

- 1-minute OHLC
- 5-minute trade volume
- 1-minute buyer/seller pressure
- daily symbol summaries
- top symbols by volume

Gold is split conceptually into:

- **`business_gold`** — market/trade analytics
- **`monitoring_gold`** — pipeline health and historical operational metrics

---

## Monitoring

The pipeline uses Prometheus and Grafana for operational monitoring.

Important metrics include:

- Kafka consumer lag
- DLQ/error count
- pipeline freshness
- ingestion latency
- throughput
- duplicate rate
- gap detection
- compaction effectiveness
- Airflow DAG execution history

Consumer lag is monitored separately for the bronze and silver consumer groups.

---

## Orchestration

Airflow is intentionally used for batch and operational workloads rather than replacing the streaming consumers.

Planned DAG responsibilities:

- MinIO file compaction
- pipeline health checks
- gold batch refresh
- DLQ monitoring
- gap detection
- duplicate-rate calculation
- compaction effectiveness
- DAG run history

---

## Running Locally

### Prerequisites

- Docker
- Docker Compose
- Git

### Setup

Create a local environment file from the example:

```bash
cp .env.example .env
```

Fill in the required local configuration values.

> **Note:** the `.env` file must remain outside Git.

Start the infrastructure:

```bash
docker compose up -d
```

Check the running services:

```bash
docker compose ps
```

Useful local interfaces:

- Kafka UI
- MinIO Console
- ClickHouse HTTP interface
- Airflow UI
- Grafana
- Prometheus

Exact ports and configuration are defined in `docker-compose.yml`.

---

## Project Structure

```text
.
├── airflow/
│   └── dags/
├── consumers/
├── dockerfiles/
├── grafana/
│   └── dashboards/
├── ingestion/
├── monitoring/
├── processing/
├── schemas/
├── sql/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   └── monitoring/
├── tests/
├── requirements_files/
├── docker-compose.yml
├── streaming-etl-design-doc.md
└── README.md
```

---

## Testing

The project contains unit tests covering validation, message handling, consumer batching and monitoring logic.

Run the test suite with:

```bash
pytest
```

---

## CI/CD

GitHub Actions is used for automated checks and Docker image publishing.

The CI pipeline is intended to:

- run lint checks
- run automated tests
- build service-specific Docker images
- publish images to Docker Hub

Terraform/IaC deployment is intentionally outside the current v1 scope.

---

## Scope

### In scope for v1

- Binance WebSocket ingestion
- Kafka/KRaft
- Avro + Schema Registry
- Kafka fan-out consumer groups
- MinIO bronze storage
- ClickHouse bronze/silver/gold layers
- Gold Materialized Views
- Airflow orchestration
- Prometheus/Grafana monitoring
- Consumer lag monitoring
- DLQ
- Automated tests
- Docker Compose deployment
- GitHub Actions CI/CD
- Architecture and operational documentation

### Out of scope

The following are intentionally deferred:

- Terraform/IaC
- Kubernetes
- PostgreSQL
- Streamlit
- Vault / external secrets management
- IAM/VPC hardening
- Distributed tracing
- Chaos testing
- Formal schema evolution strategy
- Fraud/anomaly detection
- ClickHouse TTL/retention policies

Future ideas are tracked separately rather than implemented during v1.

---

## Design Decisions

The main architectural decisions are documented in [`streaming-etl-design-doc.md`](streaming-etl-design-doc.md).

The document contains the ADRs, project scope, Definition of Done and implementation plan.

Key decisions:

- Kafka in KRaft mode
- Avro + Schema Registry
- MinIO + ClickHouse storage architecture
- Kafka fan-out consumer groups
- ClickHouse Medallion architecture
- Airflow for batch/orchestration workloads
- Grafana for both operational and business visualization
- Docker Compose instead of Terraform for v1

---

## Operational Documentation

A runbook will document common failure scenarios such as:

- producer stopped
- consumer lag increasing
- DLQ growth
- ClickHouse ingestion problems
- MinIO storage problems
- failed Airflow DAG
- stale pipeline data

---

## Project Status

The ingestion and fan-out consumer foundation is implemented.

Current project progress is tracked in the design document and will be complemented by a progress log as development continues.

The project follows a phase-based implementation plan rather than a fixed calendar schedule.

---

## License

This project is intended as a personal data engineering portfolio project.
