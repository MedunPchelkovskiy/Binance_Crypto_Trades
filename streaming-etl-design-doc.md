# Streaming ETL Проект — Binance Crypto Trades
## Design Doc + Task План

> Правило №1 на този документ: веднъж записано решение тук, не се променя по средата на implementation без формален "ADR update" (виж по-долу). Промени в движение = загубено време. Ако усетиш нужда да променяш нещо — първо се връщаш тук, обновяваш документа, после пипаш код.

> **Бележка за произход на проекта:** първоначалната идея беше AIS ship tracking (AISstream.io). Сменена е с Binance WebSocket crypto trades заради нестабилност на AIS API-то. Архитектурата и по-голямата част от решенията по-долу останаха непроменени спрямо оригиналния план — само data source-ът, частично storage layer-ът и dashboard-ът се смениха.

---

## 0. Времеви бюджет — реалистична преценка

**Контекст:** пълна работна заетост (8:30-17:30), първи проект от този тип (streaming), предходен batch проект (`weather-data-platform` — Prefect, Azure, Terraform, CI/CD, реално деплойнат) отне повече от очакваното поради постоянно преработване в движение (адресирано чрез ADR подхода по-горе).

**Реалистичен наличен бюджет:** ~6-10 часа/седмица (вечери + уикенд). Долният план използва **"Фаза"**, не "Седмица" — всяка фаза е дефинирана в часове полезна работа, не в календарни дни. Колко календарни седмици ще отнеме една фаза зависи от реалната ти наличност тази конкретна седмица (болест, извънредна ситуация на работа и т.н. — нормално е да варира).

**Груба обща оценка:** ~120-150 часа за целия v1 scope (Фаза 0-4, без Buffer). При 8ч/седмица среден темп → **~15-19 календарни седмици (3.5-4.5 месеца)**.

**Важно за мотивацията:** решенията са заключени предварително — часовете вървят фокусирано, не в преоткриване на архитектурата всяка седмица. Ако прогресът "усеща се" по-бавен в началото, това е нормално (concept-pass фазите тежат повече в първите седмици) — темпото се ускорява, щом навлезеш в изпълнителска рутина.

**Практически съвет:** воденето на прост дневник (`docs/progress-log.md`) с дата + какво е свършено, помага да виждаш реален напредък дори когато седмично усещането е "почти нищо не стана" — особено важно психологически при дълъг part-time проект.

---

## 1. Architecture Decision Records (ADR)

### ADR-001: Message Broker
**Решение:** Apache Kafka (KRaft mode, `apache/kafka` Docker image, без Zookeeper)
**Алтернативи разгледани:** Redpanda, AWS Kinesis
**Защо:** Първоначалният план предвиждаше Redpanda заради по-лесен local setup. На практика проектът тръгна директно с `apache/kafka` в KRaft mode — постигна се същата цел (без Zookeeper, лесен Docker setup), но с "истинския" Kafka, което е по-разпознаваемо в CV/интервюта.
**Статус:** Final — заменя предишна версия (Redpanda)

### ADR-002: Формат на съобщенията
**Решение:** Avro + Schema Registry (директно, не през JSON междинен етап)
**Защо:** Първоначалният план предвиждаше JSON за v1 → Avro за v2 ("не почвай с premature complexity"). На практика Avro беше въведено директно, защото Binance data source-ът и wire format нуждите го налагаха по-рано от очакваното. Schema Registry гарантира, че producer и consumers не се разминават в очаквания формат.
**Статус:** Final — заменя предишна версия (JSON-first)

### ADR-003: Stream Processing Engine
**Решение:** PySpark Structured Streaming (планирано, все още не имплементирано към момента на този update)
**Алтернативи разгледани:** PyFlink, ръчен Python consumer
**Защо:** PyFlink има по-добра latency, но е по-крехък за настройка и по-малко Python-native документация. Spark Structured Streaming има micro-batch модел, по-лесен за разбиране и дебъг, и е по-широко използван в индустрията → по-разпознаваем в CV.
**Забележка:** Реалният прогрес до момента (bronze/silver consumers) е реализиран с ръчни Python Kafka consumers (`confluent-kafka`), не Spark. Spark остава в scope-а за агрегациите на gold ниво (виж ADR-011) — консумерите за bronze/silver са съзнателно leaner, защото не правят windowing/stateful обработка.
**Статус:** Final

### ADR-004: Storage
**Решение:** Two-tier — MinIO (S3-compatible, Avro Object Container Files, bronze/raw layer) + ClickHouse (bronze + silver + gold layers, serving/query layer)
**Алтернативи разгледани:** PostgreSQL за serving layer
**Защо:** Първоначалният план предвиждаше PostgreSQL за serving layer. Сменено на ClickHouse — колонарна база, оптимизирана за analytical queries и Grafana dashboarding с висок insert throughput, каквото streaming trade данните изискват. PostgreSQL няма роля в текущия проект.
**Статус:** Final — заменя предишна версия (PostgreSQL)

### ADR-004a: Fan-out Consumer Pattern
**Решение:** Два независими Kafka consumers четат от един и същ topic (`trade_streams_avro_dev`), всеки със собствена consumer group:
- `bronze_minio_consumer` → пише raw Avro batches в MinIO (bronze layer, source of truth, immutable)
- `silver_consumer` → пише cleaned/typed records в ClickHouse (silver layer, query-ready)

**Защо:** Разделянето на consumer group-ите гарантира, че двата процеса не си делят partitions/offsets и не влияят на скоростта/наличността един на друг. MinIO пази пълна fidelity на суровите данни (replay capability), ClickHouse съдържа четим, типизиран slice за бързи заявки и dashboard-и.
**Статус:** Final

### ADR-004b: ClickHouse Layering (Bronze / Silver / Gold)
**Решение:** Три нива вътре в ClickHouse самата:
    - **Bronze** (`binance_agg_trades_bronze`): полета както идват от Binance (съкратени имена: `e, E, s, a, p, q, f, l, T, m, M`), минимални типови промени само където е нужно за integrity (`p`, `q` останаха `String`, за да не се губи decimal precision от source-а).
- **Silver** (`binance_agg_trades_silver`): преименувани, читаеми колони (`symbol`, `price`, `quantity`, `trade_time`, и т.н.), правилни типове (`Decimal64(8)` за цена/количество, `DateTime64(3, 'UTC')` за време), плюс `ingested_at` за monitoring на lag между produce и ClickHouse insert.
- **Gold** (виж ADR-011 по-долу): агрегирани, dashboard-ready таблици, поддържани автоматично чрез Materialized Views.

**Защо:** Разделянето следва стандартния medallion pattern — bronze е близо до source-а (debug/audit), silver е "почистена истина" (readable, typed), gold е "готово за консумация" (агрегати, без нужда от runtime `GROUP BY` върху сурови трейдове).
**Статус:** Final

### ADR-005: Orchestrator
**Решение:** Airflow, единствено за batch/around-the-stream задачи (compaction, health checks, gold layer refresh/backfill — виж Секция 3 и Фаза 3)
**Статус:** Final

### ADR-006: Dashboard / Monitoring Visualization
**Решение:** Grafana (директно, не Streamlit)
**Защо:** Първоначалният план предвиждаше Streamlit за v1 dashboard, с Grafana отложена за по-късно "advanced monitoring". Решението е сменено — авторът вече има реален Prometheus/Grafana/Alertmanager опит от батч проекта, а ClickHouse има нативна, добре документирана Grafana data source интеграция (директно през SQL queries върху silver/gold таблиците). Grafana покрива едновременно operational metrics (consumer lag, error rates) и data dashboards (trade volume, price trends) в един инструмент, вместо два паралелни (Streamlit + Grafana).
**Статус:** Final — заменя предишна версия (Streamlit)

### ADR-007: Infra
**Решение:** Docker Compose (не Terraform/IaC за v1). Docker images, push-нати към личен Docker Hub профил.
**Защо:** Terraform беше в първоначалния план (автора има опит от батч проекта), но е извадено от scope-а за този проект, за да не удвоява ефорта между "streaming концепции" (новото учене) и "IaC concepts" (вече доказано в другия проект). Docker Compose е достатъчен за local/single-VM deployment сложността на текущия scope. Terraform остава explicit кандидат за follow-up (виж Секция 6).
**Статус:** Final — заменя предишна версия (Terraform)

### ADR-008: CI/CD
**Решение:** GitHub Actions — build & push Docker images (build context per-service: `ingestion/`, `consumers/`, бъдещи Spark/Airflow images) + lint gate (black/ruff). Без Terraform plan/apply стъпки (отпаднали с ADR-007).
**Защо:** Image build/push е познат процес от батч проекта — просто се разширява към новите services. Lint gate пази код качеството без допълнителна сложност.
**Статус:** Final — заменя предишна версия (включваше terraform plan/apply)

### ADR-009: Monitoring
**Решение:** Prometheus + Grafana + Pushgateway (за краткотрайните Airflow/batch тасковете) + Alertmanager
**Защо:** Директно преизползване на съществуващ опит от батч проекта. Добавя се нова метрика, специфична за streaming: **consumer lag** (за двата независими consumer-а поотделно — bronze и silver могат да изостават различно).
**Статус:** Final

### ADR-010: Hosting за continuous producer + Kafka
**Решение:** Обмисля се Azure VM (B1s, вероятно все още в 12-месечния free tier прозорец) за continuous producer+Kafka hosting, докато лична машина е изключена; после локален sync/backup.
**Статус:** В движение — не е финализирано към момента на този update. Ще се допълни, щом решението е взето.

### ADR-011: Gold Layer Агрегации
**Решение:** Gold layer в ClickHouse, поддържан чрез Materialized Views (не ръчен batch job) за real-time-обновявани агрегати, плюс отделни Airflow-managed таблици за агрегации, изискващи периодичен batch преизчисление (backfill, late-data correction).

**Материализирани, real-time агрегати (MV, AggregatingMergeTree/SummingMergeTree):**
- `gold_price_ohlc_1m` — OHLC (open/high/low/close) + volume per symbol, 1-минутен tumbling window
- `gold_trade_volume_5m.sql` — сума на `quantity` и брой трейдове per symbol, 5-минутен prозорец
- `gold_buyer_seller_pressure_1m` — съотношение `is_buyer_maker = true/false` per symbol per минута (proxy за buy/sell pressure)

**Batch-refreshed агрегати (Airflow DAG, виж Фаза 3):**
- `gold_daily_symbol_summary` — дневен summary per symbol (avg/min/max price, total volume, trade count) — преизчислява се веднъж дневно от silver, позволява late-arriving data correction, което чист MV не поддържа елегантно
- `gold_top_symbols_by_volume` — класация на symbols по обем, обновявана на всеки час от Airflow, ползва се за overview panel в Grafana

**Защо MV + Airflow комбинация, не само едното:** ClickHouse Materialized Views са отлични за append-only, real-time-нужни агрегати, но не се преизчисляват автоматично при late-arriving/коригирани данни. Airflow DAGs покриват case-овете, при които трябва контролиран, повторяем batch recompute (напр. ако discovered bug в silver transform логиката налага backfill).
**Статус:** Final за MV частта; batch DAG детайлите виж Фаза 3

### ADR-012: Разделяне на Gold — `business_gold` vs `monitoring_gold`
**Решение:** Gold layer в ClickHouse се разделя на две ясно различни части:
- **`business_gold`** — данните за самия crypto пазар (OHLC, volume, buyer/seller pressure — виж ADR-011). Това е "какво се случва с трейдовете".
- **`monitoring_gold`** — метрики за здравето на самия pipeline, не бизнес данни. Това е "как се чувства pipeline-ът, докато обработва трейдовете".

**Защо отделна таблица/namespace, не просто "още едно поле" някъде:** monitoring данните имат различен lifecycle, различни consumers (dashboard за операции vs dashboard за пазарни данни), и логически не бива да замърсяват business агрегатите. Разделянето е чисто организационно, не техническо ограничение на ClickHouse.

**Разделение на отговорностите спрямо вече съществуващия Prometheus/Grafana monitoring (ADR-009):**
- **Остава само в Prometheus/Alertmanager** (real-time alerting use case, кратък retention достатъчен): consumer lag, DLQ/error count. Тези вече са планирани в Фаза 3.6-3.8 — не се дублират в ClickHouse, за да няма два "източника на истината" за едно и също число.
- **Отива само в `monitoring_gold`** (ClickHouse, historical trend/dashboard use case, нужен по-дълъг retention с добра query производителност назад във времето): freshness, ingestion latency (avg/p95/max), throughput, gap detection, duplicate rate, stage-by-stage latency breakdown, compaction effectiveness, DAG run history summary.

**Схема — `trades.pipeline_metrics`:**
```sql
CREATE TABLE trades.pipeline_metrics
(
    timestamp           DateTime64(3, 'UTC'),
    layer               LowCardinality(String),   -- bronze / silver / gold
    symbol              LowCardinality(String),

    records_processed   UInt64,
    records_per_second  Float64,

    avg_latency_ms      Float64,
    max_latency_ms      Float64,
    p95_latency_ms      Float64,

    latest_event_time   DateTime64(3, 'UTC'),
    freshness_seconds   Float64,

    duplicate_count      UInt64,
    duplicate_rate       Float64,

    error_count         UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (layer, symbol, timestamp);
```

**Ключови метрики и как се смятат:**
- **Freshness** — `now() - max(event_time)`; най-важната единична метрика, показва "колко назад изостава pipeline-ът" в човешки разбираем вид.
- **Ingestion latency** — `ingested_at - event_time` (вече изчислимо, `ingested_at` колоната съществува в silver — ADR-004b), пазено като avg/max/p95. Приоритет на p95 пред max при преценка на здравето — max е податлив на единични извънредни случаи.
- **Throughput** — records/second (или /minute), директен сигнал ако consumer започне да изостава.
- **Gap detection (per symbol)** — различно от общата freshness: проверява дали конкретен symbol е "замълчал" необичайно дълго спрямо обичайната си честота. Полезно за откриване на проблем със subscription на конкретен symbol, не с целия pipeline.
- **Duplicate rate** — при избраната at-least-once семантика (ADR-003 бележка) дублирани записи са очаквани при retry след failure; рязък ръст в тази метрика сигнализира проблем в commit логиката на consumer, не просто "нормален шум".
- **Stage-by-stage latency breakdown** — разбива общото latency на конкретни етапи (Kafka produce → bronze consume, Kafka produce → silver consume, silver write → gold MV update), за да се вижда КОЙ етап бави, не само че общо закъснява.
- **Compaction effectiveness** — брой файлове в MinIO преди/след всеки `compaction_dag` run (Фаза 3.2); проверява дали компакцията реално смалява броя файлове с времето.
- **DAG run history summary** — обобщение (success/failure/duration) на Airflow DAG изпълненията в `monitoring_gold`, за overview без нужда да влизаш в Airflow UI за всяка проверка.

**Кой пише тези метрики:** и двата consumer-а (`bronze_minio_consumer`, `silver_consumer`) трябва да публикуват записи в `pipeline_metrics` след всеки batch flush (records processed, latency за батча). Gap detection и duplicate rate се смятат по-добре batch-нато (Airflow DAG, периодично), не inline във всеки consumer loop — виж нова задача във Фаза 3.

**Grafana panels от `monitoring_gold`:** latency over time, throughput over time, freshness over time, gap alerts, duplicate rate over time — отделен "Pipeline Health" dashboard, различен от бизнес dashboard-а (ADR-006).

**Статус:** Final

**Ако искаш да промениш нещо от горното по средата на работа:** спри, добави нов ред "ADR-00X-v2: [промяна] — причина: [...] — cost на промяната: [...]" и едва тогава пипай код.

---

## 2. Умения — оценка и приоритетни нива

### Вече налично (преизползва се, не се учи наново)
Docker/Docker Hub image workflow, GitHub Actions basics, Prometheus, Grafana, Pushgateway, Grafana Alerting + базов Alertmanager опит, structured (JSON) logging, README/архитектурна документация, базов Terraform опит (от друг проект, не се ползва тук по ADR-007), Kafka basics (topics/partitions/consumer groups — научено в движение по време на реалната имплементация).

### Tier 1 — учи се "в движение", вградено в основния план (без него проектът не е "достатъчен")
- Kafka fan-out consumer groups (реализирано — ADR-004a)
- Avro + Schema Registry (реализирано по-рано от план — виж ADR-002)
- ClickHouse schema design: `ORDER BY`/`PARTITION BY` избор, MergeTree family engines, Materialized Views за агрегации
- Consumer lag мониторинг (Kafka exporter → Grafana panel), поотделно за bronze и silver consumer
- Windowing (tumbling) + watermarks + checkpointing в Spark Structured Streaming — **изисква кратък concept-pass преди implementation** (виж бележка по-долу); все още предстоящо
- Dead Letter Queue (DLQ) за невалидни съобщения
- Schema validation на вход (pydantic модели)
- Grafana + ClickHouse data source интеграция (SQL queries директно от dashboard panels)
- Unit тестове на трансформации + integration тестове (testcontainers)
- ADR документ (вече правим) + Runbook

### Tier 2 — добавя се ако остане време, не блокира "готов" статус
- Exactly-once vs at-least-once semantics — предимно концептуално решение + обосновка в документ, не тежка имплементация
- Data quality checks (Great Expectations или custom Airflow assertions)
- Retention policy за raw данни в MinIO (ClickHouse gold/silver остават без TTL по решение по-рано в проекта — "не искам загуба на данни")
- Indexing/projection стратегия в ClickHouse за по-тежки dashboard queries
- Load/throughput тест (по-висок обем от реалния, за да провериш границите)

### Tier 3 — explicit извън scope, споменава се в README като "future work"
- Terraform/IaC за deployment (виж ADR-007 — explicit follow-up проект, не текущ scope)
- Secrets management (Vault/AWS Secrets Manager/SSM) — за v1 е приемливо `.env` **извън git** (в `.gitignore`), с ясна бележка в README
- Least-privilege IAM roles / VPC network isolation
- Distributed tracing (OpenTelemetry)
- Chaos-тип тестове (kill container mid-run)
- Формална schema evolution стратегия
- Kubernetes deployment

> **Бележка за Windowing/watermarks/checkpointing:** това остава единствената група от Tier 1, за която препоръчвам 2-3 часа четене на официалната Spark Structured Streaming документация ("Programming Model" секцията) ПРЕДИ да пишеш job-а. Грешен избор тук струва преработка на архитектура, не на няколко реда код.

---

## 3. MVP Scope — какво влиза в v1

### ВЛИЗА:
- [x] Binance WebSocket listener (Python, `ingestion/binance_client.py` + `ingestion/producer.py`) → пише в Kafka topic
- [x] Pydantic/Avro валидация на входните съобщения (Schema Registry)
- [ ] DLQ topic за невалидни съобщения (планирано, все още не имплементирано)
- [x] `bronze_minio_consumer`: Kafka → Avro Object Container Files → MinIO (партиционирано по дата/час)
- [x] `silver_consumer`: Kafka → типизирани, читаеми записи → ClickHouse silver таблица
- [ ] Gold layer: Materialized Views за real-time агрегации (OHLC, volume, buyer/seller pressure — виж ADR-011)
- [ ] Spark Structured Streaming job (ако/когато windowing логика надрасне това, което MV в ClickHouse може елегантно да покрие — виж бележка в ADR-003)
- [ ] Compaction на малки Avro файлове в MinIO (Airflow DAG)
- [ ] Airflow DAGs: compaction, health check, gold batch refresh (виж Фаза 3 за пълен списък)
- [ ] Grafana dashboard: consumer lag panels + data panels (price/volume trends от ClickHouse silver/gold)
- [ ] GitHub Actions: build/push images (per-service), lint gate
- [ ] Prometheus + Grafana: базови метрики + **consumer lag** panels (bronze и silver поотделно) + Alertmanager rule
- [ ] README с архитектурна диаграма
- [ ] ADR документ (този файл) + кратък Runbook ("какво правя ако X се счупи")
- [ ] 5-10 unit теста на трансформационната логика + integration тест (testcontainers)

### НЕ ВЛИЗА (explicit "graveyard" за по-късно — НЕ мисли за тях сега):
- Terraform/IaC deployment (виж ADR-007)
- Kubernetes deploy
- PostgreSQL (напуснало scope-а — ADR-004)
- Streamlit dashboard (напуснало scope-а — ADR-006)
- Secrets management с Vault/Secrets Manager (за v1: `.env` извън git, документирано ограничение)
- IAM least-privilege / VPC isolation
- Distributed tracing
- Fraud/anomaly detection логика
- Chaos тестове
- Формална schema evolution стратегия
- TTL/retention policy върху ClickHouse таблиците (explicit решение — "без загуба на данни" в текущия scope)

**Правило:** Ако по време на работа ти хрумне добра идея извън списъка — записваш я в `docs/backlog.md`, НЕ я имплементираш веднага.

---

## 3.1 Definition of Done (важи за всяка задача по-долу)

Задача се счита за завършена, само ако:
1. Кодът работи end-to-end локално (не "би трябвало да работи")
2. Има поне минимален тест (за код) или ръчна проверка с checklist (за infra)
3. Закоментиран е накратко WHY, не само WHAT
4. Commit-нат е в git с ясно съобщение
5. Не оставя "TODO: fix later" за нещо критично за следващата задача

---

## 3.5 Top-level Skeleton структура

> Тази структура е "архитектурно" решение — фиксира се сега, не се променя в движение (виж Секция 5). Вътрешната организация на файлове **в рамките на** дадена папка е свободна и може да се рефакторира докато пишеш.

```
streaming-etl-portfolio/
├── ingestion/                     # Binance WebSocket producer
│   ├── binance_client.py           # WebSocket client логика, отделена от Kafka
│   ├── producer.py                 # Kafka producer логика
│   ├── models.py                   # Pydantic валидация
│   └── tests/
│
├── consumers/                     # Fan-out consumers (ADR-004a)
│   ├── bronze_minio_consumer.py    # Kafka → Avro OCF → MinIO
│   ├── silver_consumer.py          # Kafka → ClickHouse silver
│   └── tests/
│
├── schemas/                       # Споделени Avro схеми (Schema Registry source of truth)
│   └── trade_schema.py
│
├── clickhouse/                    # DDL за bronze/silver/gold таблици + Materialized Views
│   ├── bronze/
│   ├── silver/
│   └── gold/
│       ├── business_gold/          # OHLC, volume, buyer/seller pressure (ADR-011)
│       └── monitoring_gold/        # pipeline_metrics, health metrics (ADR-012)
│
├── streaming-jobs/                # PySpark Structured Streaming (предстоящо, ADR-003)
│   ├── jobs/
│   ├── transformations/
│   └── tests/
│
├── orchestration/                 # Airflow DAGs (Фаза 3)
│   └── dags/
│       ├── compaction_dag.py
│       ├── health_check_dag.py
│       └── gold_batch_refresh_dag.py
│
├── monitoring/                    # Prometheus/Grafana/Alertmanager конфигурации
│   ├── grafana-dashboards/
│   └── alert-rules/
│
├── docs/
│   ├── architecture.md             # диаграма + обяснение
│   ├── design-decisions.md         # ADR-и (Секция 1 от този документ)
│   ├── schema.md
│   ├── backlog.md
│   ├── progress-log.md
│   └── runbook.md
│
├── .github/workflows/              # CI/CD pipeline дефиниции (build/push, lint)
│
├── docker-compose.yml               # цялата local/single-VM инфраструктура (ADR-007)
│
└── README.md
```

---

> Всяка фаза = блок работа с оценка в часове, не фиксирана календарна седмица. При ~8ч/седмица наличност, очаквай ~1.5-3 календарни седмици на фаза в зависимост от обема ѝ.

### Фаза 0 (~5-6 часа) — Concept pass / Reading

**Задължително преди Spark работа (все още предстоящо):**
| # | Задача | Ресурс | DoD критерий |
|---|---|---|---|
| 0.1 | Spark Structured Streaming — Programming Model (~2-3ч) | [spark.apache.org — Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html), секции: Basic Concepts, Window Operations, Handling Late Data and Watermarking | Можеш с 2-3 изречения да обясниш разликата tumbling vs sliding window и защо watermark е нужен |
| 0.2 | ClickHouse MergeTree fundamentals (~1.5ч) | [ClickHouse docs — MergeTree engine](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree) | Можеш да обясниш защо `ORDER BY`/`PARTITION BY` са structural решения, не query-time hints |
| 0.3 | Kafka основни концепции (вече покрито в движение) | — | Можеш да обясниш topic/partition/consumer group/offset/consumer lag с прости думи |
| 0.4 | Exactly-once vs at-least-once — кратък overview (за Tier 2 решение по-късно) | Кратка статия по избор | Записан е избор в ADR (дори "отлагаме до Tier 2, стартираме с at-least-once") |

**Чете се "в движение", точно преди съответната задача:**
| Тема | Кога | Ресурс |
|---|---|---|
| ClickHouse Materialized Views basics (~30 мин) | Преди gold layer MV задачите | [ClickHouse docs — Materialized View](https://clickhouse.com/docs/en/sql-reference/statements/create/view#materialized-view) |
| Dead Letter Queue pattern (~20 мин) | Преди DLQ задача | Кратка статия — търси "DLQ pattern Kafka" |
| Grafana + ClickHouse data source setup (~20 мин) | Преди dashboard задачите | [Grafana ClickHouse plugin docs](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/) |

**Не чети предварително:** Great Expectations, OpenTelemetry, Vault, Terraform modules — извън текущия scope.

### Фаза 1 (~20-25 часа) — Ingestion & Fan-out Consumers *(до голяма степен завършена)*
| # | Задача | Статус  | DoD критерий |
|---|---|---------|---|
| 1.1 | Docker Compose: Kafka (KRaft), MinIO, ClickHouse, Schema Registry контейнери | Done    | `docker-compose up` вдига всички, достъпни на портовете си |
| 1.2 | Binance WebSocket producer (`binance_client.py` + `producer.py`) | Done    | Съобщения видими в Kafka topic-a за >2 минути без прекъсване |
| 1.3 | Avro schema дефиниция + Schema Registry интеграция | Done    | Producer и consumers се съгласяват на схемата без грешки |
| 1.4 | `bronze_minio_consumer`: Kafka → MinIO (Avro OCF, партиционирано dt/hour) | Done    | Файлове се появяват в MinIO с правилно partitioning |
| 1.5 | `silver_consumer`: Kafka → ClickHouse silver таблица | Done    | Записи се появяват в ClickHouse с коректни типове/имена |
| 1.6 | DLQ topic за невалидни съобщения | Done | Невалидно съобщение отива в DLQ, не гърми consumer-а |
| 1.7 | GitHub Actions: build & push Docker images (per-service) + lint gate | Done    | Push към main тригва build, images се появяват в Docker Hub |
| 1.8 | Kafka backup стратегия (`kafka_backup.py`, периодичен tar.gz backup на volume) | Done    | Backup скрипт работи, `restart: unless-stopped` в compose |

### Фаза 2 (~25-30 часа) — Gold Layer & Stream Processing
| # | Задача                                                 | DoD критерий |
|---|--------------------------------------------------------|------|
| 2.1 | ClickHouse gold: `gold_price_ohlc_1m` Materialized View| Done | При insert в silver, OHLC MV се обновява автоматично, стойностите съвпадат с ръчна проверка |
| 2.2 | ClickHouse gold: `gold_trade_volume_5m.sql` Materialized View| Done | Volume/trade count per symbol/5-мин прозорец коректни спрямо raw данни                       |
| 2.3 | ClickHouse gold: `gold_buyer_seller_pressure_1m` Materialized View | Done | Съотношението `is_buyer_maker` коректно агрегирано на минута |
| 2.3a | ClickHouse `monitoring_gold`: `pipeline_metrics` таблица (ADR-012) | Done | Таблицата се създава, приема тестов ръчен insert без грешка                                 |
| 2.3b | И двата consumer-а публикуват записи в `pipeline_metrics` след всеки batch flush (records processed, avg/max/p95 latency за батча) | Done | След реален run се виждат нови редове в `pipeline_metrics` за bronze и silver слоя поотделно |
| 2.4 | (Ако е нужно) PySpark environment setup за по-сложна windowing логика, извън това, което MV елегантно покрива | `spark-submit` тестов job се свързва към Kafka успешно |
| 2.5 | Unit тестове за трансформационната/агрегационна логика (изолирано, testable функции) | Done | 5+ теста минават, покриват нормален + edge case (напр. late-arriving trade) |
| 2.6 | GitHub Actions: automated unit тестове на всеки PR     | PR с чупещ тест не се merge-ва (branch protection) |

### Фаза 3 (~30-35 часа) — Orchestration, Batch Gold & Monitoring
| # | Задача | DoD критерий |
|---|---|---|
| 3.1 | Airflow deploy (Docker Compose service) | UI достъпен, вижда се празен DAG list |
| 3.2 | DAG: `compaction_dag` — hourly compaction на малки Avro файлове в MinIO | Ръчен trigger работи, файловете се сливат коректно, брой файлове намалява |
| 3.3 | DAG: `health_check_dag` — периодична проверка на producer + двата consumer процеса (heartbeat/last-message-timestamp check) + Pushgateway метрика | Симулирано прекъсване на потока → метриката в Prometheus го отразява до следващия DAG run |
| 3.4 | DAG: `gold_batch_refresh_dag` — дневен recompute на `gold_daily_symbol_summary` от silver (поддържа late-arriving data correction, за разлика от MV) СМИСЛЕНО СЛЕД ДОБАВЯНЕ НА РЕКОВЪРИ МЕХАНИЗЪМ(GET /api/v3/aggTrades) | Ръчен backfill trigger пресмята коректно за минал ден, презаписва без дубликати |
| 3.5 | DAG: `gold_top_symbols_dag` — hourly refresh на `gold_top_symbols_by_volume` класация | Таблицата отразява актуална класация след всеки run |
| 3.6 | DAG: DLQ monitoring task — брои съобщения в DLQ topic, alert при рязък ръст | Изкуствено пуснато невалидно съобщение вдига брояча, видимо в Grafana |
| 3.7 | Prometheus: consumer lag exporter (Kafka), поотделно за bronze и silver consumer group → Grafana panels | И двата панела показват реален lag, растат при изкуствено забавяне на съответния consumer |
| 3.8 | Grafana alert + Alertmanager rule при lag над праг (за всеки consumer group поотделно) | Изкуствен lag spike на bronze ИЛИ silver тригва съответен alert |
| 3.9 | DAG: `gap_detection_dag` (ADR-012) — периодична проверка per symbol за необичайно дълга "тишина" спрямо обичайната честота, запис в `pipeline_metrics`"recovery е отделен модул, стартиран по график от Airflow" | Изкуствено спряно subscription за 1 symbol се засича до следващия DAG run |
| 3.10 | DAG: `duplicate_rate_dag` (ADR-012) — периодично изчисление на % дублирани записи в silver, запис в `pipeline_metrics` | Изкуствено вкаран дубликат вдига метриката видимо |
| 3.11 | Compaction effectiveness метрика — `compaction_dag` (3.2) логва брой файлове преди/след всеки run в `pipeline_metrics` | Графика показва намаляващ брой файлове след всеки run |
| 3.12 | DAG run history summary — обобщение (success/failure/duration) на всички DAG-ове, запис в `pipeline_metrics` или отделна monitoring_gold таблица | Overview panel показва последните N run-а без нужда от Airflow UI |

### Фаза 4 (~20-25 часа) — Dashboard, Integration Testing, Документация
| # | Задача | DoD критерий |
|---|---|---|
| 4.1 | Grafana dashboard: OHLC/price trend panel (от `gold_price_ohlc_1m`) | Done | Панелът се обновява с нови данни в реално време |
| 4.2 | Grafana dashboard: trade volume panel (от `gold_trade_volume_5m.sql`) | Done | Графиката отразява реални исторически данни |
| 4.3 | Grafana dashboard: top symbols by volume panel (от `gold_top_symbols_by_volume`) | Done |  Класацията се обновява след Airflow DAG run |
| 4.4 | Grafana: обединен operational dashboard (consumer lag, DLQ count, health check status) | Done |  Всички operational метрики видими на един dashboard |
| 4.4a | Grafana: отделен "Pipeline Health" dashboard от `monitoring_gold` (ADR-012) — latency/freshness/throughput/gap/duplicate rate over time | Всички `monitoring_gold` панели показват реални, обновяващи се данни |
| 4.5 | Integration тест (end-to-end, producer → Kafka → двата consumer-а → MinIO/ClickHouse) чрез testcontainers | Тестът минава локално и в CI |
| 4.6 | README с архитектурна диаграма + explicit "in scope / out of scope" секция | Друг човек може да пусне проекта само по README |
| 4.7 | Runbook: сценарии ("producer спря", "consumer lag расте", "DLQ се пълни", "gold DAG failed") + стъпки за диагностика | Всеки сценарий има конкретни команди/панели за проверка |
| 4.8 | Демо видео/GIF за портфолиото | 60-90 сек, показва живи данни на Grafana dashboard-а |

### Buffer / Tier 2 (само ако Фаза 1-4 приключат, а мотивация/време остават)
| # | Задача |
|---|---|
| B.1 | Exactly-once semantics: имплементация + документирана обосновка |
| B.2 | Data quality checks (Great Expectations) в Airflow DAG |
| B.3 | Retention policy job за raw данни в MinIO (не за ClickHouse — explicit решение за без загуба на данни там) |
| B.4 | Load/throughput тест (симулиран по-висок обем) |
| B.5 | ClickHouse projections/indexing за по-тежки dashboard queries |

---

## 4. Как да не повториш грешката от батч проекта

1. **Преди всяка работна сесия:** прочети отново ADR секцията. Ако имаш идея за промяна — тя чака до края на v1.
2. **Времеви бюджет за "проучване":** максимум 20% от времето за фаза (изключение: Фаза 0, която е изцяло concept-pass). Ако прекараш повече от това в четене на документация без да пишеш код — знак си, че задачата е твърде голяма и трябва да се разбие.
3. **"Cut line" на фаза:** ако задача не е завършена до планирания часови бюджет на фазата, тя НЕ спира следващата фаза — маркираш я "carried over" в progress log-а, но не позволяваш scope на следваща фаза да се смеси с текущата.
4. **Backlog файл, не памет:** всяка идея извън текущия scope → `docs/backlog.md`, веднага, за да спре да "тежи" в главата ти по време на работа.
5. **При пълна работна заетост — приемай варирането в темпото.** Седмица с 3 часа вместо 8 не е провал, просто фазата отнема малко по-дълго календарно. Progress log-ът е за да виждаш реалния кумулативен напредък, не седмичното темпо.

---

## 5. Следващи стъпки (след v1)

Само след завършен и работещ v1, в ред по избор:
- Terraform/IaC deployment (explicit follow-up проект — виж ADR-007)
- Exactly-once semantics (ако не е стигнато до Buffer B.1)
- Формална schema evolution стратегия
- Secrets management (Vault/AWS Secrets Manager)
- IAM least-privilege / VPC network isolation
- Distributed tracing (OpenTelemetry)
- Chaos тестове
- Втори window тип (sliding/session) за gold агрегациите
- K8s deployment (като отделен "deployment" проект)
- Freelance-ready packaging: ако проектът се ползва като база за freelance предлагане, обмисли multi-tenant конфигурация (различни symbol lists per client) като отделна follow-up тема
