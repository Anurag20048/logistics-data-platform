# Logistics Data Platform v2

An end-to-end **Data Engineering platform** for logistics analytics, built around real-time streaming, lakehouse-style storage, data quality, orchestration, and analytical marts.

## Architecture

```text
Kafka -> Spark Structured Streaming -> MinIO Bronze/Silver
                                  |
                                  +-> PostgreSQL -> dbt -> Gold Marts
                                  |
Airflow -> Quality Gate -> Gold Build -> Warehouse Load -> dbt Tests
```

## Technology Stack

- **Streaming:** Apache Kafka
- **Processing:** Apache Spark Structured Streaming
- **Lakehouse storage:** MinIO / S3-compatible object storage
- **Warehouse:** PostgreSQL
- **Transformation:** dbt
- **Orchestration:** Apache Airflow
- **Testing:** Pytest + dbt tests
- **Infrastructure:** Docker Compose
- **Analytics:** Customer Logistics 360 + Shipment Performance

## Engineering Features

- Kafka producer configured with `acks=all`, retries, compression, and shipment-keyed events.
- Spark event-time processing with a 10-minute watermark.
- Event-level deduplication using `event_id`.
- Partitioned curated storage by ingestion date.
- Batch and streaming processing paths.
- Automated schema, null, duplicate, referential-integrity, and business-range checks.
- Customer-level and shipment-level analytical marts.
- Airflow retries and dependency-aware pipeline execution.
- dbt tests for uniqueness and not-null constraints.
- Reproducible synthetic dataset generation.

## Validation

The project was validated locally before publication:

- Python syntax compilation: **PASS**
- YAML parsing: **PASS**
- Synthetic data generation: **PASS**
- Data-quality gate: **PASS**
- Gold transformations: **PASS**
- Pytest: **4/4 PASS**
- Generated test dataset: **1,000 customers and 50,000 logistics events**
- Docker runtime: not executed in the validation environment because Docker CLI was unavailable.

Generated raw datasets are intentionally excluded from the repository; use the data-generation script to recreate them locally.

## Run Locally

```bash
python -m pip install -r requirements.txt
python scripts/generate_data.py
python -m src.quality.validate
python -m src.quality.quality_report
python -m src.transform.build_gold
pytest -q
```

## Streaming Producer

```bash
python streaming/kafka_producer.py
```

Set these variables when required:

```text
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=logistics-events
EVENT_COUNT=100
```

## Docker

```bash
docker compose -f docker-compose.modern.yml up -d
```

The modern Compose stack includes PostgreSQL, Kafka, MinIO, and Airflow.

See `docs/architecture_v2.md`, `docs/interview_story.md`, and `docs/runbook.md` for architecture, interview preparation, and operational guidance.
