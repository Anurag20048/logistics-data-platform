# Logistics Data Platform v2

An interview-ready **end-to-end Data Engineering platform** for logistics analytics.

### Core themes
**Real-time Streaming + Lakehouse + Data Quality + Orchestration + Analytics**

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

## Advanced Engineering Features

- Kafka producer with `acks=all`, retries, compression and shipment-keyed events.
- Spark event-time processing with a 10-minute watermark.
- Event-level deduplication using `event_id`.
- Partitioned curated storage by ingestion date.
- Batch and streaming paths.
- Automated schema, null, duplicate, referential-integrity and business-range checks.
- Quality report written to `data/quality/latest_quality_report.json`.
- Customer-level and shipment-level analytical marts.
- Airflow retries and dependency-aware pipeline.
- dbt model tests for uniqueness and not-null constraints.
- Reproducible synthetic dataset generation.

## Local validation

```bash
python -m pip install -r requirements.txt
python scripts/generate_data.py
python -m src.quality.validate
python -m src.quality.quality_report
python -m src.transform.build_gold
pytest -q
```

## Streaming producer

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

Services include PostgreSQL, Kafka, MinIO and Airflow.

See `docs/architecture_v2.md` and `docs/interview_story.md` for the system design and interview explanation.
