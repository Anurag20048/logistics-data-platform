# Logistics Data Platform

Advanced interview-ready logistics data engineering platform demonstrating real-time streaming, lakehouse-style storage, orchestration, data quality, transformation, and analytics.

## Stack

- Apache Kafka
- Apache Spark Structured Streaming
- MinIO
- PostgreSQL
- Apache Airflow
- dbt
- Docker Compose
- Pytest
- GitHub Actions

## Architecture

Kafka -> Spark Structured Streaming -> MinIO Bronze/Silver -> dbt -> PostgreSQL -> Analytics

Airflow orchestrates batch and transformation workflows while automated quality checks validate schema, duplicates, nulls, referential integrity, and business ranges.

## Local validation

The project generates synthetic data with:

- 1,000 customers
- 50,000 logistics events
- Customer Logistics 360
- Shipment Performance analytics

Validated locally:

- Python compilation: PASS
- YAML validation: PASS
- Data quality gate: PASS
- Pytest: 4/4 PASS

See `docs/TEST_REPORT.md`, `docs/architecture_v2.md`, and `docs/runbook.md` for details.

## Run

```bash
python scripts/generate_data.py
python -m src.quality.validate
python -m src.quality.quality_report
python -m src.transform.build_gold
pytest -q
```

For the containerized stack, see `docker-compose.modern.yml` and `.env.example`.
