# Local Runbook

## 1. Generate and validate batch data

```bash
python scripts/generate_data.py
python -m src.quality.validate
python -m src.quality.quality_report
python -m src.transform.build_gold
pytest -q
```

## 2. Start infrastructure

```bash
docker compose -f docker-compose.modern.yml up -d
```

## 3. Publish streaming events

```bash
python streaming/kafka_producer.py
```

## 4. Production-style flow

```text
Kafka -> Spark -> MinIO -> PostgreSQL -> dbt -> BI
                 ^
                 |
              Airflow
```

## Operational controls

- Kafka producer uses acknowledgements and retries.
- Spark uses event-time watermarking and event-id deduplication.
- Quality gate blocks downstream work on invalid source data.
- Airflow retries failed tasks.
- dbt tests enforce key integrity in analytical marts.
