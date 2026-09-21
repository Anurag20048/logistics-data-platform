# Logistics Data Platform v2 Architecture

```text
                         +-------------------+
                         | Logistics Events  |
                         +---------+---------+
                                   |
                                   v
                         +-------------------+
                         | Kafka             |
                         | keyed by shipment |
                         +---------+---------+
                                   |
                                   v
                    +---------------------------+
                    | Spark Structured Streaming |
                    | watermark + deduplication  |
                    +-------------+-------------+
                                  |
                    +-------------+-------------+
                    |                           |
                    v                           v
              Bronze / Raw                 Silver / Curated
              MinIO / S3                  MinIO / S3
                    |                           |
                    +-------------+-------------+
                                  |
                                  v
                         PostgreSQL Warehouse
                                  |
                                  v
                              dbt
                         staging -> marts
                                  |
                                  v
                     Customer Logistics 360
                     Shipment Performance
```

## Engineering patterns

- Event-driven ingestion with Kafka.
- Shipment-keyed Kafka messages.
- At-least-once producer delivery with `acks=all` and retries.
- Spark event-time processing with a 10-minute watermark.
- Event-level deduplication using `event_id`.
- Partitioned curated storage by ingestion date.
- Data-quality gate before warehouse/dbt processing.
- Separate customer and shipment analytical marts.
- Airflow retries and dependency-aware execution.
- PostgreSQL serving layer for BI/SQL workloads.
