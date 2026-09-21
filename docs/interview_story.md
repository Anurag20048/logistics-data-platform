# Interview Story

I built an end-to-end logistics data platform to demonstrate how I would handle both batch and streaming workloads.

Kafka receives shipment events. Spark Structured Streaming processes events using event time, watermarking and event-id deduplication. MinIO acts as the S3-compatible lakehouse storage layer, separating raw and curated data. Airflow orchestrates the batch quality and transformation workflow. PostgreSQL provides the analytical serving layer, while dbt creates tested customer and shipment marts.

The platform also has a quality gate that validates schemas, uniqueness, referential integrity, timestamp validity and business ranges before downstream processing.

The main analytical outputs are Customer Logistics 360 and Shipment Performance, which expose delay rate, delivery rate, shipment volume, distance, truck usage and operational delay metrics.
