# Technical Architecture — BigData Logistics Platform

## Overview

This document describes the technical architecture of the BigData Logistics Platform, a Lambda Architecture implementation for real-time and historical monitoring of a simulated freight logistics network.

## System Architecture Pattern

The platform implements the **Lambda Architecture** pattern, which processes data through two parallel paths:

- **Speed Layer**: Real-time path (NiFi → Kafka → Spark Streaming → Cassandra) for low-latency queries
- **Batch Layer**: Historical path (HDFS → Spark Batch → Hive) for high-throughput analytics
- **Serving Layer**: Unified Streamlit dashboard that queries both layers simultaneously

## Component Deep Dive

### 1. Data Ingestion — Apache NiFi 2.8

NiFi runs a flow with two parallel ingestion branches:

**Transport Events Branch**
- `GenerateFlowFile` processor simulates a truck GPS beacon every 5 seconds
- Each event is a JSON record with: `truck_id`, `route_id`, `origin_warehouse`, `dest_warehouse`, `timestamp`, `latitude`, `longitude`, `speed`, `delay_minutes`, `cargo_type`
- `EvaluateJsonPath` extracts fields for validation
- `RouteOnAttribute` enforces business rules:
  - `speed` ∈ [0, 120] km/h
  - `delay_minutes` ∈ [0, 180] min
  - `latitude`, `longitude` are numeric
- Valid events → topic `transport_filtered`
- Invalid events → topic `transport_raw` (DLQ) + file sink to `/data/audit/` on HDFS

**Weather Branch**
- `InvokeHTTP` polls Open-Meteo API for Madrid (lat=40.4168, lon=-3.7038) every 30 seconds
- Response is split and pushed to HDFS `/data/weather/raw/`

### 2. Messaging — Apache Kafka 3.9.1 (KRaft)

Single-broker setup in KRaft mode (no ZooKeeper dependency):
- `transport_raw` — all events before validation (audit trail)
- `transport_filtered` — validated events consumed by Spark Streaming
- Internal topics: `__consumer_offsets`, `__transaction_state`

Kafka listens on:
- `PLAINTEXT://kafka:9092` — internal Docker network
- `PLAINTEXT_HOST://localhost:39093` — external host access

### 3. Distributed Storage — HDFS 3.4.2

HDFS directory structure:
```
/
├── data/
│   ├── transport/
│   │   ├── raw/          ← raw events from Kafka (DLQ + audit)
│   │   ├── clean/        ← output of clean_logistics.py
│   │   ├── curated/      ← output of curated_logistics.py (with weather)
│   │   └── analytics/    ← output of batch_analytics.py (PageRank, paths)
│   └── weather/
│       ├── raw/          ← raw Open-Meteo JSON responses
│       └── clean/        ← output of clean_weather.py
├── spark-jars/           ← Spark JARs uploaded by spark-client container
├── tmp/
│   └── spark-checkpoints/ ← Structured Streaming checkpoint location
└── user/
    └── hive/
        └── warehouse/    ← Hive managed tables (logistics_lake)
```

### 4. Real-Time Processing — Spark Structured Streaming

`streaming_logistics.py` runs as a YARN application (deploy-mode: client):

```
Kafka (transport_filtered)
        │
        ▼
  Parse JSON schema
        │
        ▼
  Enrich with dim_routes (HDFS parquet broadcast join)
        │
        ▼
  15-minute tumbling window aggregations:
  - avg_speed, avg_delay per warehouse_id
  - event_count, truck_count
        │
     ┌──┴──┐
     ▼     ▼
 HDFS    Cassandra
(delay_  (truck_last_state +
stats_    warehouse_realtime_stats)
15m)      via foreachBatch upsert
```

**Key technical decisions:**
- `foreachBatch` for Cassandra writes — enables upsert-by-PK semantics
- Checkpoint stored in HDFS for fault tolerance
- `spark.executorEnv.PYSPARK_PYTHON` must point to Python 3.8 on NodeManager

### 5. Batch Processing — Spark on YARN

Orchestrated monthly by Airflow. Job execution order:

```
bootstrap_check
     │
     ├── clean_logistics (PySpark: raw → clean parquet, date partitioned)
     │
     ├── clean_weather   (PySpark: raw JSON → clean parquet)
     │
     ├── curated_logistics (PySpark: clean transport LEFT JOIN clean weather
     │                      → daily_warehouse_weather, weather_delay_index)
     │
     ├── batch_analytics  (GraphFrames PageRank + shortestPaths
     │                     → warehouse_importance, warehouse_shortest_paths)
     │
     ├── register_hive    (SparkSQL CREATE TABLE IF NOT EXISTS USING parquet
     │                     LOCATION 'hdfs://...' for all curated tables)
     │
     └── cleanup          (removes tmp files, rotates old checkpoints)
```

**`partitionOverwriteMode=dynamic`**: Only affected date partitions are overwritten, making reruns fully idempotent.

### 6. Serving Layer — Apache Hive 2.3.2

HiveServer2 provides a JDBC/Thrift interface for SQL queries over HDFS parquet data.

**Why `bde2020/hive:2.3.2`?**: Compatible with the Hadoop 3.x HDFS format while being a stable image with known configuration patterns.

**Note**: The Hive Metastore Thrift server is co-located with HiveServer2 inside the same container. It is not exposed as a separate service, which is why Spark batch jobs read `dim_routes` directly from HDFS rather than through the metastore API.

### 7. Resource Management — Apache YARN 3.4.2

YARN manages resource allocation for all Spark jobs (both streaming and batch).

Custom NodeManager image (`Dockerfile.nodemanager`):
- Base: `apache/hadoop:3.4.2` (CentOS 7)
- Adds Python 3.8 via Software Collections Library (SCL) from CentOS vault
- Installs Spark 3.5.8 to provide `ExecutorLauncher` on the worker classpath
- Configures `PYSPARK_PYTHON=/usr/local/bin/python3`

**Why SCL?**: The base Hadoop image uses CentOS 7 which ships with Python 2.7. SCL is the standard CentOS 7 mechanism to install Python 3.x without breaking system Python (required by yum).

**Dynamic allocation disabled**: `spark.dynamicAllocation.enabled=false` avoids executor count oscillation in a constrained single-machine environment.

### 8. Orchestration — Apache Airflow 2.11.1

DAG: `logistics_pipeline_yarn` (schedule: `@monthly`)

- Uses `LocalExecutor` (no Celery/Redis dependency)
- PostgreSQL backend for metadata
- BashOperator tasks submit `spark-submit` commands via Docker socket

### 9. Visualization — Streamlit Dashboard

`dashboard_logistics.py` connects to:
- **Cassandra** (`cassandra-driver`): queries `truck_last_state` and `warehouse_realtime_stats` for real-time view
- **Hive** (`pyhive` + `thrift`): queries `logistics_lake` tables for historical analytics

Dashboard displays:
- Live truck map (Pydeck / Mapbox)
- Real-time warehouse KPIs (Cassandra)
- Historical delay trends (Hive + Plotly)
- GraphFrames PageRank results (warehouse importance)

## Network Topology

All services share the `socnet` bridge network. Container hostnames match service names:

```
socnet (172.18.0.0/16)
├── kafka          (kraft-kafka)         :9092 internal
├── kraft-namenode                       :8020 HDFS RPC, :9870 UI
├── datanode                             :9864
├── resourcemanager                      :8088 YARN UI
├── nodemanager                          :8042
├── jobhistory                           :19888 → host 18188
├── cassandra                            :9042 → host 19042
├── kraft-nifi     (kraft-nifi)          :8443 → host 18443
├── hive-metastore-db                    :5432
├── kraft-hive-server                    :10000 → host 11000
├── kraft-postgres                       :5432
├── kraft-airflow-webserver              :8080 → host 18082
├── kraft-airflow-scheduler
├── kraft-spark-client
└── kraft-streaming-logistics
```

## Security Notes

- All credentials are stored in `.env` (never commit this file)
- NiFi uses self-signed TLS (accept browser warning on first access)
- Cassandra and HDFS have no authentication in this dev configuration
- Docker socket mount (`/var/run/docker.sock`) allows Airflow to submit Docker-based tasks

## Scalability Considerations

This setup is designed for a **single-machine academic/demo environment**. For production:
- Kafka: increase to 3+ brokers with replication factor 3
- HDFS: add DataNodes; set replication to 3
- YARN: add NodeManager nodes
- Cassandra: use a 3-node rack-aware cluster
- Airflow: switch to CeleryExecutor or KubernetesExecutor
