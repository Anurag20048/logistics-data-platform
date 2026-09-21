-- ============================================================================
-- Keyspace & tablas Cassandra para el proyecto de logística
-- ============================================================================
-- Se asume un solo nodo, por eso usamos SimpleStrategy RF=1
-- ============================================================================

CREATE KEYSPACE IF NOT EXISTS logistics
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};

USE logistics;

-- --------------------------------------------------------------------------
-- Tabla de “último estado del camión” (speed layer)
-- Coincide con lo que tienes ahora mismo en el cluster:
-- truck_id | delay_minutes | event_ts | lat | lon | route_id | speed | updated_at | warehouse_id
-- --------------------------------------------------------------------------  

CREATE TABLE IF NOT EXISTS truck_last_state (
    truck_id text PRIMARY KEY,
    delay_minutes int,
    event_ts timestamp,
    lat double,
    lon double,
    route_id text,
    speed double,
    updated_at timestamp,
    warehouse_id text
);

-- --------------------------------------------------------------------------
-- Tabla de “estado en tiempo casi real por almacén” (PageRank diario)
-- Usada por batch_analytics.py cuando pasas --write-cassandra
-- --------------------------------------------------------------------------  

CREATE TABLE IF NOT EXISTS warehouse_realtime_stats (
    warehouse_id text PRIMARY KEY,
    importance_score double,
    last_update timestamp
);
