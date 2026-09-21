-- ============================================================================
--  Inicialización de Hive para el proyecto "Data Lakehouse Logística"
--  DB: logistics_lake
--  Tablas EXTERNAL sobre ficheros Parquet ya generados por Spark
-- ============================================================================

CREATE DATABASE IF NOT EXISTS logistics_lake;
USE logistics_lake;

-- ============================================================================
-- 1) Tabla RAW / Estadísticas de retrasos a 15 minutos (delay_stats_15m)
--    Fuente: /data/transport/analytics/delay_15m/dt=YYYY-MM-DD/*.parquet
--    Esquema generado por streaming_logistics.py + batch/clean
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS delay_stats_15m (
  warehouse_id        STRING,
  window_start        TIMESTAMP,
  window_end          TIMESTAMP,
  avg_delay_minutes   DOUBLE,
  total_trips         INT
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/data/transport/analytics/delay_15m';

-- ============================================================================
-- 2) Tabla CURATED: daily_warehouse_stats
--    Fuente: /data/transport/curated/daily_warehouse_stats/dt=YYYY-MM-DD/*.parquet
--    Contiene métricas diarias por almacén (viajes, retrasos, etc.)
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS daily_warehouse_stats (
  warehouse_id       STRING,
  total_trips        INT,
  avg_delay_minutes  DOUBLE,
  max_delay_minutes  DOUBLE,
  min_delay_minutes  DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/data/transport/curated/daily_warehouse_stats';

-- ============================================================================
-- 3) Tabla CURATED: daily_weather
--    Fuente: /data/weather/curated/dt=YYYY-MM-DD/*.parquet
--    Agregados diarios de la API meteorológica por almacén
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS daily_weather (
  warehouse_id  STRING,
  `date`        STRING,
  avg_temp_c    DOUBLE,
  avg_wind_kmh  DOUBLE
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/data/weather/curated';

-- ============================================================================
-- 4) WAREHOUSE IMPORTANCE (PageRank sobre grafo de rutas)
--    Fuente: /data/transport/curated/warehouse_importance/dt=YYYY-MM-DD/*.parquet
--    Generado por batch_analytics.py (GraphFrames)
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS warehouse_importance (
  warehouse_id      STRING,
  importance_score  DOUBLE,
  last_update       TIMESTAMP
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/data/transport/curated/warehouse_importance';

-- ============================================================================
-- 5) Caminos más cortos entre almacenes (shortest paths)
--    Fuente: /data/transport/curated/warehouse_shortest_paths/dt=YYYY-MM-DD/*.parquet
--    Generado por batch_analytics.py (all-pairs shortest paths)
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS warehouse_shortest_paths (
  from_warehouse STRING,
  to_warehouse   STRING,
  hops           INT
)
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION '/data/transport/curated/warehouse_shortest_paths';

-- ============================================================================
-- 6) Dimensión de rutas (dim_routes)
--    Fuente: /data/transport/dim/dim_routes/*.parquet (o similar)
--    Tabla sin particiones (dimensional)
-- ============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS dim_routes (
  route_id          STRING,
  origin_warehouse  STRING,
  dest_warehouse    STRING,
  distance_km       DOUBLE
)
STORED AS PARQUET
LOCATION '/data/transport/dim/dim_routes';

-- ============================================================================
-- 7) Levantar particiones de tablas particionadas
--    (delay_stats_15m, daily_warehouse_stats, daily_weather,
--     warehouse_importance, warehouse_shortest_paths)
-- ============================================================================

MSCK REPAIR TABLE delay_stats_15m;
MSCK REPAIR TABLE daily_warehouse_stats;
MSCK REPAIR TABLE daily_weather;
MSCK REPAIR TABLE warehouse_importance;
MSCK REPAIR TABLE warehouse_shortest_paths;
