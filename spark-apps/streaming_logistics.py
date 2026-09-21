#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
streaming_logistics.py
======================

OBJETIVO (práctica)
-------------------
1) Consumir eventos "válidos" desde Kafka (topic transport_filtered).
2) Enriquecer cada evento con un maestro de rutas (dim_routes).
3) Generar una métrica agregada en ventanas de 15 minutos por almacén (warehouse_id)
   y guardarla en HDFS en formato Parquet (para que Hive la consulte como tabla externa).
4) Mantener en Cassandra el "último estado" de cada camión (speed layer).

IMPORTANTE SOBRE HIVE / METASTORE
---------------------------------
Aunque Hive muestre la tabla logistics_lake.dim_routes por beeline,
Spark solo puede hacer spark.table("logistics_lake.dim_routes") si tiene acceso
al Hive Metastore (thrift) o a un catálogo compartido.

En tu entorno Docker, has visto que NO hay puerto 9083 escuchando (metastore thrift),
por eso Spark NO puede resolver la tabla por nombre.

Solución robusta para la práctica: leer dim_routes DIRECTAMENTE desde HDFS (Parquet),
que tú ya comprobaste que funciona.

Tablas reales que usas (según tus DESCRIBE):
- HIVE external:
  logistics_lake.delay_stats_15m  (LOCATION hdfs:///data/transport/analytics/delay_15m)
- HDFS / Hive managed:
  logistics_lake.dim_routes en:
  hdfs://namenode:8020/user/hive/warehouse/logistics_lake.db/dim_routes

Cassandra real:
- logistics.truck_last_state (truck_id PK)
- logistics.warehouse_realtime_stats (si más adelante quieres usarla)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.window import Window


# -----------------------------
# CONFIGURACIÓN (ajústala si tu compose cambia)
# -----------------------------

# Kafka
KAFKA_BOOTSTRAP = "kafka:9092"
TOPIC = "transport_filtered"

HDFS_NN = "hdfs://kraft-namenode:8020"

# Maestro dim_routes: lectura directa desde HDFS (evita metastore)
DIM_ROUTES_PATH = f"{HDFS_NN}/user/hive/warehouse/logistics_lake.db/dim_routes"

# Output
HDFS_OUT_15M = f"{HDFS_NN}/data/transport/analytics/delay_15m"

# Checkpoints
CHECKPOINT_BASE = f"{HDFS_NN}/checkpoints/streaming_logistics"


# Cassandra (tu servicio en docker compose es "cassandra"; contenedor "cassandra-node")
CASSANDRA_HOST = "cassandra"
CASSANDRA_KEYSPACE = "logistics"
CASSANDRA_TRUCK_TABLE = "truck_last_state"


# -----------------------------
# ESQUEMA DEL JSON QUE LLEGA EN KAFKA
# -----------------------------
# Nota: desde NiFi suelen llegar números como string -> casteamos en Spark.
EVENT_SCHEMA = StructType([
    StructField("truck_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("origin_warehouse", StringType(), True),
    StructField("dest_warehouse", StringType(), True),
    StructField("timestamp", StringType(), True),      # "yyyy-MM-dd HH:mm:ss"
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("speed", StringType(), True),
    StructField("delay_minutes", StringType(), True),
    StructField("cargo_type", StringType(), True),
])


def build_spark() -> SparkSession:
    """
    Crea la SparkSession.

    - NO usamos enableHiveSupport() porque en tu entorno no hay metastore thrift accesible.
      (y no quieres bloquearte por configuración extra)
    - Configuramos Cassandra connector.
    """
    return (
        SparkSession.builder
        .appName("streaming_logistics_filtered_enriched")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .getOrCreate()
    )


def load_dim_routes_static(spark: SparkSession):
    """
    Carga el maestro dim_routes como DataFrame ESTÁTICO desde HDFS.

    Esto cubre el requisito de "enriquecer con maestros", pero sin depender del metastore.
    """
    dim = (
        spark.read.parquet(DIM_ROUTES_PATH)
        .select(
            F.upper(F.trim(F.col("route_id"))).alias("route_id"),
            F.col("route_type"),
            F.col("expected_minutes").cast("int").alias("expected_minutes"),
        )
        .dropDuplicates(["route_id"])
    )
    return F.broadcast(dim)  # pequeño -> broadcast para join eficiente


def write_last_state_to_cassandra(batch_df, batch_id: int):
    """
    foreachBatch (speed layer):
    ---------------------------
    - En cada micro-batch, cogemos el evento MÁS RECIENTE por truck_id
      (según event_ts) y lo upsert-eamos en Cassandra.

    Por qué foreachBatch:
    - Cassandra no es un sink nativo "exactly-once" en streaming como Parquet.
    - foreachBatch nos permite controlar el upsert por PK y quedarnos con el último estado.
    """
    if batch_df.isEmpty():
        return

    w = Window.partitionBy("truck_id").orderBy(F.col("event_ts").desc())

    latest = (
        batch_df
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("updated_at", F.current_timestamp())
    )

    # Mapear exactamente a la tabla logistics.truck_last_state
    (
        latest.select(
            F.col("truck_id"),
            F.col("delay_minutes").cast("int").alias("delay_minutes"),
            F.col("event_ts").alias("event_ts"),
            F.col("lat").cast("double").alias("lat"),
            F.col("lon").cast("double").alias("lon"),
            F.col("route_id"),
            F.col("speed").cast("double").alias("speed"),
            F.col("updated_at"),
            F.col("warehouse_id"),
        )
        .write
        .format("org.apache.spark.sql.cassandra")
        .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TRUCK_TABLE)
        .mode("append")   # en Cassandra es upsert por PK (truck_id)
        .save()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # 0) Cargar maestro (estático) desde HDFS
    dim_routes = load_dim_routes_static(spark)

    # 1) Leer stream de Kafka (solo los "buenos" según tu NiFi -> topic transport_filtered)
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")  # práctica: empezamos desde lo nuevo
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2) Parseo JSON + tipado + columnas estándar
    parsed = (
        kafka_df.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        # event time real para ventanas
        .withColumn("event_ts", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
        .drop("timestamp")
        # cast numérico
        .withColumn("lat", F.col("latitude").cast("double"))
        .withColumn("lon", F.col("longitude").cast("double"))
        .drop("latitude", "longitude")
        .withColumn("speed", F.col("speed").cast("double"))
        .withColumn("delay_minutes", F.col("delay_minutes").cast("int"))
        # normalizamos IDs
        .withColumn("truck_id", F.upper(F.trim(F.col("truck_id"))))
        .withColumn("route_id", F.upper(F.trim(F.col("route_id"))))
        # warehouse_id de referencia (para esta práctica usamos el destino)
        .withColumn("warehouse_id", F.trim(F.col("dest_warehouse")).cast("string"))
    )

    # (opcional pero recomendable) descartar eventos sin event_ts o sin truck_id
    parsed = parsed.filter(F.col("event_ts").isNotNull() & F.col("truck_id").isNotNull())

    # 3) Enriquecimiento con dim_routes (join estático)
    enriched = parsed.join(dim_routes, on="route_id", how="left")

    # 4) Histórica: ventanas 15 min por warehouse_id -> Parquet en HDFS
    #    Hive espera en delay_stats_15m:
    #    window_start, window_end, warehouse_id, avg_delay, cnt, partición dt
    windowed_15m = (
        enriched
        .withWatermark("event_ts", "10 minutes")  # tolera eventos atrasados moderadamente
        .groupBy(
            F.window(F.col("event_ts"), "15 minutes"),
            F.col("warehouse_id")
        )
        .agg(
            F.avg("delay_minutes").alias("avg_delay"),
            F.count(F.lit(1)).cast("bigint").alias("cnt"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("warehouse_id"),
            F.col("avg_delay").cast("double").alias("avg_delay"),
            F.col("cnt"),
            F.date_format(F.col("window.start"), "yyyy-MM-dd").alias("dt"),
        )
    )

    q_hdfs = (
        windowed_15m.writeStream
        .format("parquet")
        .option("path", HDFS_OUT_15M)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/delay_stats_15m")
        .partitionBy("dt")
        # Con watermark + agregación por ventana, lo más estable para ficheros es append
        .outputMode("append")
        .start()
    )

    # 5) Speed layer: último estado por truck -> Cassandra
    q_cassandra = (
        enriched.writeStream
        .foreachBatch(write_last_state_to_cassandra)
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/truck_last_state")
        .outputMode("update")  # válido con foreachBatch
        .start()
    )

    print("[STREAM] Ejecutando: (1) Parquet 15m en HDFS + (2) last_state en Cassandra", flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
