#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
batch_analytics.py
=================
Batch analytics de grafos (GraphFrames) para la práctica.

Alineación con tus tablas reales:
- Hive external: logistics_lake.warehouse_traffic_geometry
  LOCATION hdfs:///data/transport/curated/warehouse_importance
  Columnas: warehouse_id, importance_score, last_update  PARTITIONED BY dt

- Cassandra: logistics.warehouse_realtime_stats
  PK: warehouse_id
  Columnas: importance_score, last_update

Uso:
  spark-submit batch_analytics.py --dt 2026-02-12

Notas:
- Calculamos PageRank como métrica de "importancia" de almacén.
- Escribimos en parquet particionado por dt para que Hive pueda hacer MSCK REPAIR.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, explode
from graphframes import GraphFrame


# --- Config "contrato" con Hive (según tu DESCRIBE FORMATTED) ---
HIVE_IMPORTANCE_OUT = "hdfs:///data/transport/curated/warehouse_importance"

# --- Input batch (tu clean ya existe y está particionado por dt) ---
CLEAN_BASE = "hdfs:///data/transport/clean"

# --- Cassandra (speed layer / consulta rápida) ---
CASSANDRA_HOST = "cassandra"          # en tu docker compose el service es "cassandra"
CASSANDRA_PORT = "9042"
CASSANDRA_KEYSPACE = "logistics"
CASSANDRA_TABLE = "warehouse_realtime_stats"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dt", required=True, help="YYYY-MM-DD (ej: 2026-02-12)")
    p.add_argument("--write-cassandra", action="store_true", help="Si se indica, escribe también a Cassandra")
    p.add_argument("--top", type=int, default=10, help="Top N a mostrar por logs")
    return p.parse_args()


def main():
    args = parse_args()
    dt = args.dt

    spark = (
        SparkSession.builder
        .appName(f"Logistics-Graph-Analytics-{dt}")
        # Overwrite controlado por partición dt
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Conector Cassandra (host)
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", CASSANDRA_PORT)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[BATCH] 1) Leyendo CLEAN para dt={dt}", flush=True)
    df_clean = spark.read.parquet(f"{CLEAN_BASE}/dt={dt}")

    # ----------------------------------------------------------------------
    # 2) Construcción del grafo
    # ----------------------------------------------------------------------
    # Edges: origin_warehouse -> dest_warehouse
    # Usamos delay_minutes como atributo (puede servir para futuras métricas)
    print("[BATCH] 2) Construyendo edges/vertices del grafo", flush=True)

    edges = (
        df_clean
        .select(
            col("origin_warehouse").cast("string").alias("src"),
            col("dest_warehouse").cast("string").alias("dst"),
            col("delay_minutes").cast("double").alias("delay_minutes"),
        )
        .na.drop(subset=["src", "dst"])
        .dropDuplicates(["src", "dst"])  # evita duplicados masivos por día
        .cache()
    )
    edges_cnt = edges.count()
    print(f"[BATCH]    edges={edges_cnt}", flush=True)

    # Vertices: conjunto de almacenes presentes en el día
    vertices = (
        edges.select(col("src").alias("id"))
        .union(edges.select(col("dst").alias("id")))
        .dropDuplicates(["id"])
        .cache()
    )
    vertices_cnt = vertices.count()
    print(f"[BATCH]    vertices={vertices_cnt}", flush=True)

    g = GraphFrame(vertices, edges)

    # ----------------------------------------------------------------------
    # 3) PageRank (importancia de almacenes)
    # ----------------------------------------------------------------------
    print("[BATCH] 3) Calculando PageRank", flush=True)

    pr = g.pageRank(resetProbability=0.15, maxIter=10)

    # IMPORTANTE: el esquema y columnas deben casar con Hive:
    # warehouse_id, importance_score, last_update, dt (partición)
    warehouse_importance = (
        pr.vertices
        .select(
            col("id").alias("warehouse_id"),
            col("pagerank").cast("double").alias("importance_score")
        )
        .withColumn("last_update", F.current_timestamp())
        .withColumn("dt", lit(dt))
    )

    print("[BATCH]    Top warehouses (PageRank):", flush=True)
    warehouse_importance.orderBy(col("importance_score").desc()).show(args.top, truncate=False)

    # ----------------------------------------------------------------------
    # 3bis) Caminos más cortos (en número de saltos) entre almacenes
    # ----------------------------------------------------------------------
    print("[BATCH] 3bis) Calculando caminos más cortos (en nº de saltos)", flush=True)

    # 1) Elegimos los "landmarks" (aquí, todos los almacenes)
    landmarks = [row["id"] for row in vertices.select("id").collect()]

    # 2) shortestPaths devuelve un DataFrame con:
    #    id = almacén
    #    distances = mapa { landmark_id -> nº de saltos }
    sp = g.shortestPaths(landmarks=landmarks)

    # 3) Aplanamos el mapa distances en filas (from_warehouse, to_warehouse, hops)
    shortest_paths = (
        sp.select(
            col("id").alias("from_warehouse"),
            explode("distances").alias("to_warehouse", "hops")
        )
        .withColumn("dt", lit(dt))  # misma partición temporal que el resto
    )

    # 3.bis) Mostrar algo de información en logs
    total_pairs = shortest_paths.count()
    print(f"[BATCH]    total_shortest_paths={total_pairs}", flush=True)
    print("[BATCH]    Ejemplo de caminos mínimos (from_warehouse -> to_warehouse, hops):", flush=True)
    (
        shortest_paths
        .orderBy(col("hops").asc(), col("from_warehouse").asc(), col("to_warehouse").asc())
        .show(10, truncate=False)
    )

    # 4) Guardamos en HDFS como capa curated de caminos mínimos
    out_shortest = f"hdfs:///data/transport/curated/warehouse_shortest_paths/dt={dt}"

    (
        shortest_paths
        .write
        .mode("overwrite")
        .parquet(out_shortest)
    )

    print(f"[BATCH]    OK shortest paths -> {out_shortest}", flush=True)



    #4) Persistencia en HDFS (Hive external)
    # ----------------------------------------------------------------------
    # Esto alimenta: logistics_lake.warehouse_traffic_geometry
    # LOCATION: /data/transport/curated/warehouse_importance
    print("[BATCH] 4) Guardando parquet para Hive EXTERNAL", flush=True)

    (
        warehouse_importance
        .write
        .mode("overwrite")      # con partitionOverwriteMode=dynamic pisa solo dt
        .partitionBy("dt")
        .parquet(HIVE_IMPORTANCE_OUT)
    )

    print(f"[BATCH]    OK -> {HIVE_IMPORTANCE_OUT}/dt={dt}", flush=True)

    # ----------------------------------------------------------------------
    # 5) (Opcional) Cassandra speed layer
    # ----------------------------------------------------------------------
    # Tabla: logistics.warehouse_realtime_stats (PK warehouse_id)
    # append actúa como upsert en Cassandra cuando la PK coincide.
    if args.write_cassandra:
        print("[BATCH] 5) Escribiendo a Cassandra logistics.warehouse_realtime_stats", flush=True)
        (
            warehouse_importance
            .select("warehouse_id", "importance_score", "last_update")
            .write
            .format("org.apache.spark.sql.cassandra")
            .options(keyspace=CASSANDRA_KEYSPACE, table=CASSANDRA_TABLE)
            .mode("append")
            .save()
        )
        print("[BATCH]    OK Cassandra upsert", flush=True)

    spark.stop()
    print(f"[BATCH] FIN dt={dt}", flush=True)


if __name__ == "__main__":
    main()
