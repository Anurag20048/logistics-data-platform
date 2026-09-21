import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, sum as fsum, avg, max as fmax, min as fmin,
    count, when, round, trim
)

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dt", required=True, help="YYYY-MM-DD")
    return p.parse_args()

args = parse_args()
dt = args.dt

spark = (
    SparkSession.builder
    .appName(f"Logistics-Curated-{dt}")
    .enableHiveSupport()
    .getOrCreate()
)

# IMPORTANTÍSIMO: overwrite solo de la(s) partición(es) presentes (no borra todo el histórico)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# RUTAS
clean_path = "hdfs:///data/transport/clean"
out_path = "hdfs:///data/transport/curated/warehouse_daily"
weather_path = "hdfs:///data/weather/clean/daily"
# Leemos SOLO el dt que toca
df = spark.read.parquet(clean_path).filter(col("dt") == dt)

# Evitar WH NULL / vacíos
df = df.withColumn("origin_warehouse", trim(col("origin_warehouse")))
df = df.filter(col("origin_warehouse").isNotNull() & (col("origin_warehouse") != ""))

curated = (
    df.groupBy("dt", "origin_warehouse")
      .agg(
          count("*").alias("total_trips"),
          avg("delay_minutes").alias("avg_delay"),
          fsum(when(col("delay_minutes") > 30, 1).otherwise(0)).alias("critical_delays"),
          fmax("speed").alias("max_speed_recorded"),
          fmin("ts").alias("first_departure"),
          fmax("ts").alias("last_departure"),
          # Porcentaje de viajes refrigerados
          (fsum(when(col("cargo_type") == "Refrigerado", 1).otherwise(0)) 
           / count("*")).alias("refrigerated_ratio")
      )
      .withColumn(
          "delay_impact_score",
          round(
              (col("avg_delay") * lit(1.5)) +
              (col("critical_delays") * lit(10.0)), 2
          )
      )
)

# --- BLOQUE DE ENRIQUECIMIENTO (Requisito Rúbrica Fase II.2) ---
# Leemos el clima limpio para el mismo día
df_weather = spark.read.parquet(weather_path).filter(col("dt") == dt)

# Unimos tus métricas con los datos de clima mediante un LEFT JOIN
# Esto permite que si no hay datos de clima, tus datos de logística se mantengan
curated_final = curated.join(df_weather, on="dt", how="left")
# ---------------------------------------------------------------

# Escritura idempotente usando el DataFrame enriquecido
writer = curated_final.write.mode("overwrite").partitionBy("dt")

# Si INSISTES en 1 fichero por partición (no recomendado), descomenta:
# curated_final = curated_final.coalesce(1)
# writer = curated_final.write.mode("overwrite").partitionBy("dt")

writer.parquet(out_path)

print(f"OK CURATED LOGISTICS ENRIQUECIDO -> {out_path} (dt={dt})", flush=True)
spark.stop()
