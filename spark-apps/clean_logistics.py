import argparse
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import (
    col, lit, upper, to_timestamp, when,
    input_file_name, trim
)
from pyspark.storagelevel import StorageLevel

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dt", required=True, help="YYYY-MM-DD")
    p.add_argument("--debug", action="store_true", help="Imprimir esquema y muestras")
    return p.parse_args()

args = parse_args()
dt = args.dt

spark = SparkSession.builder.appName(f"Logistics-Clean-{dt}").getOrCreate()

raw_path = f"hdfs:///data/transport/raw/dt={dt}/*.json"
out_path = f"hdfs:///data/transport/clean/dt={dt}"

# Esquema completo para capturar registros corruptos
schema = StructType([
    StructField("truck_id", StringType(), True),
    StructField("route_id", StringType(), True),
    StructField("origin_warehouse", StringType(), True),
    StructField("dest_warehouse", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("speed", StringType(), True),
    StructField("delay_minutes", StringType(), True),
    StructField("cargo_type", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

df_raw = (
    spark.read
    .schema(schema)
    .option("multiLine", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .json(raw_path)
    .withColumn("source_file", input_file_name())
    .persist(StorageLevel.MEMORY_AND_DISK)
)


# Separar datos buenos de malos
df_bad = df_raw.filter(col("_corrupt_record").isNotNull())
df_good = df_raw.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")

# Transformaciones y Limpieza (Fase II.1)
df_clean = (
    df_good
    .withColumn("truck_id", trim(upper(col("truck_id"))))
    .withColumn("route_id", trim(upper(col("route_id"))))
    .withColumn("ts", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("speed", col("speed").cast("double"))
    .withColumn("speed", when(col("speed") < 0, lit(0)).otherwise(col("speed")))
    .withColumn("delay_minutes", col("delay_minutes").cast("int"))
    .withColumn("delay_minutes", when(col("delay_minutes").isNull(), lit(0)).otherwise(col("delay_minutes")))
    .withColumn("dt", lit(dt).cast("string"))
)

# Eliminar duplicados exactos (Requisito Fase II.1)
df_final = df_clean.dropDuplicates(["truck_id", "ts"])

good_cnt = df_final.count()
bad_cnt = df_bad.count()

print(f"[INFO] Filas procesadas OK: {good_cnt} | Filas corruptas: {bad_cnt}", flush=True)

if good_cnt == 0:
    raise SystemExit("ERROR: No hay datos válidos para procesar.")

(
    # Guardado en Parquet (Capa Clean)
    df_final.select(
        "dt", "ts", "truck_id", "route_id",
        "origin_warehouse", "dest_warehouse","latitude", "longitude",
        "speed", "delay_minutes", "cargo_type"
    )
    # .coalesce(1)  # opcional: déjalo solo si lo necesitas
    .write.mode("overwrite")
    .parquet(out_path)
)

if args.debug:
    df_final.show(10, truncate=False)

df_raw.unpersist()
spark.stop()
