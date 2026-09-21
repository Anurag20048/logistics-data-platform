import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dt", required=True, help="YYYY-MM-DD")
    return p.parse_args()

args = parse_args()
dt = args.dt

spark = SparkSession.builder.appName("Weather-Clean").getOrCreate()

raw_path = "hdfs:///data/weather/raw/*/*.json"
out_path = "hdfs:///data/weather/clean/daily"

df = spark.read.option("multiLine", "true").json(raw_path)

df_clean = df.select(
    col("current_weather.temperature").cast("double").alias("temp_c"),
    col("current_weather.windspeed").cast("double").alias("wind_kmh"),
    col("current_weather.weathercode").cast("int").alias("condition_code"),
    lit("Madrid").alias("location"),
    lit(dt).alias("dt")              # <- STRING, no DateType
).distinct()

df_clean.write.mode("overwrite").partitionBy("dt").parquet(out_path)
spark.stop()
