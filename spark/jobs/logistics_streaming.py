from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

KAFKA = "kafka:9092"
BRONZE = "s3a://logistics/bronze/logistics_events"
SILVER = "s3a://logistics/silver/logistics_events"
CHECKPOINT = "s3a://logistics/checkpoints/logistics-stream"

schema = StructType([
    StructField("event_id", StringType()),
    StructField("shipment_id", StringType()),
    StructField("truck_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("event_type", StringType()),
    StructField("delay_minutes", IntegerType()),
    StructField("event_ts", StringType()),
])

spark = (
    SparkSession.builder.appName("LogisticsStreaming")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA)
       .option("subscribe", "logistics-events")
       .option("startingOffsets", "latest")
       .load())

events = (
    raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
       .select("e.*")
       .withColumn("event_time", F.to_timestamp("event_ts"))
       .filter(F.col("event_id").isNotNull())
       .withWatermark("event_time", "10 minutes")
       .dropDuplicates(["event_id"])
)

silver = events.withColumn("ingest_date", F.to_date("event_time"))

query = (
    silver.writeStream.format("parquet")
    .outputMode("append")
    .partitionBy("ingest_date")
    .option("path", SILVER)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()
