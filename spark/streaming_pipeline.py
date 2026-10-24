"""
Reads sensor telemetry off Kafka, scores each reading for anomalies
using simple rule-based thresholds, and writes results to PostgreSQL
on every micro-batch.

Run inside the Spark container:
    docker exec -it predictive-maintenance-pipeline-spark-1 spark-submit \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0 \
      /spark-jobs/streaming_pipeline.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, window, avg, stddev, count
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Inside Docker, containers reach each other by service name, not localhost.
KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC = "sensor_telemetry"

PG_URL = "jdbc:postgresql://postgres:5432/telemetry_db"
PG_PROPS = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver",
}

schema = (
    StructType()
    .add("device_id", StringType())
    .add("timestamp", TimestampType())
    .add("temperature", DoubleType())
    .add("vibration", DoubleType())
    .add("pressure", DoubleType())
    .add("rpm", DoubleType())
)

spark = SparkSession.builder.appName("TelemetryStreamingPipeline").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed = raw.selectExpr("CAST(value AS STRING)").select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# Same rule-based logic the producer uses to inject faults, so you
# can prove end-to-end: trigger an anomaly, watch it get caught here.
scored = (
    parsed.withColumn(
        "is_anomaly",
        (col("temperature") > 85.0) | (col("vibration") > 4.0),
    )
    .withColumn(
        "alert_level",
        when(col("temperature") > 95.0, "CRITICAL")
        .when(col("temperature") > 85.0, "WARNING")
        .otherwise("NORMAL"),
    )
)


def write_batch(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return

    batch_df.write.format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "raw_telemetry") \
        .options(**PG_PROPS) \
        .mode("append") \
        .save()

    alerts = batch_df.filter(col("is_anomaly") == True)  # noqa: E712
    if not alerts.rdd.isEmpty():
        alerts.selectExpr(
            "device_id", "timestamp",
            "((temperature/100.0 + vibration/8.0)/2.0) as anomaly_score",
            "is_anomaly", "alert_level",
        ).write.format("jdbc") \
            .option("url", PG_URL) \
            .option("dbtable", "sensor_alerts") \
            .options(**PG_PROPS) \
            .mode("append") \
            .save()


query = scored.writeStream \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", "/tmp/checkpoints/telemetry") \
    .start()

# ---- Rolling features: 5-minute window, sliding every 1 minute, per device ----
features = (
    parsed.withWatermark("timestamp", "1 minute")
    .groupBy(window(col("timestamp"), "5 minutes", "1 minute"), col("device_id"))
    .agg(
        avg("temperature").alias("avg_temperature"),
        stddev("temperature").alias("std_temperature"),
        avg("vibration").alias("avg_vibration"),
        stddev("vibration").alias("std_vibration"),
        count("*").alias("reading_count"),
    )
    .select(
        "device_id",
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "avg_temperature",
        "std_temperature",
        "avg_vibration",
        "std_vibration",
        "reading_count",
    )
)


def write_features(batch_df, batch_id: int) -> None:
    if batch_df.rdd.isEmpty():
        return
    batch_df.write.format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "device_rolling_features") \
        .options(**PG_PROPS) \
        .mode("append") \
        .save()


features_query = (
    features.writeStream
    .outputMode("append")
    .foreachBatch(write_features)
    .option("checkpointLocation", "/tmp/checkpoints/rolling_features")
    .start()
)

spark.streams.awaitAnyTermination()