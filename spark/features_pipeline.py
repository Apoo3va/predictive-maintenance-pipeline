"""
Reads DSP features from the Kafka topic `sensor_features`, flags faults
from the vibration signature, and writes them to Postgres.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
TOPIC = "sensor_features"
PG_URL = "jdbc:postgresql://postgres:5432/telemetry_db"
PG_PROPS = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

schema = (
    StructType()
    .add("device_id", StringType())
    .add("timestamp", TimestampType())
    .add("rpm", DoubleType())
    .add("temperature", DoubleType())
    .add("rms", DoubleType())
    .add("peak", DoubleType())
    .add("crest_factor", DoubleType())
    .add("kurtosis", DoubleType())
    .add("dominant_hz", DoubleType())
    .add("band_energy", DoubleType())
)

spark = SparkSession.builder.appName("FeaturesPipeline").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

parsed = raw.selectExpr("CAST(value AS STRING)").select(
    from_json(col("value"), schema).alias("d")
).select("d.*")

# Noise filter: drop malformed rows and impossible values
clean = parsed.filter(
    col("device_id").isNotNull()
    & col("timestamp").isNotNull()
    & col("temperature").between(-40, 150)
    & col("rms").between(0, 10)
    & col("band_energy").between(0, 1)
)

# Fault flag from the vibration signature (band energy and kurtosis)
scored = clean.withColumn("is_fault", (col("band_energy") > 0.2) | (col("kurtosis") > 2.8))


def write_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return
    batch_df.write.format("jdbc") \
        .option("url", PG_URL) \
        .option("dbtable", "vibration_features") \
        .options(**PG_PROPS) \
        .mode("append") \
        .save()


query = (
    scored.writeStream.foreachBatch(write_batch)
    .option("checkpointLocation", "/tmp/checkpoints/vibration_features")
    .start()
)
query.awaitTermination()