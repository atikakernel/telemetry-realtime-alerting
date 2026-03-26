from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

# Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC = "telemetry-acc"
CHECKPOINT_DIR = "/home/diegokernel/proyectos/sim-racing-telemetry/spark-consumer/checkpoint"

# Define the schema matching acc_producer.py
schema = StructType([
    StructField("timestamp", DoubleType(), True),
    StructField("packetId", IntegerType(), True),
    StructField("rpms", IntegerType(), True),
    StructField("speedKmh", DoubleType(), True),
    StructField("gear", IntegerType(), True),
    StructField("throttle", DoubleType(), True),
    StructField("brake", DoubleType(), True),
    StructField("car", StringType(), True),
    StructField("track", StringType(), True)
])

def main():
    # Initialize Spark Session
    # Note: Requires spark-sql-kafka connector
    spark = SparkSession.builder \
        .appName("ACC-Telemetry-Alerting") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    print(f"📡 Reading from Kafka topic: {TOPIC}...")

    # 1. Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # 2. Parse JSON and add Processing Timestamp
    # We use current_timestamp() to handle the windowing in Spark
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .withColumn("processing_time", current_timestamp())

    # 3. Windowed Aggregation (5s window, 5s sliding)
    # This calculates the average RPM every 5 seconds
    windowed_avg = parsed_df \
        .withWatermark("processing_time", "10 seconds") \
        .groupBy(
            window(col("processing_time"), "5 seconds"),
            col("car"),
            col("track")
        ) \
        .agg(
            avg("rpms").alias("avg_rpms"),
            avg("speedKmh").alias("avg_speed")
        )

    # 4. Alert Logic
    # Threshold: 7500 RPM (typical for GT3 redline)
    alerts = windowed_avg.filter(col("avg_rpms") > 7500) \
        .select(
            col("window.start").alias("start_time"),
            col("window.end").alias("end_time"),
            col("car"),
            col("avg_rpms"),
            col("avg_speed")
        )

    # 5. Output to Console (Sink)
    print("🚀 Alerts Sink starting... Monitoring for OVERREV (RPM > 7500)...")
    
    query = alerts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
