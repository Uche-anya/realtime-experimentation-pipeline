import sys
import os
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DoubleType

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.spark_setup import get_spark_session, get_s3_path

def start_behavior_stream():
    spark = get_spark_session("Bronze-Behaviors")
    s3_base_path = get_s3_path()
    kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")

    properties_schema = StructType([
        StructField("page", StringType(), True),
        StructField("is_bot", BooleanType(), True),
        StructField("revenue_gbp", DoubleType(), True),
        StructField("failure_reason", StringType(), True),
    ])

    schema = StructType([
        StructField("event_type", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("experiment_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_name", StringType(), True),
        StructField("device", StringType(), True),
        StructField("country", StringType(), True),
        StructField("traffic_source", StringType(), True),
        StructField("app_version", StringType(), True),
        StructField("event_time", StringType(), True),
        StructField("properties", properties_schema, True),
    ])

    raw_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_broker) \
        .option("subscribe", "user_events") \
        .option("startingOffsets", "latest").load()

    parsed_df = raw_df.selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS kafka_value",
        "timestamp AS kafka_ingestion_timestamp",
        "topic", "partition", "offset"
    ).select(
        F.from_json(F.col("kafka_value"), schema).alias("data"),
        "kafka_key", "kafka_value", "kafka_ingestion_timestamp", "topic", "partition", "offset"
    ).select("data.*", "kafka_key", "kafka_value", "kafka_ingestion_timestamp", "topic", "partition", "offset")

    final_df = parsed_df.withColumn("event_timestamp", F.to_timestamp("event_time")) \
        .withColumn("year", F.year("event_timestamp")) \
        .withColumn("month", F.month("event_timestamp")) \
        .withColumn("day", F.dayofmonth("event_timestamp"))

    print("Streaming behaviors to Bronze...")
    query = final_df.writeStream.format("parquet") \
        .option("path", f"{s3_base_path}bronze/behavior/") \
        .option("checkpointLocation", f"{s3_base_path}checkpoints/bronze/behavior/") \
        .partitionBy("year", "month", "day") \
        .outputMode("append").trigger(processingTime="10 seconds").start()
    
    query.awaitTermination()

if __name__ == "__main__":
    start_behavior_stream()