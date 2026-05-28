import os
import sys

import pyspark.sql.functions as F
from pyspark.sql.window import Window

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.spark_setup import get_spark_session, get_s3_path


VALID_VARIANTS = ["control", "treatment"]

VALID_BEHAVIOR_EVENTS = [
    "page_view",
    "product_view",
    "add_to_cart",
    "checkout_started",
    "payment_failed",
    "purchase",
    "refund",
]


def write_parquet(df, path: str, partition_cols=None, mode: str = "overwrite") -> None:
    writer = df.write.mode(mode)

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(path)


def deduplicate_by_event_id(df):
    window = Window.partitionBy("event_id").orderBy(
        F.col("kafka_ingestion_timestamp").asc()
    )

    return (
        df.withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )


def process_exposures(spark, s3_base_path: str) -> None:
    bronze_path = f"{s3_base_path}bronze/exposures/"
    silver_path = f"{s3_base_path}silver/exposures/"
    quarantine_path = f"{s3_base_path}quarantine/exposures/"

    print(f"Reading bronze exposures from {bronze_path}")
    exp_df = spark.read.parquet(bronze_path)

    enriched_df = (
        exp_df
        .withColumn("event_timestamp", F.to_timestamp("event_time"))
        .withColumn("processing_timestamp", F.current_timestamp())
    )

    if "is_bot" in enriched_df.columns:
        enriched_df = enriched_df.withColumn(
            "is_bot",
            F.coalesce(F.col("is_bot").cast("boolean"), F.lit(False)),
        )
    else:
        enriched_df = enriched_df.withColumn("is_bot", F.lit(False))

    invalid_df = (
        enriched_df
        .filter(
            F.col("event_id").isNull()
            | F.col("user_id").isNull()
            | F.col("experiment_id").isNull()
            | F.col("assigned_variant").isNull()
            | ~F.col("assigned_variant").isin(VALID_VARIANTS)
            | F.col("event_timestamp").isNull()
        )
        .withColumn("quarantine_reason", F.lit("invalid_exposure_record"))
    )

    valid_df = (
        enriched_df
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("experiment_id").isNotNull())
        .filter(F.col("assigned_variant").isin(VALID_VARIANTS))
        .filter(F.col("event_timestamp").isNotNull())
    )

    deduped_df = deduplicate_by_event_id(valid_df)

    users_in_multiple_variants_df = (
        deduped_df
        .groupBy("experiment_id", "user_id")
        .agg(F.countDistinct("assigned_variant").alias("variant_count"))
        .filter(F.col("variant_count") > 1)
    )

    conflicted_exposures_df = (
        deduped_df.alias("e")
        .join(
            users_in_multiple_variants_df.alias("d"),
            on=["experiment_id", "user_id"],
            how="inner",
        )
        .drop("variant_count")
        .withColumn(
            "quarantine_reason",
            F.lit("user_assigned_to_multiple_variants"),
        )
    )

    clean_df = (
        deduped_df.alias("e")
        .join(
            users_in_multiple_variants_df.alias("d"),
            on=["experiment_id", "user_id"],
            how="left_anti",
        )
    )

    quarantine_df = invalid_df.unionByName(
        conflicted_exposures_df,
        allowMissingColumns=True,
    )

    clean_df = clean_df.cache()
    quarantine_df = quarantine_df.cache()

    print("Writing Silver exposures...")
    write_parquet(clean_df, silver_path, ["year", "month", "day"])

    print("Writing quarantined exposures...")
    write_parquet(quarantine_df, quarantine_path, ["year", "month", "day"])

    print(f"Silver exposures count: {clean_df.count()}")
    print(f"Quarantined exposures count: {quarantine_df.count()}")


def process_behavior(spark, s3_base_path: str) -> None:
    bronze_path = f"{s3_base_path}bronze/behavior/"
    silver_path = f"{s3_base_path}silver/behavior/"
    quarantine_path = f"{s3_base_path}quarantine/behavior/"

    print(f"Reading bronze behavior from {bronze_path}")
    beh_df = spark.read.parquet(bronze_path)

    flattened_df = (
        beh_df
        .withColumn("event_timestamp", F.to_timestamp("event_time"))
        .withColumn("processing_timestamp", F.current_timestamp())
    )

    if "properties" in flattened_df.columns:
        flattened_df = (
            flattened_df
            .withColumn("page_url", F.col("properties.page").cast("string"))
            .withColumn(
                "is_bot",
                F.coalesce(F.col("properties.is_bot").cast("boolean"), F.lit(False)),
            )
            .withColumn("revenue_gbp", F.col("properties.revenue_gbp").cast("double"))
            .withColumn("refund_gbp", F.lit(None).cast("double"))
            .withColumn(
                "failure_reason",
                F.col("properties.failure_reason").cast("string"),
            )
            .drop("properties")
        )
    else:
        flattened_df = (
            flattened_df
            .withColumn("page_url", F.lit(None).cast("string"))
            .withColumn("is_bot", F.lit(False))
            .withColumn("revenue_gbp", F.lit(None).cast("double"))
            .withColumn("refund_gbp", F.lit(None).cast("double"))
            .withColumn("failure_reason", F.lit(None).cast("string"))
        )

    invalid_df = (
        flattened_df
        .filter(
            F.col("event_id").isNull()
            | F.col("user_id").isNull()
            | F.col("experiment_id").isNull()
            | F.col("event_name").isNull()
            | ~F.col("event_name").isin(VALID_BEHAVIOR_EVENTS)
            | F.col("event_timestamp").isNull()
            | (
                (F.col("event_name") == "purchase")
                & (
                    F.col("revenue_gbp").isNull()
                    | (F.col("revenue_gbp") < 0)
                )
            )
            | (
                (F.col("event_name") == "refund")
                & (
                    F.col("refund_gbp").isNull()
                    | (F.col("refund_gbp") < 0)
                )
            )
        )
        .withColumn("quarantine_reason", F.lit("invalid_behavior_record"))
    )

    valid_df = (
        flattened_df
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("experiment_id").isNotNull())
        .filter(F.col("event_name").isin(VALID_BEHAVIOR_EVENTS))
        .filter(F.col("event_timestamp").isNotNull())
        .filter(F.col("is_bot") == F.lit(False))
        .filter(
            (F.col("event_name") != "purchase")
            | (
                F.col("revenue_gbp").isNotNull()
                & (F.col("revenue_gbp") >= 0)
            )
        )
        .filter(
            (F.col("event_name") != "refund")
            | (
                F.col("refund_gbp").isNotNull()
                & (F.col("refund_gbp") >= 0)
            )
        )
    )

    clean_df = deduplicate_by_event_id(valid_df)

    clean_df = clean_df.cache()
    invalid_df = invalid_df.cache()

    print("Writing Silver behavior...")
    write_parquet(clean_df, silver_path, ["year", "month", "day"])

    print("Writing quarantined behavior...")
    write_parquet(invalid_df, quarantine_path, ["year", "month", "day"])

    print(f"Silver behavior count: {clean_df.count()}")
    print(f"Quarantined behavior count: {invalid_df.count()}")


def process_silver_layer() -> None:
    spark = get_spark_session("Bronze-To-Silver")
    s3_base_path = get_s3_path()

    try:
        process_exposures(spark, s3_base_path)
        process_behavior(spark, s3_base_path)
    except Exception as exc:
        print(f"Silver processing failed: {exc}")
        raise
    finally:
        spark.stop()

    print("Silver layer processing complete.")


if __name__ == "__main__":
    process_silver_layer()