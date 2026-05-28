import os
import sys

import pyspark.sql.functions as F

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.spark_setup import get_spark_session, get_s3_path


FUNNEL_ORDER = {
    "page_view": 1,
    "product_view": 2,
    "add_to_cart": 3,
    "checkout_started": 4,
    "payment_failed": 5,
    "purchase": 6,
    "refund": 7,
}


def write_gold(df, path: str) -> None:
    df.write.mode("overwrite").parquet(path)


def build_experiment_metrics(exposures_df, behavior_df):
    purchases_df = (
        behavior_df
        .filter(F.col("event_name") == "purchase")
        .groupBy("experiment_id", "user_id")
        .agg(
            F.countDistinct("event_id").alias("purchase_count"),
            F.round(F.sum("revenue_gbp"), 2).alias("purchase_revenue_gbp"),
            F.min("event_timestamp").alias("first_purchase_timestamp"),
        )
    )

    refunds_df = (
        behavior_df
        .filter(F.col("event_name") == "refund")
        .groupBy("experiment_id", "user_id")
        .agg(
            F.countDistinct("event_id").alias("refund_count"),
            F.round(F.sum("refund_gbp"), 2).alias("refund_amount_gbp"),
        )
    )

    joined_df = (
        exposures_df.alias("e")
        .join(purchases_df.alias("p"), on=["experiment_id", "user_id"], how="left")
        .join(refunds_df.alias("r"), on=["experiment_id", "user_id"], how="left")
        .withColumn(
            "converted",
            F.when(F.col("purchase_count").isNotNull(), 1).otherwise(0),
        )
        .withColumn(
            "purchase_count",
            F.coalesce(F.col("purchase_count"), F.lit(0)),
        )
        .withColumn(
            "refund_count",
            F.coalesce(F.col("refund_count"), F.lit(0)),
        )
        .withColumn(
            "purchase_revenue_gbp",
            F.coalesce(F.col("purchase_revenue_gbp"), F.lit(0.0)),
        )
        .withColumn(
            "refund_amount_gbp",
            F.coalesce(F.col("refund_amount_gbp"), F.lit(0.0)),
        )
        .withColumn(
            "net_revenue_gbp",
            F.col("purchase_revenue_gbp") - F.col("refund_amount_gbp"),
        )
    )

    metrics_df = (
        joined_df
        .groupBy("experiment_id", "assigned_variant")
        .agg(
            F.countDistinct("user_id").alias("exposed_users"),
            F.sum("converted").alias("converted_users"),
            F.sum("purchase_count").alias("total_purchases"),
            F.sum("refund_count").alias("total_refunds"),
            F.round(F.sum("purchase_revenue_gbp"), 2).alias("gross_revenue_gbp"),
            F.round(F.sum("refund_amount_gbp"), 2).alias("refunds_gbp"),
            F.round(F.sum("net_revenue_gbp"), 2).alias("net_revenue_gbp"),
        )
        .fillna({
            "converted_users": 0,
            "total_purchases": 0,
            "total_refunds": 0,
            "gross_revenue_gbp": 0.0,
            "refunds_gbp": 0.0,
            "net_revenue_gbp": 0.0,
        })
        .withColumn(
            "conversion_rate_pct",
            F.round((F.col("converted_users") / F.col("exposed_users")) * 100, 2),
        )
        .withColumn(
            "revenue_per_exposed_user_gbp",
            F.round(F.col("net_revenue_gbp") / F.col("exposed_users"), 2),
        )
        .withColumn(
            "avg_order_value_gbp",
            F.when(
                F.col("converted_users") > 0,
                F.round(F.col("gross_revenue_gbp") / F.col("converted_users"), 2),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("metric_generated_at", F.current_timestamp())
        .orderBy("experiment_id", "assigned_variant")
    )

    return metrics_df


def build_funnel_metrics(exposures_df, behavior_df):
    exposed_users_df = (
        exposures_df
        .select("experiment_id", "user_id", "assigned_variant")
        .dropDuplicates(["experiment_id", "user_id"])
    )

    user_event_stage_df = (
        behavior_df
        .select("experiment_id", "user_id", "event_name")
        .dropDuplicates(["experiment_id", "user_id", "event_name"])
    )

    funnel_df = (
        exposed_users_df
        .join(user_event_stage_df, on=["experiment_id", "user_id"], how="left")
        .groupBy("experiment_id", "assigned_variant", "event_name")
        .agg(F.countDistinct("user_id").alias("users_reached_stage"))
        .filter(F.col("event_name").isNotNull())
    )

    exposure_counts_df = (
        exposed_users_df
        .groupBy("experiment_id", "assigned_variant")
        .agg(F.countDistinct("user_id").alias("exposed_users"))
    )

    funnel_with_rates_df = (
        funnel_df
        .join(exposure_counts_df, on=["experiment_id", "assigned_variant"], how="left")
        .withColumn(
            "stage_rate_pct",
            F.round((F.col("users_reached_stage") / F.col("exposed_users")) * 100, 2),
        )
        .withColumn(
            "funnel_order",
            F.create_map(
                [F.lit(x) for pair in FUNNEL_ORDER.items() for x in pair]
            )[F.col("event_name")]
        )
        .withColumn("metric_generated_at", F.current_timestamp())
        .orderBy("experiment_id", "assigned_variant", "funnel_order")
    )

    return funnel_with_rates_df


def build_segment_metrics(exposures_df, behavior_df):
    purchases_df = (
        behavior_df
        .filter(F.col("event_name") == "purchase")
        .groupBy("experiment_id", "user_id")
        .agg(
            F.countDistinct("event_id").alias("purchase_count"),
            F.round(F.sum("revenue_gbp"), 2).alias("revenue_gbp"),
        )
    )

    joined_df = (
        exposures_df
        .join(purchases_df, on=["experiment_id", "user_id"], how="left")
        .withColumn(
            "converted",
            F.when(F.col("purchase_count").isNotNull(), 1).otherwise(0),
        )
        .withColumn("revenue_gbp", F.coalesce(F.col("revenue_gbp"), F.lit(0.0)))
    )

    segment_cols = [
        "experiment_id",
        "assigned_variant",
        "country",
        "traffic_source",
        "device",
    ]

    return (
        joined_df
        .groupBy(*segment_cols)
        .agg(
            F.countDistinct("user_id").alias("exposed_users"),
            F.sum("converted").alias("converted_users"),
            F.round(F.sum("revenue_gbp"), 2).alias("revenue_gbp"),
        )
        .withColumn(
            "conversion_rate_pct",
            F.round((F.col("converted_users") / F.col("exposed_users")) * 100, 2),
        )
        .withColumn("metric_generated_at", F.current_timestamp())
        .orderBy("experiment_id", "assigned_variant", "country", "traffic_source", "device")
    )


def build_data_health_summary(exposures_df, behavior_df):
    exposure_count = exposures_df.count()
    behavior_count = behavior_df.count()

    duplicate_exposure_events = (
        exposures_df.groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    duplicate_behavior_events = (
        behavior_df.groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    behavior_without_exposure = (
        behavior_df.alias("b")
        .join(
            exposures_df.select("experiment_id", "user_id").dropDuplicates().alias("e"),
            on=["experiment_id", "user_id"],
            how="left_anti",
        )
        .count()
    )

    rows = [
        ("silver_exposure_rows", exposure_count),
        ("silver_behavior_rows", behavior_count),
        ("duplicate_exposure_event_ids", duplicate_exposure_events),
        ("duplicate_behavior_event_ids", duplicate_behavior_events),
        ("behavior_without_matching_exposure", behavior_without_exposure),
    ]

    return (
        exposures_df.sparkSession
        .createDataFrame(rows, ["metric_name", "metric_value"])
        .withColumn("metric_generated_at", F.current_timestamp())
    )


def calculate_gold_metrics() -> None:
    spark = get_spark_session("Silver-To-Gold")
    s3_base_path = get_s3_path()

    print("Reading Silver tables...")
    exposures_df = spark.read.parquet(f"{s3_base_path}silver/exposures/")
    behavior_df = spark.read.parquet(f"{s3_base_path}silver/behavior/")

    print("Building experiment metrics...")
    experiment_metrics_df = build_experiment_metrics(exposures_df, behavior_df)

    print("Building funnel metrics...")
    funnel_metrics_df = build_funnel_metrics(exposures_df, behavior_df)

    print("Building segment metrics...")
    segment_metrics_df = build_segment_metrics(exposures_df, behavior_df)

    print("Building data health summary...")
    health_summary_df = build_data_health_summary(exposures_df, behavior_df)

    experiment_metrics_df.show(truncate=False)
    funnel_metrics_df.show(truncate=False)

    print("Writing Gold outputs...")
    write_gold(experiment_metrics_df, f"{s3_base_path}gold/experiment_metrics/")
    write_gold(funnel_metrics_df, f"{s3_base_path}gold/funnel_metrics/")
    write_gold(segment_metrics_df, f"{s3_base_path}gold/segment_metrics/")
    write_gold(health_summary_df, f"{s3_base_path}gold/data_health_summary/")

    spark.stop()
    print("Gold metrics complete.")


if __name__ == "__main__":
    calculate_gold_metrics()