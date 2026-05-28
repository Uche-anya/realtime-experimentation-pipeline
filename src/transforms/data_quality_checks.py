import os
import sys

import pyspark.sql.functions as F

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


def make_check_result(
    spark,
    check_name: str,
    layer: str,
    table_name: str,
    status: str,
    observed_value: float,
    threshold: str,
    description: str,
):
    return (
        spark.createDataFrame(
            [
                (
                    check_name,
                    layer,
                    table_name,
                    status,
                    float(observed_value),
                    threshold,
                    description,
                )
            ],
            [
                "check_name",
                "layer",
                "table_name",
                "status",
                "observed_value",
                "threshold",
                "description",
            ],
        )
        .withColumn("checked_at", F.current_timestamp())
    )


def union_results(results):
    final_df = results[0]

    for result_df in results[1:]:
        final_df = final_df.unionByName(result_df)

    return final_df


def run_quality_checks() -> None:
    spark = get_spark_session("Data-Quality-Checks")
    s3_base_path = get_s3_path()

    print("Reading Silver tables...")

    exposures_df = spark.read.parquet(f"{s3_base_path}silver/exposures/")
    behavior_df = spark.read.parquet(f"{s3_base_path}silver/behavior/")

    results = []

    exposure_count = exposures_df.count()
    behavior_count = behavior_df.count()

    results.append(
        make_check_result(
            spark,
            "silver_exposures_has_rows",
            "silver",
            "exposures",
            "PASS" if exposure_count > 0 else "FAIL",
            exposure_count,
            "> 0",
            "Silver exposures should contain at least one record.",
        )
    )

    results.append(
        make_check_result(
            spark,
            "silver_behavior_has_rows",
            "silver",
            "behavior",
            "PASS" if behavior_count > 0 else "FAIL",
            behavior_count,
            "> 0",
            "Silver behavior should contain at least one record.",
        )
    )

    null_exposure_user_ids = exposures_df.filter(F.col("user_id").isNull()).count()

    results.append(
        make_check_result(
            spark,
            "exposures_user_id_not_null",
            "silver",
            "exposures",
            "PASS" if null_exposure_user_ids == 0 else "FAIL",
            null_exposure_user_ids,
            "0",
            "Every exposure record must have a user_id.",
        )
    )

    null_exposure_event_ids = exposures_df.filter(F.col("event_id").isNull()).count()

    results.append(
        make_check_result(
            spark,
            "exposures_event_id_not_null",
            "silver",
            "exposures",
            "PASS" if null_exposure_event_ids == 0 else "FAIL",
            null_exposure_event_ids,
            "0",
            "Every exposure record must have an event_id.",
        )
    )

    invalid_variant_count = exposures_df.filter(
        ~F.col("assigned_variant").isin(VALID_VARIANTS)
    ).count()

    results.append(
        make_check_result(
            spark,
            "exposures_valid_variant",
            "silver",
            "exposures",
            "PASS" if invalid_variant_count == 0 else "FAIL",
            invalid_variant_count,
            "0",
            "assigned_variant must be control or treatment.",
        )
    )

    duplicate_exposure_event_ids = (
        exposures_df
        .groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    results.append(
        make_check_result(
            spark,
            "exposures_event_id_unique",
            "silver",
            "exposures",
            "PASS" if duplicate_exposure_event_ids == 0 else "FAIL",
            duplicate_exposure_event_ids,
            "0",
            "Exposure event_id should be unique after Silver deduplication.",
        )
    )

    users_in_multiple_variants = (
        exposures_df
        .groupBy("experiment_id", "user_id")
        .agg(F.countDistinct("assigned_variant").alias("variant_count"))
        .filter(F.col("variant_count") > 1)
        .count()
    )

    results.append(
        make_check_result(
            spark,
            "user_assigned_to_single_variant",
            "silver",
            "exposures",
            "PASS" if users_in_multiple_variants == 0 else "FAIL",
            users_in_multiple_variants,
            "0",
            "A user should not be assigned to multiple variants in one experiment.",
        )
    )

    null_behavior_user_ids = behavior_df.filter(F.col("user_id").isNull()).count()

    results.append(
        make_check_result(
            spark,
            "behavior_user_id_not_null",
            "silver",
            "behavior",
            "PASS" if null_behavior_user_ids == 0 else "FAIL",
            null_behavior_user_ids,
            "0",
            "Every behavior record must have a user_id.",
        )
    )

    null_behavior_event_ids = behavior_df.filter(F.col("event_id").isNull()).count()

    results.append(
        make_check_result(
            spark,
            "behavior_event_id_not_null",
            "silver",
            "behavior",
            "PASS" if null_behavior_event_ids == 0 else "FAIL",
            null_behavior_event_ids,
            "0",
            "Every behavior record must have an event_id.",
        )
    )

    invalid_event_name_count = behavior_df.filter(
        ~F.col("event_name").isin(VALID_BEHAVIOR_EVENTS)
    ).count()

    results.append(
        make_check_result(
            spark,
            "behavior_valid_event_name",
            "silver",
            "behavior",
            "PASS" if invalid_event_name_count == 0 else "FAIL",
            invalid_event_name_count,
            "0",
            "event_name must be one of the approved funnel events.",
        )
    )

    duplicate_behavior_event_ids = (
        behavior_df
        .groupBy("event_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    results.append(
        make_check_result(
            spark,
            "behavior_event_id_unique",
            "silver",
            "behavior",
            "PASS" if duplicate_behavior_event_ids == 0 else "FAIL",
            duplicate_behavior_event_ids,
            "0",
            "Behavior event_id should be unique after Silver deduplication.",
        )
    )

    invalid_purchase_revenue = behavior_df.filter(
        (F.col("event_name") == "purchase")
        & (
            F.col("revenue_gbp").isNull()
            | (F.col("revenue_gbp") < 0)
        )
    ).count()

    results.append(
        make_check_result(
            spark,
            "purchase_revenue_valid",
            "silver",
            "behavior",
            "PASS" if invalid_purchase_revenue == 0 else "FAIL",
            invalid_purchase_revenue,
            "0",
            "Purchase events must have non-negative revenue_gbp.",
        )
    )

    invalid_refund_amount = behavior_df.filter(
        (F.col("event_name") == "refund")
        & (
            F.col("refund_gbp").isNull()
            | (F.col("refund_gbp") < 0)
        )
    ).count()

    results.append(
        make_check_result(
            spark,
            "refund_amount_valid",
            "silver",
            "behavior",
            "PASS" if invalid_refund_amount == 0 else "FAIL",
            invalid_refund_amount,
            "0",
            "Refund events must have non-negative refund_gbp.",
        )
    )

    behavior_without_exposure = (
        behavior_df.alias("b")
        .join(
            exposures_df
            .select("experiment_id", "user_id")
            .dropDuplicates()
            .alias("e"),
            on=["experiment_id", "user_id"],
            how="left_anti",
        )
        .count()
    )

    results.append(
        make_check_result(
            spark,
            "behavior_has_matching_exposure",
            "silver",
            "behavior",
            "WARN" if behavior_without_exposure > 0 else "PASS",
            behavior_without_exposure,
            "0 preferred",
            "Behavior events should normally have a matching exposure.",
        )
    )

    future_exposure_events = exposures_df.filter(
        F.col("event_timestamp") > F.current_timestamp()
    ).count()

    results.append(
        make_check_result(
            spark,
            "exposure_event_time_not_in_future",
            "silver",
            "exposures",
            "PASS" if future_exposure_events == 0 else "FAIL",
            future_exposure_events,
            "0",
            "Exposure timestamps should not be in the future.",
        )
    )

    future_behavior_events = behavior_df.filter(
        F.col("event_timestamp") > F.current_timestamp()
    ).count()

    results.append(
        make_check_result(
            spark,
            "behavior_event_time_not_in_future",
            "silver",
            "behavior",
            "PASS" if future_behavior_events == 0 else "FAIL",
            future_behavior_events,
            "0",
            "Behavior timestamps should not be in the future.",
        )
    )

    final_results = union_results(results)

    final_results.show(truncate=False)

    output_path = f"{s3_base_path}gold/data_quality_results/"

    print(f"Writing data quality results to {output_path}")

    final_results.write.mode("overwrite").parquet(output_path)

    spark.stop()
    print("Data quality checks complete.")


if __name__ == "__main__":
    run_quality_checks()