import os
from pathlib import Path

import pyspark
from dotenv import load_dotenv
from pyspark.sql import SparkSession


def load_project_env() -> None:
    airflow_env = Path("/opt/airflow/.env")

    if airflow_env.exists():
        load_dotenv(airflow_env, override=True)
    else:
        load_dotenv(override=True)


def get_required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise ValueError(f"Missing required environment variable: {name}")

    return value


def get_s3_path() -> str:
    load_project_env()

    path = get_required_env("S3_LAKE_PATH")

    if path.startswith("s3a://"):
        normalized = path if path.endswith("/") else path + "/"
        return normalized

    if path.startswith("file://"):
        local_path = Path(path[7:])
    else:
        local_path = Path(path)

    if not local_path.is_absolute():
        local_path = Path.cwd() / local_path

    local_path = local_path.expanduser().resolve()
    normalized = local_path.as_posix()

    if not normalized.endswith("/"):
        normalized += "/"

    return normalized


def get_spark_session(app_name: str) -> SparkSession:
    load_project_env()

    env = os.getenv("APP_ENV", "dev")
    aws_region = os.getenv("AWS_REGION", "eu-north-1")
    spark_master = os.getenv("SPARK_MASTER", "local[*]")
    spark_deploy_mode = os.getenv("SPARK_DEPLOY_MODE", "")
    s3_path = os.getenv("S3_LAKE_PATH", "")
    use_s3 = s3_path.startswith("s3a://")

    spark_version = pyspark.__version__
    kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{spark_version}"
    hadoop_aws_pkg = "org.apache.hadoop:hadoop-aws:3.4.2"
    aws_sdk_pkg = "software.amazon.awssdk:bundle:2.29.52"
    packages = ",".join([kafka_pkg, hadoop_aws_pkg, aws_sdk_pkg])

    print(
        f"Initializing Spark Engine: {app_name} [{env.upper()}] "
        f"master={spark_master} deploy_mode={spark_deploy_mode or 'default'}"
    )

    spark_builder = (
        SparkSession.builder
        .appName(f"{app_name}-{env}")
        .master(spark_master)
        .config("spark.jars.packages", packages)
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )

    if spark_deploy_mode:
        spark_builder = spark_builder.config("spark.submit.deployMode", spark_deploy_mode)

    if use_s3:
        aws_access_key = get_required_env("AWS_ACCESS_KEY_ID")
        aws_secret_key = get_required_env("AWS_SECRET_ACCESS_KEY")

        print(f"Initializing Spark Engine for S3: {app_name} [{env.upper()}]")
        spark_builder = spark_builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        spark_builder = spark_builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        spark_builder = spark_builder.config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        spark_builder = spark_builder.config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        spark_builder = spark_builder.config("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com")
        spark_builder = spark_builder.config("spark.hadoop.fs.s3a.endpoint.region", aws_region)
    else:
        print(f"Initializing Spark Engine for local storage: {app_name} [{env.upper()}]")

    spark = spark_builder.getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    return spark