# Real-Time Experimentation Pipeline

This repository implements a local Airflow-driven medallion pipeline for experimentation analytics.

## What this project does

The pipeline is designed to:
- ingest event streams into a Bronze layer (Kafka / S3)
- transform Bronze data into a Silver layer with validation and deduplication
- run Silver data quality checks
- calculate Gold-level experiment metrics from the Silver layer

The main DAG is defined in `dags/pipeline_dag.py` and runs three tasks:
- `process_bronze_to_silver`
- `run_data_quality_checks`
- `calculate_gold_experiment_metrics`

## Architecture

- `docker-compose.yml` launches:
  - Kafka broker
  - Postgres metadata database for Airflow
  - Airflow init container
  - Airflow webserver
  - Airflow scheduler
- `dockerfile` builds a custom Airflow image with Python dependencies.
- `src/transforms/` contains Spark scripts for Silver and Gold layers.
- `src/utils/spark_setup.py` loads environment variables and configures Spark for S3.

## Recommended project improvements

1. Keep secrets out of version control
   - remove AWS credentials from `.env`
   - use `.env.sample` for required variables
   - add `.env` to `.gitignore`

2. Add a clear local dev mode
   - support local filesystem or MinIO for development
   - make `S3_LAKE_PATH` configurable without requiring AWS credentials for local testing

3. Add a `Makefile` or dev script
   - simplify build/run commands such as `make up`, `make stop`, `make test`

4. Add tests for transform logic
   - unit test the Silver, quality-check, and Gold metrics code
   - use small Spark DataFrame fixtures or local pandas alternatives

5. Add documentation for data layout
   - describe `bronze/`, `silver/`, and `quarantine/` folder structure
   - explain expected schema for `exposures` and `behavior`

6. Add a Bronze ingestion path
   - the current DAG starts at Silver processing only
   - add an ingestion task for streaming or batch Bronze writes if needed

## Local setup

### Prerequisites

- Docker
- Docker Compose
- Python 3.12 (for local development if you need to run scripts outside Docker)

### Environment

Create a local `.env` file with the required values.
Do not commit this file.

Example:

```env
APP_ENV=dev
KAFKA_BROKER=localhost:9092
# Use a local folder for development or an S3 path for AWS.
# Local example (inside the Airflow container):
S3_LAKE_PATH=/opt/airflow/data/raw/
# AWS example:
# S3_LAKE_PATH=s3a://telemetry-lake-dev/raw/
AWS_REGION=eu-north-1
AWS_ACCESS_KEY_ID=YOUR_KEY_ID
AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
```

### Local development with file storage

If you want to run the pipeline without AWS, use a local folder path such as:

```env
S3_LAKE_PATH=/opt/airflow/data/raw/
```

The container mounts `./data` to `/opt/airflow/data`, so local data paths are available inside Airflow tasks.

### EMR / Cluster mode

To run Spark jobs on a remote cluster, configure the Spark runtime and S3 storage options:

```env
S3_LAKE_PATH=s3a://telemetry-lake-dev/raw/
SPARK_MASTER=yarn
SPARK_DEPLOY_MODE=cluster
AWS_REGION=eu-north-1
AWS_ACCESS_KEY_ID=YOUR_KEY_ID
AWS_SECRET_ACCESS_KEY=YOUR_SECRET_KEY
```

For EMR or YARN cluster execution, make sure the Airflow container can reach the cluster and has the correct Hadoop/Spark configuration.

### Build and run

From the repo root:

```bash
docker compose up --build
```

This starts Airflow and supporting services.

### Access Airflow

Open:

```text
http://localhost:8080
```

Login with the default Airflow admin user created by the init container.

### Run the DAG

In the Airflow UI, enable and trigger `ab_test_medallion_pipeline`.

## Notes specific to this repo

- `requirements.txt` installs `pyspark`, `confluent-kafka`, `pandas`, `pyarrow`, `boto3`.
- The Airflow DAG uses `BashOperator` to run local Python scripts.
- `spark_setup.py` now supports local filesystem mode and remote Spark cluster mode via `SPARK_MASTER`.
- `producer.py` now supports idempotent Kafka delivery and optional SASL authentication via environment variables.

## Next step suggestions

If you want to make this project stronger, I recommend:
1. creating `.env.sample`
2. adding a `Makefile` for common commands
3. implementing local path fallback for `S3_LAKE_PATH`
4. adding tests for the transform pipeline
5. adding a Bronze ingestion task so the DAG covers the entire pipeline end-to-end
