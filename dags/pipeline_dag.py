from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "data_platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="ab_test_medallion_pipeline",
    default_args=default_args,
    description="Bronze to Silver to Gold A/B testing analytics pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    tags=["experimentation", "spark", "medallion"],
) as dag:

    process_silver = BashOperator(
        task_id="process_bronze_to_silver",
        bash_command="python /opt/airflow/src/transforms/silver_transform.py",
    )

    run_quality_checks = BashOperator(
        task_id="run_data_quality_checks",
        bash_command="python /opt/airflow/src/transforms/data_quality_checks.py",
    )

    calculate_gold_metrics = BashOperator(
        task_id="calculate_gold_experiment_metrics",
        bash_command="python /opt/airflow/src/transforms/gold_metrics.py",
    )

    process_silver >> run_quality_checks >> calculate_gold_metrics