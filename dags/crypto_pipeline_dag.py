from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "Laila",
    "depends_on_past": False,
    "start_date": datetime(2026, 6, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}


with DAG(
    dag_id="crypto_pipeline_dag",
    default_args=default_args,
    description="Pipeline Crypto Bronze Silver Gold Snowflake",
    schedule="@daily",
    catchup=False,
    tags=["crypto", "etl", "minio", "snowflake"]
) as dag:


    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command="""
        cd /opt/airflow/scripts &&
        python ingest_bronze.py
        """
    )


    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command="""
        cd /opt/airflow/scripts &&
        python transform_silver.py
        """
    )


    build_gold_model = BashOperator(
        task_id="build_gold_model",
        bash_command="""
        cd /opt/airflow/scripts &&
        python build_gold.py
        """
    )


    load_snowflake = BashOperator(
        task_id="load_snowflake",
        bash_command="""
        cd /opt/airflow/scripts &&
        python load_snowflake.py
        """
    )


    ingest_bronze >> transform_silver >> build_gold_model >> load_snowflake