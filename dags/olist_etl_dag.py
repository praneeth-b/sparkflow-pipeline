from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Resilient baseline arguments for expert-level pipelines
default_args = {
    'owner': 'data_engineering',
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=15)
}

def check_silver_quality(**kwargs):
    """
    Simulated Data Quality Check.
    In a real scenario, this would query the Silver tables to ensure no Nulls
    exist in critical columns before allowing Gold aggregations.
    """
    quality_passed = True
    if quality_passed:
        return 'create_gold_metrics'
    return 'quarantine_alert'

with DAG(
    'olist_medallion_pipeline',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['portfolio', 'pyspark', 'medallion']
) as dag:

    # 1. BRONZE: Ingest CSVs with strict schemas
    ingest_to_bronze = SparkSubmitOperator(
        task_id='ingest_to_bronze',
        application='/opt/airflow/src/bronze_ingestion.py',
        conn_id='spark_default',
        conf={"spark.submit.deployMode": "client"}
    )

    # 2. SILVER: Clean, deduplicate, format dates
    transform_to_silver = SparkSubmitOperator(
        task_id='transform_to_silver',
        application='/opt/airflow/src/silver_transformations.py',
        conn_id='spark_default',
        conf={"spark.submit.deployMode": "client"}
    )

    # 3. BRANCHING: Data Quality Gate
    quality_check = BranchPythonOperator(
        task_id='quality_check',
        python_callable=check_silver_quality
    )

    # 4a. GOLD: Heavy aggregations and broadcast joins (Success Path)
    create_gold_metrics = SparkSubmitOperator(
        task_id='create_gold_metrics',
        application='/opt/airflow/src/gold_aggregations.py',
        conn_id='spark_default',
        conf={"spark.submit.deployMode": "client"}
    )

    # 4b. QUARANTINE: Alerting (Failure Path)
    quarantine_alert = EmptyOperator(
        task_id='quarantine_alert'
    )

    # 5. END STATE
    pipeline_complete = EmptyOperator(
        task_id='pipeline_complete',
        trigger_rule='none_failed_min_one_success'
    )

    # Define DAG Dependencies
    ingest_to_bronze >> transform_to_silver >> quality_check
    quality_check >> create_gold_metrics >> pipeline_complete
    quality_check >> quarantine_alert >> pipeline_complete