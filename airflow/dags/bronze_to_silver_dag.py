from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

# Default arguments
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    'bronze_to_silver_processing',
    default_args=default_args,
    description='Process Bronze layer to Silver layer with data quality and transformations',
    schedule_interval='0 */2 * * *',  # Run every 2 hours
    start_date=days_ago(1),
    catchup=False,
    tags=['olist', 'silver', 'data-quality'],
)

def log_start():
    logging.info("=" * 80)
    logging.info("Starting Bronze to Silver Layer Processing Pipeline")
    logging.info("=" * 80)

def log_completion():
    logging.info("=" * 80)
    logging.info("✓ Bronze to Silver Pipeline Completed Successfully!")
    logging.info("=" * 80)

# Task 1: Log start
start_task = PythonOperator(
    task_id='log_pipeline_start',
    python_callable=log_start,
    dag=dag,
)

# Task 2: Submit Spark job to process Bronze to Silver
process_bronze_to_silver = BashOperator(
    task_id='process_bronze_to_silver',
    bash_command='''
    docker exec spark-master /opt/spark/bin/spark-submit \
      --master local[*] \
      --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
      /opt/spark/app/process_bronze_to_silver.py
    ''',
    dag=dag,
)

# Task 3: Log completion
complete_task = PythonOperator(
    task_id='log_pipeline_completion',
    python_callable=log_completion,
    dag=dag,
)

# Define task dependencies
start_task >> process_bronze_to_silver >> complete_task
