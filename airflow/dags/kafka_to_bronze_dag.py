from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'kafka_to_bronze_batch',
    default_args=default_args,
    description='Batch processing from Kafka to Bronze layer',
    schedule_interval='*/30 * * * *',  # Run every 30 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=['olist', 'bronze', 'kafka', 'batch'],
)

def log_start():
    logging.info("=" * 80)
    logging.info("Starting Kafka to Bronze Batch Processing")
    logging.info("=" * 80)

def log_completion():
    logging.info("=" * 80)
    logging.info("✓ Kafka to Bronze Batch Processing Completed!")
    logging.info("=" * 80)

start_task = PythonOperator(
    task_id='log_pipeline_start',
    python_callable=log_start,
    dag=dag,
)

process_kafka_to_bronze = BashOperator(
    task_id='process_kafka_to_bronze_batch',
    bash_command='''
    docker exec spark-master /opt/spark/bin/spark-submit \
      --master local[*] \
      --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      /opt/spark/app/batch_kafka_to_bronze.py
    ''',
    dag=dag,
)

complete_task = PythonOperator(
    task_id='log_pipeline_completion',
    python_callable=log_completion,
    dag=dag,
)

start_task >> process_kafka_to_bronze >> complete_task