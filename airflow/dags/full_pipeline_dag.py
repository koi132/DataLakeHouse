from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

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
    'full_data_pipeline',
    default_args=default_args,
    description='Full data pipeline: Bronze -> Silver -> Gold layers',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['olist', 'bronze', 'silver', 'gold', 'full-pipeline'],
)

# ============================================================================
# BRONZE LAYER
# ============================================================================

# Task: Process Kafka to Bronze
process_kafka_to_bronze = BashOperator(
    task_id='process_kafka_to_bronze',
    bash_command='''
    docker exec spark-master /opt/spark/bin/spark-submit \
      --master local[*] \
      --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
      /opt/spark/app/process_kafka_to_bronze.py
    ''',
    dag=dag,
)

# ============================================================================
# SILVER LAYER
# ============================================================================

# Task: Process Bronze to Silver
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

# ============================================================================
# GOLD LAYER
# ============================================================================

# Task: Process Silver to Gold
process_silver_to_gold = BashOperator(
    task_id='process_silver_to_gold',
    bash_command='''
    docker exec spark-master /opt/spark/bin/spark-submit \
      --master local[*] \
      --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
      /opt/spark/app/process_silver_to_gold.py
    ''',
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES
# ============================================================================
# Flow: Bronze -> Silver -> Gold

process_kafka_to_bronze >> process_bronze_to_silver >> process_silver_to_gold
