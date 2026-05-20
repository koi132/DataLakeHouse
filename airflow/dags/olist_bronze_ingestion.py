"""
olist_bronze_ingestion
======================
Standalone Bronze DAG for the Olist lakehouse.

Reads raw CDC data from Kafka into Delta bronze tables and registers the
bronze schema in Hive. When it finishes, it triggers the silver DAG.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from spark_submit_defaults import SPARK_APP_DIR, common_kwargs

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

B2S = f"{SPARK_APP_DIR}/bronze_to_silver"

with DAG(
    dag_id="olist_bronze_ingestion",
    description="Kafka → Bronze ingestion for Olist",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["olist", "spark", "bronze"],
    doc_md=__doc__,
) as dag:

    kafka_to_bronze = SparkSubmitOperator(
        task_id="kafka_to_bronze",
        **common_kwargs(
            "OlistKafkaToBronze",
            f"{SPARK_APP_DIR}/process_kafka_to_bronze.py",
        ),
    )

    register_bronze_hive = SparkSubmitOperator(
        task_id="register_bronze_hive",
        **common_kwargs("B2S_RegisterHive", f"{B2S}/register_hive.py"),
    )


    kafka_to_bronze >> register_bronze_hive 