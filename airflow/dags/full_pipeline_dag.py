"""Orchestrator for the full medallion pipeline.

Fans out to the 9 Bronze per-topic DAGs in parallel and waits for them.
Silver and Gold DAGs cascade automatically via Airflow Datasets once their
upstream Bronze tables land, so no explicit silver/gold triggers are needed.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from include.callbacks import on_task_failure


BRONZE_DAG_IDS = [
    "bronze_olist_orders",
    "bronze_olist_customers",
    "bronze_olist_geolocation",
    "bronze_olist_order_items",
    "bronze_olist_order_payments",
    "bronze_olist_order_reviews",
    "bronze_olist_products",
    "bronze_olist_sellers",
    "bronze_product_category_translation",
]

default_args = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_task_failure,
}

with DAG(
    dag_id="full_data_pipeline",
    default_args=default_args,
    description="Orchestrator: trigger all Bronze DAGs; Silver and Gold cascade via Datasets",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["olist", "orchestrator", "full-pipeline"],
    doc_md="""
### Full Data Pipeline

Kicks off the 9 Bronze DAGs in parallel and waits for ingestion + Hive
registration to complete for every topic. Silver DAGs auto-trigger on
their Bronze Dataset updates; Gold DAGs auto-trigger on Silver (and Gold
dim) Dataset updates. `gold_fact_orders` uses DatasetAll so it only runs
once every upstream has refreshed at least once.
""",
) as dag:
    for bronze_dag_id in BRONZE_DAG_IDS:
        TriggerDagRunOperator(
            task_id=f"trigger_{bronze_dag_id}",
            trigger_dag_id=bronze_dag_id,
            wait_for_completion=True,
            poke_interval=30,
            execution_timeout=timedelta(minutes=45),
            reset_dag_run=True,
        )
