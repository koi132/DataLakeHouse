"""
olist_silver_to_gold
====================
Standalone Gold DAG for the Olist lakehouse.

Builds the dimensional and fact tables from silver data. The Spark jobs run in
sequence to keep the local Spark cluster from running multiple gold jobs at the
same time.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from spark_submit_defaults import SPARK_APP_DIR, common_kwargs

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

S2G = f"{SPARK_APP_DIR}/silver_to_gold"

with DAG(
    dag_id="olist_silver_to_gold",
    description="Silver → Gold transforms for Olist",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["olist", "spark", "gold"],
    doc_md=__doc__,
) as dag:

    s2g_dim_date = SparkSubmitOperator(
        task_id="dim_date",
        **common_kwargs("S2G_DimDate", f"{S2G}/dim_date.py"),
    )
    s2g_dim_customer = SparkSubmitOperator(
        task_id="dim_customer",
        **common_kwargs("S2G_DimCustomer", f"{S2G}/dim_customer.py"),
    )
    s2g_dim_seller = SparkSubmitOperator(
        task_id="dim_seller",
        **common_kwargs("S2G_DimSeller", f"{S2G}/dim_seller.py"),
    )
    s2g_dim_product = SparkSubmitOperator(
        task_id="dim_product",
        **common_kwargs("S2G_DimProduct", f"{S2G}/dim_product.py"),
    )

    s2g_fact_orders = SparkSubmitOperator(
        task_id="fact_orders",
        **common_kwargs("S2G_FactOrders", f"{S2G}/fact_orders.py"),
    )
    s2g_fact_reviews = SparkSubmitOperator(
        task_id="fact_reviews",
        **common_kwargs("S2G_FactReviews", f"{S2G}/fact_reviews.py"),
    )

    register_gold_hive = SparkSubmitOperator(
        task_id="register_gold_hive",
        **common_kwargs("S2G_RegisterHive", f"{S2G}/register_hive.py"),
    )

    gold_tasks = [
        s2g_dim_customer,
        s2g_dim_date,
        s2g_dim_seller,
        s2g_dim_product,
        s2g_fact_orders,
        s2g_fact_reviews,
    ]

    for current_task, next_task in zip(gold_tasks, gold_tasks[1:]):
        current_task >> next_task

    gold_tasks[-1] >> register_gold_hive