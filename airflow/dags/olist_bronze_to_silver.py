"""
olist_bronze_to_silver
======================
Standalone Silver DAG for the Olist lakehouse.

Transforms bronze Delta tables into cleaned silver tables. The Spark jobs run
one after another so a local Spark setup does not need to execute them in
parallel. When finished, this DAG triggers the gold DAG.
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
    dag_id="olist_bronze_to_silver",
    description="Bronze → Silver transforms for Olist",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["olist", "spark", "silver"],
    doc_md=__doc__,
) as dag:

    b2s_customers = SparkSubmitOperator(
        task_id="customers",
        **common_kwargs("B2S_Customers", f"{B2S}/customers.py"),
    )
    b2s_sellers = SparkSubmitOperator(
        task_id="sellers",
        **common_kwargs("B2S_Sellers", f"{B2S}/sellers.py"),
    )
    b2s_products = SparkSubmitOperator(
        task_id="products",
        **common_kwargs("B2S_Products", f"{B2S}/products.py"),
    )
    b2s_orders = SparkSubmitOperator(
        task_id="orders",
        **common_kwargs("B2S_Orders", f"{B2S}/orders.py"),
    )
    b2s_order_items = SparkSubmitOperator(
        task_id="order_items",
        **common_kwargs("B2S_OrderItems", f"{B2S}/order_items.py"),
    )
    b2s_order_payments = SparkSubmitOperator(
        task_id="order_payments",
        **common_kwargs("B2S_OrderPayments", f"{B2S}/order_payments.py"),
    )
    b2s_order_reviews = SparkSubmitOperator(
        task_id="order_reviews",
        **common_kwargs("B2S_OrderReviews", f"{B2S}/order_reviews.py"),
    )

    register_silver_hive = SparkSubmitOperator(
        task_id="register_silver_hive",
        **common_kwargs("B2S_RegisterHive", f"{B2S}/register_hive.py"),
    )


    silver_tasks = [
        b2s_customers,
        b2s_sellers,
        b2s_products,
        b2s_orders,
        b2s_order_items,
        b2s_order_payments,
        b2s_order_reviews,
    ]

    for current_task, next_task in zip(silver_tasks, silver_tasks[1:]):
        current_task >> next_task

    silver_tasks[-1] >> register_silver_hive