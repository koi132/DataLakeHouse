"""
olist_etl_pipeline
==================
Full medallion ETL pipeline for the Olist e-commerce data lakehouse.

Stage 1 – Kafka → Bronze
    Reads all Debezium CDC topics from Kafka and writes Delta tables to MinIO.

Stage 2 – Bronze → Silver  (runs after Bronze Hive registration)
    Cleans, types, and enriches each domain entity.

Stage 3 – Silver → Gold  (runs after Silver Hive registration)
    Builds dimension and fact tables for analytics.

Each Spark job is submitted to the standalone spark-master via
SparkSubmitOperator. Airflow must be on the same Docker network
('data-network') as the Spark cluster.

Connections required in Airflow UI (Admin → Connections):
  - conn_id: spark_default
    conn_type: Spark
    host: spark://spark-master
    port: 7077
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import TaskGroup

from spark_submit_defaults import common_kwargs, SPARK_APP_DIR

# ---------------------------------------------------------------------------
# Default task arguments
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

B2S = f"{SPARK_APP_DIR}/bronze_to_silver"
S2G = f"{SPARK_APP_DIR}/silver_to_gold"

# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------
with DAG(
    dag_id="olist_etl_pipeline",
    description="Kafka → Bronze → Silver → Gold medallion ETL (Olist)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["olist", "spark", "etl", "medallion"],
    doc_md=__doc__,
) as dag:

    # =========================================================================
    # Stage 1: Kafka → Bronze
    # =========================================================================
    with TaskGroup("bronze_ingestion") as bronze_group:

        kafka_to_bronze = SparkSubmitOperator(
            task_id="kafka_to_bronze",
            **common_kwargs(
                "OlistKafkaToBronze",
                f"{SPARK_APP_DIR}/process_kafka_to_bronze.py",
            ),
        )

        register_bronze_hive = SparkSubmitOperator(
            task_id="register_bronze_hive",
            **common_kwargs(
                "B2S_RegisterHive",
                f"{B2S}/register_hive.py",
            ),
        )

        kafka_to_bronze >> register_bronze_hive

    # =========================================================================
    # Stage 2: Bronze → Silver
    # =========================================================================
    with TaskGroup("bronze_to_silver") as silver_group:

        # 7 entity transforms run in parallel
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

        # All 7 transforms run in parallel, then Hive registration
        for b2s_task in [
            b2s_customers, b2s_sellers, b2s_products, b2s_orders,
            b2s_order_items, b2s_order_payments, b2s_order_reviews,
        ]:
            b2s_task >> register_silver_hive

    # =========================================================================
    # Stage 3: Silver → Gold
    # =========================================================================
    with TaskGroup("silver_to_gold") as gold_group:

        # Dimensions: date is independent; customer/seller/product depend on silver
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

        # Facts depend on all 4 dimensions being ready
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

        dims = [s2g_dim_date, s2g_dim_customer, s2g_dim_seller, s2g_dim_product]
        facts = [s2g_fact_orders, s2g_fact_reviews]

        for dim in dims:
            for fact in facts:
                dim >> fact
        for fact in facts:
            fact >> register_gold_hive

    # =========================================================================
    # Cross-group dependencies
    # =========================================================================
    register_bronze_hive >> b2s_customers
    register_bronze_hive >> b2s_sellers
    register_bronze_hive >> b2s_products
    register_bronze_hive >> b2s_orders
    register_bronze_hive >> b2s_order_items
    register_bronze_hive >> b2s_order_payments
    register_bronze_hive >> b2s_order_reviews
    register_silver_hive >> s2g_dim_date
    register_silver_hive >> s2g_dim_customer
    register_silver_hive >> s2g_dim_seller
    register_silver_hive >> s2g_dim_product
