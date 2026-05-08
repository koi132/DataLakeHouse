"""Central registry of Airflow Datasets for the medallion Lakehouse.

Each Delta table surface-area owned by a DAG is declared here so per-table
DAGs can outlet exactly one dataset and downstream DAGs can consume them
without hard-coding paths.

URIs mirror the physical Delta locations written by the Spark scripts:
- Bronze lands at s3a://bronze/<kafka_topic_name>/
- Silver lands at s3a://silver/<table_name>/
- Gold   lands at s3a://gold/<table_name>/
"""
from __future__ import annotations

from airflow.datasets import Dataset


# Bronze: raw CDC landings, one per Debezium topic
BRONZE_OLIST_ORDERS = Dataset("s3a://bronze/olist.public.olist_orders/")
BRONZE_OLIST_CUSTOMERS = Dataset("s3a://bronze/olist.public.olist_customers/")
BRONZE_OLIST_GEOLOCATION = Dataset("s3a://bronze/olist.public.olist_geolocation/")
BRONZE_OLIST_ORDER_ITEMS = Dataset("s3a://bronze/olist.public.olist_order_items/")
BRONZE_OLIST_ORDER_PAYMENTS = Dataset("s3a://bronze/olist.public.olist_order_payments/")
BRONZE_OLIST_ORDER_REVIEWS = Dataset("s3a://bronze/olist.public.olist_order_reviews/")
BRONZE_OLIST_PRODUCTS = Dataset("s3a://bronze/olist.public.olist_products/")
BRONZE_OLIST_SELLERS = Dataset("s3a://bronze/olist.public.olist_sellers/")
BRONZE_PRODUCT_CATEGORY_TRANSLATION = Dataset("s3a://bronze/olist.public.product_category_translation/")

# Silver: cleaned + enriched, one per business table
SILVER_CUSTOMERS = Dataset("s3a://silver/olist_customers/")
SILVER_SELLERS = Dataset("s3a://silver/olist_sellers/")
SILVER_PRODUCTS = Dataset("s3a://silver/olist_products/")
SILVER_ORDERS = Dataset("s3a://silver/olist_orders/")
SILVER_ORDER_ITEMS = Dataset("s3a://silver/olist_order_items/")
SILVER_ORDER_PAYMENTS = Dataset("s3a://silver/olist_order_payments/")
SILVER_ORDER_REVIEWS = Dataset("s3a://silver/olist_order_reviews/")

# Gold: star schema dims and fact
GOLD_DIM_DATE = Dataset("s3a://gold/dim_date/")
GOLD_DIM_CUSTOMER = Dataset("s3a://gold/dim_customer/")
GOLD_DIM_SELLER = Dataset("s3a://gold/dim_seller/")
GOLD_DIM_PRODUCT = Dataset("s3a://gold/dim_product/")
GOLD_FACT_ORDERS = Dataset("s3a://gold/fact_orders/")
