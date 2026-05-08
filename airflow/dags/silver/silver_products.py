from include.dag_factory import build_silver_dag
from include.datasets import (
    BRONZE_OLIST_PRODUCTS,
    BRONZE_PRODUCT_CATEGORY_TRANSLATION,
    SILVER_PRODUCTS,
)

dag = build_silver_dag(
    table="products",
    upstream_datasets=[BRONZE_OLIST_PRODUCTS, BRONZE_PRODUCT_CATEGORY_TRANSLATION],
    output_dataset=SILVER_PRODUCTS,
)
