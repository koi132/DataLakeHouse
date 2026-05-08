from include.dag_factory import build_bronze_dag
from include.datasets import BRONZE_OLIST_PRODUCTS

dag = build_bronze_dag(
    table="olist_products",
    topic="olist.public.olist_products",
    output_dataset=BRONZE_OLIST_PRODUCTS,
)
