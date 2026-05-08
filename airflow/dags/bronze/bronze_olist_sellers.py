from include.dag_factory import build_bronze_dag
from include.datasets import BRONZE_OLIST_SELLERS

dag = build_bronze_dag(
    table="olist_sellers",
    topic="olist.public.olist_sellers",
    output_dataset=BRONZE_OLIST_SELLERS,
)
