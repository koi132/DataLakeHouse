from include.dag_factory import build_bronze_dag
from include.datasets import BRONZE_OLIST_ORDER_REVIEWS

dag = build_bronze_dag(
    table="olist_order_reviews",
    topic="olist.public.olist_order_reviews",
    output_dataset=BRONZE_OLIST_ORDER_REVIEWS,
)
