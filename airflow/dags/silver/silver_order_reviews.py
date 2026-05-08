from include.dag_factory import build_silver_dag
from include.datasets import BRONZE_OLIST_ORDER_REVIEWS, SILVER_ORDER_REVIEWS

dag = build_silver_dag(
    table="order_reviews",
    upstream_datasets=[BRONZE_OLIST_ORDER_REVIEWS],
    output_dataset=SILVER_ORDER_REVIEWS,
)
