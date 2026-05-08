from include.dag_factory import build_silver_dag
from include.datasets import BRONZE_OLIST_ORDER_ITEMS, SILVER_ORDER_ITEMS

dag = build_silver_dag(
    table="order_items",
    upstream_datasets=[BRONZE_OLIST_ORDER_ITEMS],
    output_dataset=SILVER_ORDER_ITEMS,
)
