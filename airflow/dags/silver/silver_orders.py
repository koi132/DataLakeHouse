from include.dag_factory import build_silver_dag
from include.datasets import BRONZE_OLIST_ORDERS, SILVER_ORDERS

dag = build_silver_dag(
    table="orders",
    upstream_datasets=[BRONZE_OLIST_ORDERS],
    output_dataset=SILVER_ORDERS,
)
