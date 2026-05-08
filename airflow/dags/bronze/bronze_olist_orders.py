from include.dag_factory import build_bronze_dag
from include.datasets import BRONZE_OLIST_ORDERS

dag = build_bronze_dag(
    table="olist_orders",
    topic="olist.public.olist_orders",
    output_dataset=BRONZE_OLIST_ORDERS,
)
