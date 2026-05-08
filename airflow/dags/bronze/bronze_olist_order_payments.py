from include.dag_factory import build_bronze_dag
from include.datasets import BRONZE_OLIST_ORDER_PAYMENTS

dag = build_bronze_dag(
    table="olist_order_payments",
    topic="olist.public.olist_order_payments",
    output_dataset=BRONZE_OLIST_ORDER_PAYMENTS,
)
