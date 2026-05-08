from include.dag_factory import build_silver_dag
from include.datasets import BRONZE_OLIST_ORDER_PAYMENTS, SILVER_ORDER_PAYMENTS

dag = build_silver_dag(
    table="order_payments",
    upstream_datasets=[BRONZE_OLIST_ORDER_PAYMENTS],
    output_dataset=SILVER_ORDER_PAYMENTS,
)
