from include.dag_factory import build_bronze_dag
from include.datasets import BRONZE_OLIST_CUSTOMERS

dag = build_bronze_dag(
    table="olist_customers",
    topic="olist.public.olist_customers",
    output_dataset=BRONZE_OLIST_CUSTOMERS,
)
