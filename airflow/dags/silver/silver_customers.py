from include.dag_factory import build_silver_dag
from include.datasets import (
    BRONZE_OLIST_CUSTOMERS,
    BRONZE_OLIST_GEOLOCATION,
    SILVER_CUSTOMERS,
)

dag = build_silver_dag(
    table="customers",
    upstream_datasets=[BRONZE_OLIST_CUSTOMERS, BRONZE_OLIST_GEOLOCATION],
    output_dataset=SILVER_CUSTOMERS,
)
