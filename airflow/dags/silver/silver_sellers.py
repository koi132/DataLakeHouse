from include.dag_factory import build_silver_dag
from include.datasets import (
    BRONZE_OLIST_SELLERS,
    BRONZE_OLIST_GEOLOCATION,
    SILVER_SELLERS,
)

dag = build_silver_dag(
    table="sellers",
    upstream_datasets=[BRONZE_OLIST_SELLERS, BRONZE_OLIST_GEOLOCATION],
    output_dataset=SILVER_SELLERS,
)
