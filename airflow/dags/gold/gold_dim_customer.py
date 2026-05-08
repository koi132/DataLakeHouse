from include.dag_factory import build_gold_dag
from include.datasets import GOLD_DIM_CUSTOMER, SILVER_CUSTOMERS

dag = build_gold_dag(
    table="dim_customer",
    upstream_datasets=[SILVER_CUSTOMERS],
    output_dataset=GOLD_DIM_CUSTOMER,
)
