from include.dag_factory import build_gold_dag
from include.datasets import GOLD_DIM_DATE, SILVER_ORDERS

dag = build_gold_dag(
    table="dim_date",
    upstream_datasets=[SILVER_ORDERS],
    output_dataset=GOLD_DIM_DATE,
)
