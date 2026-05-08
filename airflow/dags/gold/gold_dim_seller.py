from include.dag_factory import build_gold_dag
from include.datasets import GOLD_DIM_SELLER, SILVER_SELLERS

dag = build_gold_dag(
    table="dim_seller",
    upstream_datasets=[SILVER_SELLERS],
    output_dataset=GOLD_DIM_SELLER,
)
