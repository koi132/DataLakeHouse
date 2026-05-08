from include.dag_factory import build_gold_dag
from include.datasets import GOLD_DIM_PRODUCT, SILVER_PRODUCTS

dag = build_gold_dag(
    table="dim_product",
    upstream_datasets=[SILVER_PRODUCTS],
    output_dataset=GOLD_DIM_PRODUCT,
)
