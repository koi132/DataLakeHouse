from include.dag_factory import build_gold_dag
from include.datasets import (
    GOLD_DIM_CUSTOMER,
    GOLD_DIM_PRODUCT,
    GOLD_DIM_SELLER,
    GOLD_FACT_ORDERS,
    SILVER_CUSTOMERS,
    SILVER_ORDER_ITEMS,
    SILVER_ORDER_PAYMENTS,
    SILVER_ORDER_REVIEWS,
    SILVER_ORDERS,
)

dag = build_gold_dag(
    table="fact_orders",
    upstream_datasets=[
        SILVER_ORDERS,
        SILVER_ORDER_ITEMS,
        SILVER_ORDER_PAYMENTS,
        SILVER_ORDER_REVIEWS,
        SILVER_CUSTOMERS,
        GOLD_DIM_CUSTOMER,
        GOLD_DIM_SELLER,
        GOLD_DIM_PRODUCT,
    ],
    output_dataset=GOLD_FACT_ORDERS,
    use_dataset_all=True,
    transform_timeout_min=30,
)
