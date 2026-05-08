from include.dag_factory import build_bronze_dag
from include.datasets import BRONZE_PRODUCT_CATEGORY_TRANSLATION

dag = build_bronze_dag(
    table="product_category_translation",
    topic="olist.public.product_category_translation",
    output_dataset=BRONZE_PRODUCT_CATEGORY_TRANSLATION,
)
