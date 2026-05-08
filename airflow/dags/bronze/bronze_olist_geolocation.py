from include.dag_factory import build_bronze_dag
from include.datasets import BRONZE_OLIST_GEOLOCATION

dag = build_bronze_dag(
    table="olist_geolocation",
    topic="olist.public.olist_geolocation",
    output_dataset=BRONZE_OLIST_GEOLOCATION,
)
