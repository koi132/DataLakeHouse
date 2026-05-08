import argparse
import sys

from config import create_spark_session, get_logger, SILVER_BUCKET
from utils import register_hive_tables

logger = get_logger("b2s.register_hive")

SILVER_TABLES_CONFIG = {
    "olist_customers": f"{SILVER_BUCKET}/olist_customers/",
    "olist_sellers": f"{SILVER_BUCKET}/olist_sellers/",
    "olist_products": f"{SILVER_BUCKET}/olist_products/",
    "olist_orders": f"{SILVER_BUCKET}/olist_orders/",
    "olist_order_items": f"{SILVER_BUCKET}/olist_order_items/",
    "olist_order_payments": f"{SILVER_BUCKET}/olist_order_payments/",
    "olist_order_reviews": f"{SILVER_BUCKET}/olist_order_reviews/",
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Register Silver Delta tables in Hive")
    parser.add_argument("--table", help="Register only this single Silver table")
    args = parser.parse_args()

    if args.table and args.table not in SILVER_TABLES_CONFIG:
        logger.error("Unknown Silver table: %s", args.table)
        sys.exit(1)

    targets = (
        {args.table: SILVER_TABLES_CONFIG[args.table]}
        if args.table
        else SILVER_TABLES_CONFIG
    )

    spark = create_spark_session("B2S_RegisterHive", f"{SILVER_BUCKET}/")
    try:
        _, failed = register_hive_tables(spark, "silver", targets)
        if failed:
            sys.exit(1)
        logger.info("Silver Hive registration completed")
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
