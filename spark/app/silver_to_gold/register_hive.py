import argparse
import sys

from config import create_spark_session, get_logger, GOLD_BUCKET
from utils import register_hive_tables

logger = get_logger("s2g.register_hive")

GOLD_TABLES_CONFIG = {
    "dim_date": f"{GOLD_BUCKET}/dim_date/",
    "dim_customer": f"{GOLD_BUCKET}/dim_customer/",
    "dim_seller": f"{GOLD_BUCKET}/dim_seller/",
    "dim_product": f"{GOLD_BUCKET}/dim_product/",
    "fact_orders": f"{GOLD_BUCKET}/fact_orders/",
    "fact_reviews": f"{GOLD_BUCKET}/fact_reviews/",
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Register Gold Delta tables in Hive")
    parser.add_argument("--table", help="Register only this single Gold table")
    args = parser.parse_args()

    if args.table and args.table not in GOLD_TABLES_CONFIG:
        logger.error("Unknown Gold table: %s", args.table)
        sys.exit(1)

    targets = (
        {args.table: GOLD_TABLES_CONFIG[args.table]}
        if args.table
        else GOLD_TABLES_CONFIG
    )

    spark = create_spark_session("S2G_RegisterHive", f"{GOLD_BUCKET}/")
    try:
        _, failed = register_hive_tables(spark, "gold", targets)
        if failed:
            sys.exit(1)
        logger.info("Gold Hive registration completed")
    except Exception:
        logger.exception("Failed")
        sys.exit(1)
    finally:
        spark.stop()
