"""Reusable data quality framework for Silver and Gold layers.

Validates Delta tables against configurable rules and writes results to a
quality log Delta table at QUALITY_LOG_PATH.  Returns exit-code-friendly
pass/fail so Airflow can gate downstream steps.
"""

from __future__ import annotations

import sys
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql.functions import col, count, when, isnan, sum as spark_sum

from config import create_spark_session, get_logger, QUALITY_LOG_PATH

logger = get_logger("data_quality")


# ---------------------------------------------------------------------------
# Quality check definitions
# ---------------------------------------------------------------------------
def get_silver_checks() -> dict[str, dict]: return {
    "olist_customers": {
        "path": "s3a://silver/olist_customers/",
        "primary_keys": ["customer_id"],
        "rules": {
            "Valid State Code": col("customer_state").rlike("^[A-Z]{2}$"),
            "Valid Zip Code": col("customer_zip_code_prefix").rlike("^[0-9]{5}$"),
            "Valid Latitude": (col("customer_latitude").isNull()) | col("customer_latitude").between(-90, 90),
            "Valid Longitude": (col("customer_longitude").isNull()) | col("customer_longitude").between(-180, 180),
        },
    },
    "olist_sellers": {
        "path": "s3a://silver/olist_sellers/",
        "primary_keys": ["seller_id"],
        "rules": {
            "Valid State Code": col("seller_state").rlike("^[A-Z]{2}$"),
            "Valid Latitude": (col("seller_latitude").isNull()) | col("seller_latitude").between(-90, 90),
            "Valid Longitude": (col("seller_longitude").isNull()) | col("seller_longitude").between(-180, 180),
        },
    },
    "olist_products": {
        "path": "s3a://silver/olist_products/",
        "primary_keys": ["product_id"],
        "rules": {
            "Positive Weight": (col("product_weight_g").isNull()) | (col("product_weight_g") > 0),
            "Positive Volume": (col("product_volume_cm3").isNull()) | (col("product_volume_cm3") > 0),
        },
    },
    "olist_orders": {
        "path": "s3a://silver/olist_orders/",
        "primary_keys": ["order_id"],
        "rules": {
            "Valid Status": col("order_status").isin(
                ["DELIVERED", "SHIPPED", "CANCELED", "UNAVAILABLE",
                 "INVOICED", "PROCESSING", "CREATED", "APPROVED"]
            ),
            "Purchase Before Delivery": (
                col("order_delivered_customer_date").isNull()
                | (col("order_purchase_timestamp") <= col("order_delivered_customer_date"))
            ),
        },
    },
    "olist_order_items": {
        "path": "s3a://silver/olist_order_items/",
        "primary_keys": ["order_id", "order_item_id"],
        "rules": {
            "Positive Price": col("price") >= 0,
            "Positive Freight": col("freight_value") >= 0,
            "Valid Total": col("total_item_value") >= 0,
        },
    },
    "olist_order_payments": {
        "path": "s3a://silver/olist_order_payments/",
        "primary_keys": ["order_id", "payment_sequential"],
        "rules": {
            "Valid Payment Type": col("payment_type").isin(
                ["CREDIT_CARD", "BOLETO", "VOUCHER", "DEBIT_CARD"]
            ),
            "Positive Amount": col("payment_value") > 0,
            "Valid Installments": col("payment_installments") >= 1,
        },
    },
    "olist_order_reviews": {
        "path": "s3a://silver/olist_order_reviews/",
        "primary_keys": ["review_id"],
        "rules": {
            "Valid Score": col("review_score").between(1, 5),
            "Valid Rating": col("review_rating").isin(
                ["POSITIVE", "NEUTRAL", "NEGATIVE", "UNKNOWN"]
            ),
        },
    },
}

def get_gold_checks() -> dict[str, dict]: return {
    "dim_customer": {
        "path": "s3a://gold/dim_customer/",
        "primary_keys": ["customer_sk"],
        "rules": {},
    },
    "dim_date": {
        "path": "s3a://gold/dim_date/",
        "primary_keys": ["date_sk"],
        "rules": {},
    },
    "dim_seller": {
        "path": "s3a://gold/dim_seller/",
        "primary_keys": ["seller_sk"],
        "rules": {},
    },
    "dim_product": {
        "path": "s3a://gold/dim_product/",
        "primary_keys": ["product_sk"],
        "rules": {},
    },
    "fact_orders": {
        "path": "s3a://gold/fact_orders/",
        "primary_keys": ["order_item_sk"],
        "rules": {
            "Valid customer FK": col("customer_sk").isNotNull(),
            "Valid product FK": col("product_sk").isNotNull(),
            "Valid seller FK": col("seller_sk").isNotNull(),
        },
    },
    "fact_reviews": {
        "path": "s3a://gold/fact_reviews/",
        "primary_keys": ["review_sk"],
        "rules": {
            "Valid customer FK": col("customer_sk").isNotNull(),
            "Valid review score": col("review_score").between(1, 5),
        },
    },
}


# ---------------------------------------------------------------------------
# Core validation logic
# ---------------------------------------------------------------------------
def validate_table(
    spark: SparkSession,
    table_name: str,
    path: str,
    primary_keys: list[str],
    rules: dict | None = None,
) -> tuple[bool, list[dict]]:
    """Validate a Delta table and return (passed, list_of_results).

    A table *fails* if:
      - It has 0 records
      - Any primary key column has NULLs
      - Any duplicate primary keys exist
      - Any custom rule has > 5 % violation rate
    """
    results: list[dict] = []
    passed = True

    try:
        df = spark.read.format("delta").load(path)
    except Exception as exc:
        logger.error("Cannot read %s at %s: %s", table_name, path, exc)
        return False, [{"table": table_name, "check": "readable", "passed": False,
                        "detail": str(exc)}]

    total = df.count()
    if total == 0:
        logger.warning("Table %s is empty", table_name)
        return False, [{"table": table_name, "check": "non_empty", "passed": False,
                        "detail": "0 records"}]

    # PK null check
    for pk in primary_keys:
        nulls = df.filter(col(pk).isNull()).count()
        ok = nulls == 0
        if not ok:
            passed = False
        results.append({
            "table": table_name, "check": f"pk_not_null_{pk}",
            "passed": ok, "detail": f"{nulls}/{total} nulls",
        })

    # Duplicate check
    distinct = df.select(*primary_keys).distinct().count()
    dupes = total - distinct
    ok = dupes == 0
    if not ok:
        passed = False
    results.append({
        "table": table_name, "check": "no_duplicates",
        "passed": ok, "detail": f"{dupes} duplicates",
    })

    # Custom rules (5 % threshold before failing)
    for rule_name, condition in (rules or {}).items():
        try:
            violations = df.filter(~condition).count()
        except Exception:
            violations = -1
        pct = (violations / total * 100) if total > 0 else 0
        ok = pct <= 5.0
        if not ok:
            passed = False
        results.append({
            "table": table_name, "check": rule_name,
            "passed": ok, "detail": f"{violations}/{total} ({pct:.1f}%)",
        })

    status = "PASS" if passed else "FAIL"
    logger.info("%-40s %s  (%d records)", table_name, status, total)
    return passed, results


def run_quality_checks(
    spark: SparkSession,
    layer: str,
    checks: dict[str, dict],
) -> bool:
    """Run all checks for a layer. Returns True if all pass."""
    all_results: list[dict] = []
    all_passed = True

    for table_name, cfg in checks.items():
        ok, results = validate_table(
            spark, table_name,
            path=cfg["path"],
            primary_keys=cfg["primary_keys"],
            rules=cfg.get("rules", {}),
        )
        if not ok:
            all_passed = False
        all_results.extend(results)

    # Persist results to quality log Delta table
    _write_quality_log(spark, layer, all_results)

    summary = "PASSED" if all_passed else "FAILED"
    logger.info("Quality check for [%s]: %s (%d checks)", layer, summary, len(all_results))
    return all_passed


def _write_quality_log(spark: SparkSession, layer: str, results: list[dict]):
    """Append quality check results to the quality log Delta table."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    rows = [
        Row(
            layer=layer,
            table_name=r["table"],
            check_name=r["check"],
            passed=r["passed"],
            detail=r["detail"],
            checked_at=now,
        )
        for r in results
    ]
    try:
        df = spark.createDataFrame(rows)
        df.write.format("delta").mode("append").save(QUALITY_LOG_PATH)
    except Exception:
        logger.warning("Failed to write quality log — continuing", exc_info=True)


# ---------------------------------------------------------------------------
# CLI entry points (called by Airflow)
# ---------------------------------------------------------------------------
def main_silver(table: str | None = None):
    """Validate Silver layer (all tables, or a single one). Exit 1 on failure."""
    app = f"DQ_Silver_{table}" if table else "DQ_Silver"
    spark = create_spark_session(app, "s3a://silver/")
    
    SILVER_CHECKS = get_silver_checks()
    if table and table not in SILVER_CHECKS:
        logger.error("Unknown Silver table: %s", table)
        sys.exit(1)
    checks = {table: SILVER_CHECKS[table]} if table else SILVER_CHECKS
    try:
        ok = run_quality_checks(spark, "silver", checks)
        if not ok:
            logger.error("Silver quality gate FAILED")
            sys.exit(1)
        logger.info("Silver quality gate PASSED")
    finally:
        spark.stop()


def main_gold(table: str | None = None):
    """Validate Gold layer (all tables, or a single one). Exit 1 on failure."""
    app = f"DQ_Gold_{table}" if table else "DQ_Gold"
    spark = create_spark_session(app, "s3a://gold/")
    
    GOLD_CHECKS = get_gold_checks()
    if table and table not in GOLD_CHECKS:
        logger.error("Unknown Gold table: %s", table)
        sys.exit(1)
    checks = {table: GOLD_CHECKS[table]} if table else GOLD_CHECKS
    try:
        ok = run_quality_checks(spark, "gold", checks)
        if not ok:
            logger.error("Gold quality gate FAILED")
            sys.exit(1)
        logger.info("Gold quality gate PASSED")
    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Data quality gate")
    parser.add_argument("--layer", required=True, choices=["silver", "gold"])
    parser.add_argument("--table", help="Validate only this single table in the chosen layer")
    args = parser.parse_args()

    if args.layer == "silver":
        main_silver(args.table)
    else:
        main_gold(args.table)
