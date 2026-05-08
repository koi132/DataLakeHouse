from __future__ import annotations

import time
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, when, from_unixtime, row_number, desc, monotonically_increasing_id,
)
from pyspark.sql.types import TimestampType
from pyspark.sql.window import Window

from pipeline_metrics import record_metric


logger = logging.getLogger(__name__)


def safe_to_timestamp(column):
    """Convert column to timestamp, handling both TIMESTAMP and LONG (Debezium ms) types."""
    return when(
        column.cast("string").rlike("^[0-9]+$"),
        from_unixtime(column.cast("long") / 1000).cast(TimestampType()),
    ).otherwise(
        column.cast(TimestampType()),
    )


def generate_surrogate_key(df: DataFrame, key_column_name: str = "sk") -> DataFrame:
    """Add an auto-incrementing surrogate key column starting from 1."""
    return df.withColumn(key_column_name, monotonically_increasing_id() + 1)


def extract_cdc_latest(
    df: DataFrame,
    key_cols: list[str],
    ts_col: str = "ts_ms",
    select_after: bool = True,
) -> DataFrame:
    """Extract the latest CDC record per business key.

    Filters out deletes (op='d'), optionally unpacks the 'after' struct,
    and deduplicates by key_cols using ts_col descending.
    """
    df_active = df.filter(col("op") != "d")

    if select_after:
        df_active = df_active.select("after.*", ts_col)

    window_spec = Window.partitionBy(*key_cols).orderBy(desc(ts_col))
    return (
        df_active
        .withColumn("_row_num", row_number().over(window_spec))
        .filter(col("_row_num") == 1)
        .drop("_row_num", ts_col)
    )


def register_hive_tables(
    spark: SparkSession,
    database: str,
    tables_config: dict[str, str],
) -> tuple[int, int]:
    """Register Delta tables in Hive Metastore for Trino consumption.

    Returns (registered_count, failed_count).
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")

    registered, failed = 0, 0
    for table_name, path in tables_config.items():
        try:
            spark.sql(f"DROP TABLE IF EXISTS {database}.{table_name}")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {database}.{table_name}
                USING DELTA
                LOCATION '{path}'
            """)
            logger.info("Registered %s.%s", database, table_name)
            registered += 1
        except Exception:
            logger.exception("Failed to register %s.%s", database, table_name)
            failed += 1

    spark.sql(f"SHOW TABLES IN {database}").show(truncate=False)
    logger.info(
        "Hive registration for [%s]: %d success, %d failed",
        database, registered, failed,
    )
    return registered, failed


def write_with_metrics(
    df: DataFrame,
    spark: SparkSession,
    layer: str,
    table_name: str,
    path: str,
    mode: str = "overwrite",
    overwrite_schema: bool = True,
) -> int:
    """Write a DataFrame to Delta and record execution metrics.

    Returns the row count written.
    """
    start = time.time()
    row_count = -1
    try:
        writer = df.write.format("delta").mode(mode)
        if overwrite_schema:
            writer = writer.option("overwriteSchema", "true")
        writer.save(path)

        row_count = df.count()
        duration = round(time.time() - start, 2)

        record_metric(spark, layer, table_name, row_count, duration, "success")
        logger.info(
            "Wrote %s.%s — %d rows in %.1fs",
            layer, table_name, row_count, duration,
        )
    except Exception as exc:
        duration = round(time.time() - start, 2)
        record_metric(spark, layer, table_name, 0, duration, "failed", str(exc))
        raise

    return row_count
