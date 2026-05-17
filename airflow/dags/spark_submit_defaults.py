"""
Shared defaults for SparkSubmitOperator tasks in this data-lakehouse.

All Spark jobs need the same Delta / Kafka / S3 JARs, the same app-level
py-files, and the same credentials forwarded from Airflow env vars.  Keep
everything here so the DAG file stays readable.
"""

from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# Spark packages required by every job
# ---------------------------------------------------------------------------
SPARK_PACKAGES = ",".join([
    "io.delta:delta-spark_2.12:3.1.0",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
])

# ---------------------------------------------------------------------------
# Python helper modules that every job imports (config, utils, metrics, dq)
# ---------------------------------------------------------------------------
SPARK_APP_DIR = "/opt/spark/app"

PY_FILES = ",".join([
    f"{SPARK_APP_DIR}/config.py",
    f"{SPARK_APP_DIR}/utils.py",
    f"{SPARK_APP_DIR}/pipeline_metrics.py",
    f"{SPARK_APP_DIR}/data_quality.py",
])

# ---------------------------------------------------------------------------
# Spark conf overrides applied to every submit
# ---------------------------------------------------------------------------
SPARK_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.delta.logStore.class": "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.sql.legacy.parquet.datetimeRebaseModeInWrite": "CORRECTED",
    "spark.sql.legacy.parquet.int96RebaseModeInWrite": "CORRECTED",
}

# ---------------------------------------------------------------------------
# Env vars forwarded into every spark-submit process
# ---------------------------------------------------------------------------
SPARK_ENV_VARS = {
    "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "admin"),
    "AWS_SECRET_ACCESS_KEY": os.environ.get("AWS_SECRET_ACCESS_KEY", "password123"),
    "S3_ENDPOINT": os.environ.get("S3_ENDPOINT", "http://minio:9000"),
    "HIVE_METASTORE_URI": os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083"),
    "KAFKA_BROKER": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    "PYTHONPATH": SPARK_APP_DIR,
}

# ---------------------------------------------------------------------------
# Master URL  (Airflow and Spark must share the same Docker network)
# ---------------------------------------------------------------------------
SPARK_MASTER = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")


def common_kwargs(app_name: str, application: str, **extra) -> dict:
    """
    Return a dict of SparkSubmitOperator kwargs shared by all tasks.

    Usage:
        SparkSubmitOperator(
            task_id="...",
            **common_kwargs("MyApp", "/opt/spark/app/path/to/script.py"),
        )
    """
    return {
        "name": app_name,
        "application": application,
        "conn_id": "spark_default",
        "packages": SPARK_PACKAGES,
        "py_files": PY_FILES,
        "conf": SPARK_CONF,
        "env_vars": SPARK_ENV_VARS,
        "verbose": False,
        **extra,
    }
