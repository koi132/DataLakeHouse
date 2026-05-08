"""Helpers for building spark-submit bash commands executed from Airflow.

Every Spark job runs inside the spark-master container via `docker exec`,
which keeps Airflow itself free of PySpark/Hadoop/Kafka jars.
"""
from __future__ import annotations

# Delta + S3A packages needed by every job
DELTA_PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)

# Kafka connector is only needed by the Bronze ingestion script
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"


def _submit_base(with_kafka: bool) -> str:
    packages = DELTA_PACKAGES
    if with_kafka:
        packages = f"{packages},{KAFKA_PACKAGE}"
    return (
        "docker exec -e PYTHONPATH=/opt/spark/app spark-master"
        " /opt/spark/bin/spark-submit --master local[*]"
        f" --packages {packages}"
    )


def spark_submit(script: str, *args: str, with_kafka: bool = False) -> str:
    """Build a bash command that spark-submits `script` with trailing args."""
    parts = [_submit_base(with_kafka), script, *args]
    return " ".join(parts)
