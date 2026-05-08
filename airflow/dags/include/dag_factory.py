"""Factory builders for per-table Bronze / Silver / Gold DAGs.

Every DAG produced by these builders follows the same 3-task contract:

    transform_<table>  ->  register_hive_<table>  ->  validate_dq_<table>

The final task emits the output Dataset so downstream DAGs auto-trigger
via Airflow's Dataset scheduler. Bronze DAGs skip DQ (raw CDC layer).
"""
from __future__ import annotations

from datetime import timedelta
from typing import Sequence

import pendulum
from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.bash import BashOperator

from include.callbacks import on_task_failure
from include.spark_helpers import spark_submit


DEFAULT_ARGS = {
    "owner": "data-engineer",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_task_failure,
}

START_DATE = pendulum.datetime(2024, 1, 1, tz="UTC")


def build_bronze_dag(
    *,
    table: str,
    topic: str,
    output_dataset: Dataset,
    schedule: str = "*/30 * * * *",
    ingest_timeout_min: int = 15,
    register_timeout_min: int = 10,
) -> DAG:
    """Build a Bronze ingestion DAG for a single Kafka topic."""
    with DAG(
        dag_id=f"bronze_{table}",
        description=f"Ingest Kafka topic {topic} into Bronze Delta and register in Hive",
        default_args=DEFAULT_ARGS,
        schedule=schedule,
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        tags=["olist", "bronze", "per-table", table],
    ) as dag:
        ingest = BashOperator(
            task_id=f"ingest_{table}",
            bash_command=spark_submit(
                "/opt/spark/app/process_kafka_to_bronze.py",
                "--table", topic,
                with_kafka=True,
            ),
            execution_timeout=timedelta(minutes=ingest_timeout_min),
        )
        register = BashOperator(
            task_id=f"register_hive_{table}",
            bash_command=spark_submit(
                "/opt/spark/app/process_kafka_to_bronze.py",
                "--register-hive", "--table", table,
                with_kafka=True,
            ),
            execution_timeout=timedelta(minutes=register_timeout_min),
            outlets=[output_dataset],
        )
        ingest >> register
    return dag


def build_silver_dag(
    *,
    table: str,
    upstream_datasets: Sequence[Dataset],
    output_dataset: Dataset,
    transform_timeout_min: int = 20,
    register_timeout_min: int = 10,
    dq_timeout_min: int = 15,
) -> DAG:
    """Build a Silver transform DAG for a single table.

    `table` must match both the script filename under
    spark/app/bronze_to_silver/ and the business table stem. The Hive /
    DQ identifier is always `olist_<table>` to match SILVER_CHECKS keys.
    """
    silver_table = f"olist_{table}"
    with DAG(
        dag_id=f"silver_{table}",
        description=f"Transform Bronze->Silver for {silver_table} with per-table DQ gate",
        default_args=DEFAULT_ARGS,
        schedule=list(upstream_datasets),
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        tags=["olist", "silver", "per-table", table],
    ) as dag:
        transform = BashOperator(
            task_id=f"transform_{table}",
            bash_command=spark_submit(
                f"/opt/spark/app/bronze_to_silver/{table}.py",
            ),
            execution_timeout=timedelta(minutes=transform_timeout_min),
        )
        register = BashOperator(
            task_id=f"register_hive_{table}",
            bash_command=spark_submit(
                "/opt/spark/app/bronze_to_silver/register_hive.py",
                "--table", silver_table,
            ),
            execution_timeout=timedelta(minutes=register_timeout_min),
        )
        validate = BashOperator(
            task_id=f"validate_dq_{table}",
            bash_command=spark_submit(
                "/opt/spark/app/data_quality.py",
                "--layer", "silver",
                "--table", silver_table,
            ),
            execution_timeout=timedelta(minutes=dq_timeout_min),
            outlets=[output_dataset],
        )
        transform >> register >> validate
    return dag


def build_gold_dag(
    *,
    table: str,
    upstream_datasets: Sequence[Dataset],
    output_dataset: Dataset,
    use_dataset_all: bool = False,
    transform_timeout_min: int = 20,
    register_timeout_min: int = 10,
    dq_timeout_min: int = 15,
) -> DAG:
    """Build a Gold dim/fact DAG.

    `table` doubles as the script filename under spark/app/silver_to_gold/
    and the Hive / DQ identifier (e.g. dim_customer, fact_orders).

    When `use_dataset_all=True`, the DAG only fires once every upstream
    Dataset has been updated at least once since the last run - required
    by `gold_fact_orders` to avoid partial-state builds.
    """
    if use_dataset_all:
        # DatasetAll was added in Airflow 2.9; imported lazily so unit tests
        # or older planners do not fail at module load.
        from airflow.datasets import DatasetAll

        schedule = DatasetAll(*upstream_datasets)
    else:
        schedule = list(upstream_datasets)

    with DAG(
        dag_id=f"gold_{table}",
        description=f"Build Gold {table} with per-table DQ gate",
        default_args=DEFAULT_ARGS,
        schedule=schedule,
        start_date=START_DATE,
        catchup=False,
        max_active_runs=1,
        tags=["olist", "gold", "per-table", table],
    ) as dag:
        transform = BashOperator(
            task_id=f"transform_{table}",
            bash_command=spark_submit(
                f"/opt/spark/app/silver_to_gold/{table}.py",
            ),
            execution_timeout=timedelta(minutes=transform_timeout_min),
        )
        register = BashOperator(
            task_id=f"register_hive_{table}",
            bash_command=spark_submit(
                "/opt/spark/app/silver_to_gold/register_hive.py",
                "--table", table,
            ),
            execution_timeout=timedelta(minutes=register_timeout_min),
        )
        validate = BashOperator(
            task_id=f"validate_dq_{table}",
            bash_command=spark_submit(
                "/opt/spark/app/data_quality.py",
                "--layer", "gold",
                "--table", table,
            ),
            execution_timeout=timedelta(minutes=dq_timeout_min),
            outlets=[output_dataset],
        )
        transform >> register >> validate
    return dag
