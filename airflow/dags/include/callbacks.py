"""Shared task-level callbacks for per-table DAGs."""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def on_task_failure(context):
    """Structured failure log consumable by log aggregators / alert pipelines."""
    ti = context["task_instance"]
    exec_date = context.get("execution_date", "N/A")
    duration = ti.duration if ti.duration else "N/A"
    attempt = ti.try_number
    logger.error(
        "PIPELINE FAILURE | dag=%s task=%s exec_date=%s attempt=%d duration=%s",
        ti.dag_id, ti.task_id, exec_date, attempt, duration,
    )
