"""
device_events_gold DAG

Merges deduplicated events from bronze into gold tables every 5 minutes.
Each event type runs as a separate Trino task in parallel.
"""

from __future__ import annotations

import os
from pathlib import Path

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

SQL_ROOT = Path(os.environ["READYDATA_SQL_ROOT"])

EVENT_TABLES = [
    "throughput",
    "connectivity",
    "device_health",
    "connectivity_state",
    "infrastructure",
]

with DAG(
    dag_id="device_events_gold",
    description="Merge bronze device events into deduplicated gold tables",
    schedule="*/5 * * * *",
    catchup=False,
    tags=["gold", "merge"],
):
    for table in EVENT_TABLES:
        sql_path = SQL_ROOT / "merge" / f"merge_{table}_gold.sql"
        SQLExecuteQueryOperator(
            task_id=f"merge_{table}",
            conn_id="trino_default",
            sql=sql_path.read_text(),
        )
