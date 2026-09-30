"""
iceberg_maintenance DAG

Manually triggered. For each gold table, inspects file metadata via SQL
and only compacts tables with a small-file problem (>10 files, avg <10MB).

Destructive catalog or object cleanup (snapshot expiry, orphan file removal)
is out of scope per challenge constraints.
"""

from __future__ import annotations

import os
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import (
    BranchSQLOperator,
    SQLExecuteQueryOperator,
)
from airflow.utils.trigger_rule import TriggerRule

SQL_ROOT = Path(os.environ["READYDATA_SQL_ROOT"])

GOLD_TABLES = [
    "connectivity_events_gold",
    "throughput_events_gold",
    "device_health_events_gold",
    "connectivity_state_events_gold",
    "infrastructure_events_gold",
]

with DAG(
    dag_id="iceberg_maintenance",
    description="Inspect gold table files and compact only where justified",
    schedule=None,
    catchup=False,
    tags=["maintenance", "iceberg"],
):
    inspect_sql = (SQL_ROOT / "maintenance" / "inspect_files.sql").read_text()
    optimize_sql = (SQL_ROOT / "maintenance" / "optimize.sql").read_text()

    snapshot_sql = """SELECT IF(COUNT(*) > 0, true, false)
FROM iceberg.solution."{{ params.table_name }}$snapshots"
"""

    for table in GOLD_TABLES:
        has_snapshots = BranchSQLOperator(
            task_id=f"has_snapshots__{table}",
            conn_id="trino_default",
            sql=snapshot_sql,
            params={"table_name": table},
            follow_task_ids_if_true=[f"check__{table}"],
            follow_task_ids_if_false=[f"skip__{table}"],
        )

        check = BranchSQLOperator(
            task_id=f"check__{table}",
            conn_id="trino_default",
            sql=inspect_sql,
            params={"table_name": table},
            follow_task_ids_if_true=[f"optimize__{table}"],
            follow_task_ids_if_false=[f"skip__{table}"],
        )

        optimize = SQLExecuteQueryOperator(
            task_id=f"optimize__{table}",
            conn_id="trino_default",
            sql=optimize_sql,
            params={"table_name": table},
        )

        skip = EmptyOperator(task_id=f"skip__{table}")

        done = EmptyOperator(
            task_id=f"done__{table}",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )

        has_snapshots >> [check, skip]
        check >> [optimize, skip]
        [optimize, skip] >> done
