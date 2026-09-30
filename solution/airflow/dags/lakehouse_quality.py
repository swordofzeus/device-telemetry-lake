"""
lakehouse_quality DAG

Validates reference completeness, event-to-reference relationships, and
dedup integrity. Writes results to the quality_checks Iceberg table.

Manually triggered. All checks run in parallel.
Accepts lookback_days param (default 7) for event-scoped checks.
"""

from __future__ import annotations

import os
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

SQL_ROOT = Path(os.environ["READYDATA_SQL_ROOT"])

CHECKS = [
    # Reference uniqueness
    ("duplicate_site_ids.sql", "duplicate_site_ids", "failure"),
    ("duplicate_device_ids.sql", "duplicate_device_ids", "failure"),
    ("duplicate_census_geoids.sql", "duplicate_census_geoids", "failure"),
    ("duplicate_dependencies.sql", "duplicate_dependencies", "failure"),
    ("empty_reference_tables.sql", "empty_reference_tables", "failure"),
    # Reference relationships
    ("orphan_devices.sql", "orphan_devices", "failure"),
    ("orphan_dependencies.sql", "orphan_dependencies", "failure"),
    # Event quality
    ("events_unknown_devices.sql", "events_unknown_devices", "failure"),
    ("events_unknown_sites.sql", "events_unknown_sites", "failure"),
    ("dedup_violations.sql", "dedup_violations", "warning"),
    ("rejected_summary.sql", "rejected_summary", "warning"),
]


def build_quality_insert(check_sql: str, check_name: str, check_type: str) -> str:
    return f"""
INSERT INTO iceberg.solution.quality_checks
SELECT
    '{check_name}' AS check_name,
    '{check_type}' AS check_type,
    current_timestamp AS checked_at,
    window_start,
    window_end,
    CAST(violations AS INTEGER) AS violations
FROM ({check_sql})
"""


with DAG(
    dag_id="lakehouse_quality",
    description="Validate reference and event data quality",
    schedule=None,
    catchup=False,
    tags=["quality", "lakehouse"],
    params={
        "lookback_days": Param(
            default=7,
            type="integer",
            description="Number of days of data to check for event-scoped checks",
        ),
    },
):
    for sql_file, check_name, check_type in CHECKS:
        check_sql = (SQL_ROOT / "quality" / sql_file).read_text().strip()
        SQLExecuteQueryOperator(
            task_id=check_name,
            conn_id="trino_default",
            sql=build_quality_insert(check_sql, check_name, check_type),
        )
