"""
manual_sql_to_csv DAG

Accepts a single report_name parameter, executes it through Trino,
and writes the result as a UTF-8 CSV to solution/exports/.
Manually triggered.
"""

from __future__ import annotations

import os

from airflow.decorators import dag, task
from airflow.models.param import Param

VALID_REPORTS = {
    "4_technology_statistics/technology_throughput",
    "4_technology_statistics/technology_connectivity",
    "5_underperforming_devices/underperforming_vs_peers",
    "6_provider_comparison/advertised_vs_actual",
    "7_geographic_performance_patterns/urban_vs_rural_performance",
    "7_geographic_performance_patterns/distance_vs_performance",
    "9_critical_infra_analysis/critical_infra_connectivity",
    "9_critical_infra_analysis/critical_infra_throughput",
    "9_critical_infra_analysis/critical_infra_device_health",
    "9_critical_infra_analysis/critical_infra_blast_radius",
}


@dag(
    dag_id="manual_sql_to_csv",
    description="Execute one SQL report and export result to CSV",
    schedule=None,
    catchup=False,
    tags=["reports", "export"],
    params={
        "report_name": Param(
            default="",
            type="string",
            description="Report path relative to sql/reports/ without .sql extension "
            "(e.g. '5_underperforming_devices/underperforming_vs_peers')",
        ),
    },
)
def manual_sql_to_csv():

    @task
    def export_report(**context):
        from trino_csv_export import TrinoCsvExporter

        report_name = context["params"]["report_name"]
        if not report_name:
            raise ValueError("report_name parameter is required")
        if report_name not in VALID_REPORTS:
            raise ValueError(
                f"Unknown report: '{report_name}'. "
                f"Valid reports: {sorted(VALID_REPORTS)}"
            )

        exporter = TrinoCsvExporter(
            sql_root=os.environ["READYDATA_SQL_ROOT"],
            export_root=os.environ["READYDATA_EXPORT_ROOT"],
            fetch_size=int(os.environ.get("READYDATA_EXPORT_FETCH_SIZE", "1000")),
        )
        return exporter.export(
            report_name=report_name,
            run_id=context["run_id"],
        )

    export_report()


manual_sql_to_csv()
