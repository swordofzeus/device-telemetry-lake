"""
Exports a Trino SQL query result to a CSV file.

Streams results in batches via fetchmany() to avoid loading
unbounded result sets into memory. Null values are written as
empty strings.
"""

from __future__ import annotations

import csv
from pathlib import Path

from airflow.providers.trino.hooks.trino import TrinoHook


class TrinoCsvExporter:
    """Executes a SQL report via Trino and streams the result to a CSV file."""

    def __init__(
        self,
        sql_root: str,
        export_root: str,
        conn_id: str = "trino_default",
        fetch_size: int = 1000,
    ):
        self.conn_id = conn_id
        self.sql_root = Path(sql_root)
        self.export_root = Path(export_root)
        self.fetch_size = fetch_size

    def export(self, report_name: str, run_id: str) -> dict:
        sql = self._read_sql(report_name)
        output_path = self._build_output_path(report_name, run_id)
        total_rows = self._execute_and_write(sql, output_path)
        return {"output_path": str(output_path), "row_count": total_rows}

    def _read_sql(self, report_name: str) -> str:
        sql_path = self.sql_root / "reports" / f"{report_name}.sql"
        if not sql_path.exists():
            raise FileNotFoundError(f"Report not found: {sql_path}")
        return sql_path.read_text()

    def _build_output_path(self, report_name: str, run_id: str) -> Path:
        safe_run_id = run_id.replace(":", "_").replace("+", "_")
        safe_report_name = report_name.replace("/", "_")
        output_path = self.export_root / f"{safe_report_name}_{safe_run_id}.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path

    def _execute_and_write(self, sql: str, output_path: Path) -> int:
        hook = TrinoHook(trino_conn_id=self.conn_id)
        conn = hook.get_conn()
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            return self._stream_to_csv(cursor, columns, output_path)
        finally:
            conn.close()

    def _stream_to_csv(self, cursor, columns: list[str], output_path: Path) -> int:
        tmp_path = output_path.with_suffix(".tmp")
        total_rows = 0
        try:
            with open(tmp_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)
                writer.writerow(columns)

                while True:
                    rows = cursor.fetchmany(self.fetch_size)
                    if not rows:
                        break
                    writer.writerows(
                        [("" if v is None else v) for v in row] for row in rows
                    )
                    total_rows += len(rows)
            tmp_path.rename(output_path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise
        return total_rows
