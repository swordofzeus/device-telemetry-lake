import base64
import json
from datetime import datetime, timezone

import pyarrow as pa
import structlog
from pyiceberg.catalog import Catalog

log = structlog.get_logger()


class BatchWriter:
    """Queues and flushes parsed and rejected records to Iceberg tables."""

    def __init__(self, catalog: Catalog, namespace: str, topic_table_map: dict[str, str]) -> None:
        self.topic_table_map = topic_table_map
        self.tables = {}
        for table_name in set(topic_table_map.values()):
            self.tables[table_name] = catalog.load_table(f"{namespace}.{table_name}")

        self.rejected_table = catalog.load_table(f"{namespace}.rejected_events")

        self._success_queue: dict[str, list[dict]] = {}
        self._rejected_queue: list[dict] = []

    @property
    def success_count(self) -> int:
        return sum(len(records) for records in self._success_queue.values())

    @property
    def rejected_count(self) -> int:
        return len(self._rejected_queue)

    def queue_success(self, topic: str, record: dict) -> None:
        table_name = self.topic_table_map[topic]
        self._success_queue.setdefault(table_name, []).append(record)

    def queue_rejected(
        self,
        topic: str,
        raw_bytes: bytes,
        reason: str,
        partition: int,
        offset: int,
        timestamp: datetime | None,
    ) -> None:
        self._rejected_queue.append({
            "rejected_at": datetime.now(timezone.utc),
            "reason": reason,
            "raw_value": base64.b64encode(raw_bytes).decode("ascii"),
            "kafka_topic": topic,
            "kafka_partition": partition,
            "kafka_offset": offset,
            "kafka_timestamp": timestamp,
        })

    def flush(self) -> tuple[int, int, list[tuple[str, list[dict], Exception]]]:
        """Flush queued records to Iceberg.

        Returns:
            (success_count, rejected_count, failures) where failures is a list
            of (table_name, records, exception) for any writes that failed.
        """
        success = self.success_count
        rejected = self.rejected_count
        failures: list[tuple[str, list[dict], Exception]] = []

        for table_name, records in self._success_queue.items():
            try:
                self._write(self.tables[table_name], records, table_name)
            except Exception as e:
                log.error("iceberg_write_failed", table=table_name, rows=len(records), error=str(e))
                failures.append((table_name, records, e))

        self._success_queue.clear()

        if self._rejected_queue:
            try:
                self._write(self.rejected_table, self._rejected_queue, "rejected_events")
                self._rejected_queue.clear()
            except Exception as e:
                log.error("iceberg_write_failed", table="rejected_events", rows=len(self._rejected_queue), error=str(e))
                failures.append(("rejected_events", self._rejected_queue.copy(), e))
                self._rejected_queue.clear()

        return success, rejected, failures

    def _write(self, table: object, records: list[dict], table_name: str) -> None:
        arrow_schema = table.schema().as_arrow()
        at = pa.Table.from_pylist(records)
        for field in arrow_schema:
            if field.name not in at.column_names:
                at = at.append_column(field.name, pa.nulls(len(at), type=field.type))
        at = at.select(arrow_schema.names).cast(arrow_schema)
        table.append(at)
