import json
from unittest.mock import Mock

import pyarrow as pa
import pytest

from ready_lakehouse_manager.iceberg.batch_writer import BatchWriter
from ready_lakehouse_manager.kafka.json_deserializer import JsonDeserializer


SAMPLE_JSON_SCHEMA = {
    "type": "object",
    "properties": {
        "event_id": {"type": "string"},
        "value": {"type": "number"},
    },
    "required": ["event_id", "value"],
}

EVENT_ARROW_SCHEMA = pa.schema([
    pa.field("event_id", pa.string(), nullable=False),
    pa.field("value", pa.float64(), nullable=False),
    pa.field("optional_field", pa.float64(), nullable=True),
])

REJECTED_ARROW_SCHEMA = pa.schema([
    pa.field("reason", pa.string()),
    pa.field("raw_value", pa.string()),
    pa.field("kafka_topic", pa.string()),
    pa.field("kafka_partition", pa.int32()),
    pa.field("kafka_offset", pa.int64()),
    pa.field("rejected_at", pa.timestamp("us", tz="UTC")),
    pa.field("kafka_timestamp", pa.timestamp("us", tz="UTC"), nullable=True),
])


class FakeTable:
    """Minimal stand-in for a PyIceberg table."""

    def __init__(self, arrow_schema):
        self._arrow_schema = arrow_schema
        self.appended = []

    def schema(self):
        mock_schema = Mock()
        mock_schema.as_arrow.return_value = self._arrow_schema
        return mock_schema

    def append(self, table):
        self.appended.append(table)


@pytest.fixture
def event_schema():
    return EVENT_ARROW_SCHEMA


@pytest.fixture
def fake_table(event_schema):
    return FakeTable(event_schema)


@pytest.fixture
def batch_writer(fake_table):
    writer = BatchWriter.__new__(BatchWriter)
    writer.topic_table_map = {"topic.a": "table_a"}
    writer.tables = {"table_a": fake_table}
    writer.rejected_table = FakeTable(REJECTED_ARROW_SCHEMA)
    writer._success_queue = {}
    writer._rejected_queue = []
    return writer


@pytest.fixture
def deserializer():
    deser = JsonDeserializer.__new__(JsonDeserializer)
    deser.client = Mock()
    deser.schemas = {"test.topic": SAMPLE_JSON_SCHEMA}
    return deser
