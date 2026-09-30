import pyarrow as pa

from conftest import FakeTable



def test_write_complete_records(batch_writer, fake_table):
    records = [{"event_id": "a", "value": 1.0, "optional_field": 2.0}]
    batch_writer._write(fake_table, records, "test")

    assert len(fake_table.appended) == 1
    result = fake_table.appended[0]
    assert result.num_rows == 1
    assert result.column_names == ["event_id", "value", "optional_field"]


def test_write_pads_missing_optional_columns(batch_writer, fake_table):
    records = [{"event_id": "a", "value": 1.0}]
    batch_writer._write(fake_table, records, "test")

    result = fake_table.appended[0]
    assert result.column("optional_field")[0].as_py() is None


def test_write_pads_multiple_missing_columns(batch_writer):
    schema = pa.schema([
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("a", pa.float64(), nullable=True),
        pa.field("b", pa.float64(), nullable=True),
        pa.field("c", pa.float64(), nullable=True),
    ])
    table = FakeTable(schema)
    batch_writer._write(table, [{"event_id": "x"}], "test")

    result = table.appended[0]
    assert result.column("a")[0].as_py() is None
    assert result.column("b")[0].as_py() is None
    assert result.column("c")[0].as_py() is None


def test_write_column_ordering_matches_schema(batch_writer, fake_table):
    records = [{"optional_field": 3.0, "value": 1.0, "event_id": "a"}]
    batch_writer._write(fake_table, records, "test")

    result = fake_table.appended[0]
    assert result.column_names == ["event_id", "value", "optional_field"]


def test_write_multiple_records_mixed_optional(batch_writer, fake_table):
    records = [
        {"event_id": "a", "value": 1.0, "optional_field": 2.0},
        {"event_id": "b", "value": 3.0},
    ]
    batch_writer._write(fake_table, records, "test")

    result = fake_table.appended[0]
    assert result.num_rows == 2
    assert result.column("optional_field")[0].as_py() == 2.0
    assert result.column("optional_field")[1].as_py() is None



def test_flush_success(batch_writer):
    batch_writer.queue_success("topic.a", {"event_id": "x", "value": 1.0})
    batch_writer.queue_success("topic.a", {"event_id": "y", "value": 2.0})

    assert batch_writer.success_count == 2
    success, rejected, failures = batch_writer.flush()
    assert success == 2
    assert rejected == 0
    assert failures == []
    assert batch_writer.success_count == 0


def test_flush_rejected(batch_writer):
    batch_writer.queue_rejected(
        "topic.a", b'{"bad": "data"}', "validation error",
        partition=0, offset=10, timestamp=None,
    )

    assert batch_writer.rejected_count == 1
    success, rejected, failures = batch_writer.flush()
    assert success == 0
    assert rejected == 1
    assert batch_writer.rejected_count == 0


def test_queue_rejected_base64_encodes_raw_bytes(batch_writer):
    raw = b'\x80\x81\x82 not valid utf-8'
    batch_writer.queue_rejected(
        "topic.a", raw, "parse error",
        partition=3, offset=99, timestamp=None,
    )

    import base64
    entry = batch_writer._rejected_queue[0]
    assert entry["raw_value"] == base64.b64encode(raw).decode("ascii")
    assert entry["kafka_partition"] == 3
    assert entry["kafka_offset"] == 99
    assert entry["kafka_timestamp"] is None


def test_queue_rejected_stores_kafka_timestamp(batch_writer):
    from datetime import datetime, timezone
    ts = datetime(2025, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
    batch_writer.queue_rejected(
        "topic.a", b'{"x": 1}', "bad schema",
        partition=1, offset=50, timestamp=ts,
    )

    assert batch_writer._rejected_queue[0]["kafka_timestamp"] == ts


def test_flush_returns_failures_on_write_error(batch_writer):
    batch_writer.tables["table_a"].append = lambda t: (_ for _ in ()).throw(RuntimeError("S3 down"))
    batch_writer.queue_success("topic.a", {"event_id": "x", "value": 1.0})

    success, rejected, failures = batch_writer.flush()
    assert len(failures) == 1
    table_name, records, error = failures[0]
    assert table_name == "table_a"
    assert len(records) == 1
    assert "S3 down" in str(error)


def test_flush_rejected_write_failure_returns_in_failures(batch_writer):
    """A failed rejected_events write must appear in failures, not be silently dropped."""
    batch_writer.rejected_table.append = lambda t: (_ for _ in ()).throw(RuntimeError("catalog down"))
    batch_writer.queue_rejected(
        "topic.a", b'{"bad": true}', "schema mismatch",
        partition=0, offset=5, timestamp=None,
    )

    success, rejected, failures = batch_writer.flush()
    assert rejected == 1
    assert len(failures) == 1
    table_name, records, error = failures[0]
    assert table_name == "rejected_events"
    assert "catalog down" in str(error)
    # Queue should be cleared even on failure (records are in failures list)
    assert batch_writer.rejected_count == 0
