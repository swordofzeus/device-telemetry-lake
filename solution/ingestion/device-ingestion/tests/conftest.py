from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from device_ingestion.parsers.flatten_record import FlattenRecord


@pytest.fixture
def parser():
    return FlattenRecord()


@pytest.fixture
def now():
    return datetime(2025, 1, 15, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def kafka_message():
    def _make(topic="network.throughput", partition=0, offset=42, timestamp_ms=1700000000000):
        msg = Mock()
        msg.topic.return_value = topic
        msg.partition.return_value = partition
        msg.offset.return_value = offset
        msg.timestamp.return_value = (1, timestamp_ms)
        return msg
    return _make


@pytest.fixture
def throughput_record():
    return {
        "event_id": "abc-123",
        "event_type": "throughput",
        "payload": {"download_mbps": 100.5, "upload_mbps": 25.3},
    }
