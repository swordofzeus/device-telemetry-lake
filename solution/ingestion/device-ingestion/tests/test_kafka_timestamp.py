from datetime import datetime, timezone
from unittest.mock import Mock

from device_ingestion.metric_consumer import MetricConsumer


def test_kafka_timestamp_valid():
    msg = Mock()
    msg.timestamp.return_value = (1, 1700000000000)

    result = MetricConsumer._kafka_timestamp(msg)
    assert result == datetime(2023, 11, 14, 22, 13, 20, tzinfo=timezone.utc)


def test_kafka_timestamp_type_zero_returns_none():
    msg = Mock()
    msg.timestamp.return_value = (0, 1700000000000)

    assert MetricConsumer._kafka_timestamp(msg) is None


def test_kafka_timestamp_none_value_returns_none():
    msg = Mock()
    msg.timestamp.return_value = (0, None)

    assert MetricConsumer._kafka_timestamp(msg) is None
