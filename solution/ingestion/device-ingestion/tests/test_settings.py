import pytest

from device_ingestion.settings import _ALL_TOPICS, _filter_topics


def test_empty_string_returns_all():
    assert _filter_topics("") == _ALL_TOPICS


def test_whitespace_only_returns_all():
    assert _filter_topics("   ") == _ALL_TOPICS


@pytest.mark.parametrize("csv,expected", [
    ("network.throughput", ["network.throughput"]),
    ("network.throughput,network.connectivity", ["network.throughput", "network.connectivity"]),
    (" network.throughput , network.connectivity ", ["network.throughput", "network.connectivity"]),
])
def test_filters_topics(csv, expected):
    result = _filter_topics(csv)
    assert set(result.keys()) == set(expected)


def test_preserves_config_values():
    result = _filter_topics("network.throughput")
    assert result["network.throughput"]["table"] == "throughput_events"


def test_invalid_topic_raises():
    with pytest.raises(ValueError, match="No valid topics"):
        _filter_topics("network.nonexistent")
