import os

from device_ingestion.parsers.flatten_record import FlattenRecord

_flatten = FlattenRecord()

_ALL_TOPICS: dict[str, dict] = {
    "network.throughput": {"table": "throughput_events", "parser": _flatten},
    "network.connectivity": {"table": "connectivity_events", "parser": _flatten},
    "network.device_health": {"table": "device_health_events", "parser": _flatten},
    "network.state": {"table": "connectivity_state_events", "parser": _flatten},
    "network.infrastructure": {"table": "infrastructure_events", "parser": _flatten},
}


def _filter_topics(topics_csv: str = "") -> dict[str, dict]:
    """Filter topics by a comma-separated string. Empty string returns all topics."""
    if not topics_csv.strip():
        return _ALL_TOPICS
    requested = [t.strip() for t in topics_csv.split(",")]
    filtered = {t: cfg for t, cfg in _ALL_TOPICS.items() if t in requested}
    if not filtered:
        raise ValueError(f"No valid topics in {topics_csv!r}. Available: {list(_ALL_TOPICS.keys())}")
    return filtered


TOPIC_CONFIG: dict[str, dict] = _filter_topics(os.environ.get("KAFKA_TOPICS", ""))
