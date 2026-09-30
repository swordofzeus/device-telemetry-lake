import pytest



def test_transform_flattens_payload(parser, throughput_record):
    result = parser.transform(throughput_record)

    assert result["event_id"] == "abc-123"
    assert result["event_type"] == "throughput"
    assert result["download_mbps"] == 100.5
    assert result["upload_mbps"] == 25.3
    assert "payload" not in result


def test_transform_payload_overrides_envelope(parser):
    record = {
        "event_type": "original",
        "payload": {"event_type": "from_payload"},
    }
    result = parser.transform(record)
    assert result["event_type"] == "from_payload"


def test_transform_empty_payload(parser):
    result = parser.transform({"event_id": "abc", "payload": {}})
    assert result == {"event_id": "abc"}


def test_transform_missing_payload_raises(parser):
    with pytest.raises(KeyError):
        parser.transform({"event_id": "abc"})



def test_enrich_adds_kafka_metadata(parser, kafka_message, now):
    msg = kafka_message(topic="network.throughput", partition=3, offset=99)
    record = {"event_id": "abc"}
    result = parser.enrich(record, msg, now)

    assert result["kafka_topic"] == "network.throughput"
    assert result["kafka_partition"] == 3
    assert result["kafka_offset"] == 99
    assert result["kafka_timestamp"].tzinfo is not None
    assert result["ingested_at"] == now


def test_enrich_mutates_original(parser, kafka_message, now):
    record = {"event_id": "abc"}
    result = parser.enrich(record, kafka_message(), now)
    assert result is record
