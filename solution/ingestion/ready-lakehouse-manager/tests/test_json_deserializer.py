import json

import pytest
from jsonschema import ValidationError


def test_deserialize_valid_message(deserializer):
    record = {"event_id": "abc-123", "value": 42.0}
    result = deserializer.deserialize("test.topic", json.dumps(record).encode())
    assert result == record


def test_deserialize_invalid_json_raises(deserializer):
    with pytest.raises(json.JSONDecodeError):
        deserializer.deserialize("test.topic", b"not valid json{{{")


def test_deserialize_missing_field_raises(deserializer):
    record = {"event_id": "abc-123"}
    with pytest.raises(ValidationError):
        deserializer.deserialize("test.topic", json.dumps(record).encode())


def test_deserialize_wrong_type_raises(deserializer):
    record = {"event_id": "abc-123", "value": "not a number"}
    with pytest.raises(ValidationError):
        deserializer.deserialize("test.topic", json.dumps(record).encode())


def test_deserialize_extra_fields_pass(deserializer):
    record = {"event_id": "abc-123", "value": 42.0, "extra": "allowed"}
    result = deserializer.deserialize("test.topic", json.dumps(record).encode())
    assert result["extra"] == "allowed"
