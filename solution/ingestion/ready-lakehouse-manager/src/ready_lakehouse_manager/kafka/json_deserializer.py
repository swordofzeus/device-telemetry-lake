import json

import structlog
from confluent_kafka.schema_registry import SchemaRegistryClient
from jsonschema import validate

log = structlog.get_logger()


class JsonDeserializer:
    """Deserializes and validates Kafka messages using JSON Schemas from the Schema Registry.

    Fetches the latest JSON Schema per topic at startup and caches them.
    Each call to deserialize() parses JSON and validates against the
    cached schema. A consumer restart picks up schema changes.
    """

    def __init__(self, registry_url: str, topics: list[str]) -> None:
        self.client = SchemaRegistryClient({"url": registry_url})
        self.schemas: dict[str, dict] = {}

        for topic in topics:
            subject = f"{topic}-value"
            registered = self.client.get_latest_version(subject)
            self.schemas[topic] = json.loads(registered.schema.schema_str)
            log.info("schema_loaded", topic=topic, subject=subject, version=registered.version)

    def deserialize(self, topic: str, value: bytes) -> dict:
        """Deserialize JSON bytes and validate against the cached schema.

        Args:
            topic: The Kafka topic name.
            value: Raw message bytes (plain JSON, no magic bytes).

        Returns:
            The deserialized and validated dict.

        Raises:
            json.JSONDecodeError: If the bytes are not valid JSON.
            jsonschema.ValidationError: If the record fails schema validation.
        """
        record = json.loads(value)
        schema = self.schemas[topic]
        validate(instance=record, schema=schema)
        return record
