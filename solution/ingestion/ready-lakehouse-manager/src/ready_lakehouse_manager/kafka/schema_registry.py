import json

import fsspec
import structlog
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

log = structlog.get_logger()


class SchemaRegistryManager:
    """Registers and updates JSON Schemas in Confluent Schema Registry."""

    def __init__(self, registry_url: str) -> None:
        self.client = SchemaRegistryClient({"url": registry_url})

    def _read_uri(self, uri: str) -> str:
        """Read a file from a local path or S3 URI via fsspec.

        Args:
            uri: Local path or S3 URI.

        Returns:
            The file contents as a string.
        """
        with fsspec.open(uri, mode="r") as f:
            return f.read()

    def _resolve_refs(self, schema_uri: str) -> str:
        """Read a JSON Schema file and inline any $ref references.

        The event schemas use allOf with a $ref to the common envelope.
        This inlines the referenced schema so the registry receives a
        self-contained document. The $ref is resolved relative to the
        schema file's location.

        Args:
            schema_uri: Local path or S3 URI to the schema file.

        Returns:
            A JSON string with $ref replaced by the referenced schema.
        """
        schema_data = json.loads(self._read_uri(schema_uri))

        if "allOf" not in schema_data:
            return json.dumps(schema_data)

        base = schema_uri.rsplit("/", 1)[0]
        resolved_all_of = []
        for entry in schema_data["allOf"]:
            if "$ref" in entry:
                ref_uri = f"{base}/{entry['$ref']}"
                ref_data = json.loads(self._read_uri(ref_uri))
                resolved_all_of.append(ref_data)
            else:
                resolved_all_of.append(entry)

        schema_data["allOf"] = resolved_all_of
        return json.dumps(schema_data)

    def register_schema(self, topic: str, schema_uri: str) -> None:
        """Register a single JSON Schema for a Kafka topic.

        Supports local paths and S3 URIs via fsspec. S3 credentials
        are read from standard AWS environment variables.
        The schema is registered under the subject ``<topic>-value``.
        If the schema already exists and is unchanged, the registry
        returns the existing version.

        Args:
            topic: The Kafka topic name (e.g. "network.connectivity").
            schema_uri: Local path or S3 URI to the JSON Schema file.
        """
        subject = f"{topic}-value"
        schema_str = self._resolve_refs(schema_uri)
        schema = Schema(schema_str, schema_type="JSON")

        schema_id = self.client.register_schema(
            subject_name=subject,
            schema=schema,
        )
        log.info(
            "schema_registered",
            subject=subject,
            schema_id=schema_id,
            uri=schema_uri,
        )
