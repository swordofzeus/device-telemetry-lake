from datetime import datetime, timezone

from device_ingestion.parsers.parser import Parser


class FlattenRecord(Parser):
    """Flattens a Kafka record by spreading the payload into the top level."""

    def transform(self, record: dict) -> dict:
        return {
            **{k: v for k, v in record.items() if k != "payload"},
            **record["payload"],
        }

    def enrich(self, record: dict, msg: object, now: datetime) -> dict:
        record["kafka_topic"] = msg.topic()
        record["kafka_partition"] = msg.partition()
        record["kafka_offset"] = msg.offset()
        record["kafka_timestamp"] = datetime.fromtimestamp(msg.timestamp()[1] / 1000, tz=timezone.utc)
        record["ingested_at"] = now
        return record
