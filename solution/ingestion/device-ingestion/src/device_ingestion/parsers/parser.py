import abc
from datetime import datetime


class Parser(abc.ABC):
    @abc.abstractmethod
    def transform(self, record: dict) -> dict:
        """Transform a deserialized record into the target schema."""

    @abc.abstractmethod
    def enrich(self, record: dict, msg: object, now: datetime) -> dict:
        """Attach metadata from the Kafka message to the record."""
