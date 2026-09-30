import json
import time
from datetime import datetime, timezone

import structlog
from confluent_kafka import Consumer, KafkaError, Message, Producer
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from pyiceberg.catalog import load_catalog

from ready_lakehouse_manager.iceberg.batch_writer import BatchWriter
from ready_lakehouse_manager.kafka.json_deserializer import JsonDeserializer

from device_ingestion.settings import TOPIC_CONFIG

tracer = trace.get_tracer("kafka.consumer")
log = structlog.get_logger()


class MetricConsumer:
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        catalog_uri: str,
        warehouse: str,
        namespace: str,
        registry_url: str,
        batch_size: int = 500,
        poll_timeout: float = 1.0,
        flush_interval: float = 5.0,
    ) -> None:
        self.batch_size = batch_size
        self.poll_timeout = poll_timeout
        self.flush_interval = flush_interval
        self._last_flush = time.monotonic()

        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })

        self.dlq_producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.dlq_topic = "network.dlq"

        self.deserializer = JsonDeserializer(registry_url, topics=list(TOPIC_CONFIG.keys()))

        catalog = load_catalog(
            "nessie",
            **{
                "type": "rest",
                "uri": catalog_uri,
                "warehouse": warehouse,
            },
        )

        topic_table_map = {topic: cfg["table"] for topic, cfg in TOPIC_CONFIG.items()}
        self.writer = BatchWriter(catalog, namespace, topic_table_map)

        self.consumer.subscribe(list(TOPIC_CONFIG.keys()))
        log.info(
            "consumer_started",
            topics=list(TOPIC_CONFIG.keys()),
            group_id=group_id,
            batch_size=batch_size,
            flush_interval=flush_interval,
        )

    def run(self) -> None:
        try:
            while True:
                self._poll_batch()
        except KeyboardInterrupt:
            log.info("consumer_shutting_down")
        finally:
            self.consumer.close()

    def _poll_batch(self) -> None:
        messages = self.consumer.consume(num_messages=self.batch_size, timeout=self.poll_timeout)
        if not messages:
            self._maybe_flush()
            return

        now = datetime.now(timezone.utc)

        with tracer.start_as_current_span(
            name="kafka.consume.batch",
            kind=trace.SpanKind.CONSUMER,
        ) as span:
            span.set_attribute("kafka.batch.size", len(messages))

            for msg in messages:
                if self._is_system_message(msg):
                    continue

                topic = msg.topic()
                config = TOPIC_CONFIG[topic]

                try:
                    record = self.deserializer.deserialize(topic, msg.value())
                    record = config["parser"].enrich(record, msg, now)
                    parsed = config["parser"].transform(record)
                    self.writer.queue_success(topic, parsed)
                except Exception as e:
                    self.writer.queue_rejected(
                        topic, msg.value(), str(e),
                        msg.partition(), msg.offset(), self._kafka_timestamp(msg),
                    )

            self._set_span_status(span)

        self._flush()

    def _maybe_flush(self) -> None:
        """Flush if the flush interval has elapsed (handles low-throughput periods)."""
        if self.writer.success_count or self.writer.rejected_count:
            if time.monotonic() - self._last_flush >= self.flush_interval:
                self._flush()

    def _flush(self) -> None:
        success, rejected, failures = self.writer.flush()
        if failures:
            self._send_to_dlq(failures)
            log.warning("batch_incomplete", parsed=success, rejected=rejected, dlq=len(failures))
            return
        if success or rejected:
            self.consumer.commit()
            self._last_flush = time.monotonic()
            log.info("batch_complete", total=success + rejected, parsed=success, rejected=rejected)

    def _send_to_dlq(self, failures: list[tuple[str, list[dict], Exception]]) -> None:
        delivery_errors: list[str] = []

        def on_delivery(err, _msg):
            if err:
                delivery_errors.append(str(err))

        for table_name, records, error in failures:
            msg = json.dumps({
                "table": table_name,
                "error": str(error),
                "record_count": len(records),
                "records": records,
            }, default=str)
            self.dlq_producer.produce(self.dlq_topic, value=msg.encode(), callback=on_delivery)
            log.warning("sent_to_dlq", table=table_name, records=len(records), error=str(error))
        self.dlq_producer.flush()

        if delivery_errors:
            raise RuntimeError(f"DLQ delivery failed for {len(delivery_errors)} message(s): {delivery_errors}")

    def _is_system_message(self, msg: Message) -> bool:
        if not msg.error():
            return False
        if msg.error().code() == KafkaError._PARTITION_EOF:
            log.debug("partition_eof", topic=msg.topic(), partition=msg.partition())
        else:
            log.error("kafka_error", error=msg.error())
        return True

    @staticmethod
    def _kafka_timestamp(msg: Message) -> datetime | None:
        ts_type, ts_val = msg.timestamp()
        if ts_type == 0 or ts_val is None:
            return None
        return datetime.fromtimestamp(ts_val / 1000, tz=timezone.utc)

    def _set_span_status(self, span: trace.Span) -> None:
        span.set_attribute("kafka.batch.parsed", self.writer.success_count)
        span.set_attribute("kafka.batch.rejected", self.writer.rejected_count)

        if self.writer.rejected_count and not self.writer.success_count:
            span.set_status(Status(StatusCode.ERROR, "Entire batch failed."))
        elif self.writer.rejected_count:
            span.set_status(Status(StatusCode.ERROR, f"Partial failure: {self.writer.rejected_count} errors."))
        else:
            span.set_status(Status(StatusCode.OK))

