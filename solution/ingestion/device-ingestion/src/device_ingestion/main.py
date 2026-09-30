import os

from device_ingestion.metric_consumer import MetricConsumer


def main() -> None:
    consumer = MetricConsumer(
        bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        group_id=os.environ.get("KAFKA_GROUP_ID", "device-ingestion"),
        catalog_uri=os.environ["CATALOG_URI"],
        warehouse=os.environ["WAREHOUSE"],
        namespace=os.environ.get("NAMESPACE", "solution"),
        registry_url=os.environ["SCHEMA_REGISTRY_URL"],
        batch_size=int(os.environ.get("BATCH_SIZE", "500")),
        poll_timeout=float(os.environ.get("POLL_TIMEOUT", "1.0")),
        flush_interval=float(os.environ.get("FLUSH_INTERVAL", "5.0")),
    )
    consumer.run()


if __name__ == "__main__":
    main()
