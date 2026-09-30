import click

from ready_lakehouse_manager.iceberg.manager import IcebergManager
from ready_lakehouse_manager.kafka.schema_registry import SchemaRegistryManager


@click.group()
@click.version_option()
@click.option("--catalog-uri", required=True, envvar="CATALOG_URI", help="Nessie/Iceberg REST catalog URI")
@click.option("--warehouse", required=True, envvar="WAREHOUSE", help="Iceberg warehouse name")
@click.option("--namespace", required=True, envvar="NAMESPACE", help="Iceberg namespace")
@click.option("--s3-endpoint", required=True, envvar="S3_ENDPOINT", help="S3-compatible endpoint URL")
@click.option("--s3-access-key", required=True, envvar="S3_ACCESS_KEY", help="S3 access key")
@click.option("--s3-secret-key", required=True, envvar="S3_SECRET_KEY", help="S3 secret key")
@click.pass_context
def cli(
    ctx: click.Context,
    catalog_uri: str,
    warehouse: str,
    namespace: str,
    s3_endpoint: str,
    s3_access_key: str,
    s3_secret_key: str,
) -> None:
    """Ready Lakehouse Manager - manage Iceberg tables, schema registry, and reference data."""
    ctx.ensure_object(dict)
    ctx.obj["iceberg"] = IcebergManager(
        catalog_uri=catalog_uri,
        warehouse=warehouse,
        namespace=namespace,
        s3_endpoint=s3_endpoint,
        s3_access_key=s3_access_key,
        s3_secret_key=s3_secret_key,
    )


@cli.command()
@click.pass_context
def create_schema(ctx: click.Context) -> None:
    """Create the Iceberg namespace."""
    manager: IcebergManager = ctx.obj["iceberg"]
    manager.create_schema()


@cli.command()
@click.pass_context
def create_tables(ctx: click.Context) -> None:
    """Create or update the Iceberg namespace and all required tables."""
    manager: IcebergManager = ctx.obj["iceberg"]
    manager.create_tables()


@cli.command()
@click.option("--table", required=True, help="Iceberg table name to load into (e.g. 'sites').")
@click.option("--csv", "csv_uri", required=True, help="Local path or S3 URI to the CSV file.")
@click.pass_context
def load_reference(ctx: click.Context, table: str, csv_uri: str) -> None:
    """Load a CSV file into an Iceberg reference table (idempotent overwrite)."""
    manager: IcebergManager = ctx.obj["iceberg"]
    manager.load_reference_data(table_name=table, csv_uri=csv_uri)


@cli.command()
@click.option("--registry-url", required=True, envvar="SCHEMA_REGISTRY_URL", help="Confluent Schema Registry URL")
@click.option("--topic", required=True, help="Kafka topic name (e.g. 'network.connectivity').")
@click.option("--schema", "schema_uri", required=True, help="Local path or S3 URI to the JSON Schema file.")
def register_schema(registry_url: str, topic: str, schema_uri: str) -> None:
    """Register a JSON Schema for a Kafka topic in the Schema Registry."""
    manager = SchemaRegistryManager(registry_url=registry_url)
    manager.register_schema(topic=topic, schema_uri=schema_uri)


if __name__ == "__main__":
    cli()
