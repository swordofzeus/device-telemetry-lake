import fsspec
import pyarrow.csv as pcsv
import structlog
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, TableAlreadyExistsError
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER

from ready_lakehouse_manager.iceberg.models import ALL_MODELS

log = structlog.get_logger()


class IcebergManager:
    """Manages Iceberg namespace and table lifecycle via PyIceberg."""

    def __init__(
        self,
        catalog_uri: str,
        warehouse: str,
        namespace: str,
        s3_endpoint: str,
        s3_access_key: str,
        s3_secret_key: str,
    ) -> None:
        self.namespace = namespace
        self.catalog = load_catalog(
            "nessie",
            **{
                "type": "rest",
                "uri": catalog_uri,
                "warehouse": warehouse,
                "s3.endpoint": s3_endpoint,
                "s3.access-key-id": s3_access_key,
                "s3.secret-access-key": s3_secret_key,
                "s3.path-style-access": "true",
            },
        )

    def create_schema(self) -> None:
        """Create the Iceberg namespace if it does not exist."""
        try:
            self.catalog.create_namespace(self.namespace)
            log.info("namespace_created", namespace=self.namespace)
        except NamespaceAlreadyExistsError:
            log.info("namespace_exists", namespace=self.namespace)

    def create_tables(self) -> None:
        """Create or update the Iceberg namespace and all required tables."""
        self.create_schema()
        for model in ALL_MODELS:
            table_id = f"{self.namespace}.{model.TABLE_NAME}"
            partition_spec = getattr(model, "PARTITION_SPEC", UNPARTITIONED_PARTITION_SPEC)
            sort_order = getattr(model, "SORT_ORDER", UNSORTED_SORT_ORDER)
            try:
                self.catalog.create_table(
                    identifier=table_id,
                    schema=model.SCHEMA,
                    partition_spec=partition_spec,
                    sort_order=sort_order,
                    properties=model.PROPERTIES,
                )
                log.info("table_created", table=table_id)
            except TableAlreadyExistsError:
                self._update_table(table_id, model)

    def _update_table(self, table_id: str, model: type) -> None:
        """Update an existing table's schema and properties to match the model."""
        table = self.catalog.load_table(table_id)

        schema_before = table.schema()
        with table.update_schema() as update_schema:
            update_schema.union_by_name(model.SCHEMA)
        schema_after = table.schema()

        if schema_before != schema_after:
            log.info("table_schema_updated", table=table_id)
        else:
            log.debug("table_schema_unchanged", table=table_id)

        props_before = dict(table.properties)
        with table.transaction() as transaction:
            transaction.remove_properties(*table.properties.keys())
            transaction.set_properties(**model.PROPERTIES)

        table = self.catalog.load_table(table_id)
        if dict(table.properties) != props_before:
            log.info("table_properties_updated", table=table_id)
        else:
            log.debug("table_properties_unchanged", table=table_id)

    def load_reference_data(self, table_name: str, csv_uri: str) -> None:
        """Load a CSV file into an Iceberg reference table.

        Supports local paths and S3 URIs via fsspec. S3 credentials
        are read from standard AWS environment variables.
        Performs an idempotent overwrite so the table always reflects
        the current contents of the source file.

        Args:
            table_name: The Iceberg table name (e.g. "sites").
            csv_uri: Local path or S3 URI to the CSV file.
        """
        table_id = f"{self.namespace}.{table_name}"

        table = self.catalog.load_table(table_id)
        iceberg_schema = table.schema()
        arrow_schema = iceberg_schema.as_arrow()

        with fsspec.open(csv_uri, mode="rb") as f:
            arrow_table = pcsv.read_csv(
                f,
                convert_options=pcsv.ConvertOptions(column_types=arrow_schema),
            )
        arrow_table = arrow_table.cast(arrow_schema)

        row_count = arrow_table.num_rows
        log.info("csv_loaded", table=table_id, uri=csv_uri, rows=row_count)

        table.overwrite(arrow_table)
        log.info("table_overwritten", table=table_id, rows=row_count)
