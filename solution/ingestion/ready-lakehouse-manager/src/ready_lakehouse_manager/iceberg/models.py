from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import NullOrder, SortDirection, SortField, SortOrder
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

PARQUET_PROPERTIES = {
    "format-version": "2",
    "write.parquet.compression-codec": "zstd",
}


class Sites:
    TABLE_NAME = "sites"
    SCHEMA = Schema(
        NestedField(field_id=1, name="site_id", field_type=StringType(), required=True),
        NestedField(field_id=2, name="latitude", field_type=DoubleType(), required=True),
        NestedField(field_id=3, name="longitude", field_type=DoubleType(), required=True),
        NestedField(field_id=4, name="region", field_type=StringType(), required=True),
        NestedField(field_id=5, name="block_group_geoid", field_type=StringType(), required=True),
        NestedField(field_id=6, name="inside_census_urban_area", field_type=BooleanType(), required=True),
        NestedField(field_id=7, name="urban_area_name", field_type=StringType(), required=False),
        NestedField(field_id=8, name="distance_to_nearest_urban_area_km", field_type=DoubleType(), required=True),
        NestedField(field_id=9, name="nearest_census_place", field_type=StringType(), required=False),
        NestedField(field_id=10, name="distance_to_nearest_place_km", field_type=DoubleType(), required=True),
        NestedField(field_id=11, name="site_type", field_type=StringType(), required=True),
        NestedField(field_id=12, name="critical_infrastructure", field_type=BooleanType(), required=True),
    )
    PROPERTIES = PARQUET_PROPERTIES


class Devices:
    TABLE_NAME = "devices"
    SCHEMA = Schema(
        NestedField(field_id=1, name="device_id", field_type=StringType(), required=True),
        NestedField(field_id=2, name="site_id", field_type=StringType(), required=True),
        NestedField(field_id=3, name="provider_id", field_type=StringType(), required=True),
        NestedField(field_id=4, name="provider_name", field_type=StringType(), required=True),
        NestedField(field_id=5, name="technology", field_type=StringType(), required=True),
        NestedField(field_id=6, name="device_class", field_type=StringType(), required=True),
        NestedField(field_id=7, name="device_model", field_type=StringType(), required=True),
        NestedField(field_id=8, name="infrastructure_role", field_type=StringType(), required=True),
        NestedField(field_id=9, name="advertised_download_mbps", field_type=DoubleType(), required=False),
        NestedField(field_id=10, name="advertised_upload_mbps", field_type=DoubleType(), required=False),
        NestedField(field_id=11, name="deployment_date", field_type=StringType(), required=True),
    )
    PROPERTIES = PARQUET_PROPERTIES


class DeviceDependencies:
    TABLE_NAME = "device_dependencies"
    SCHEMA = Schema(
        NestedField(field_id=1, name="downstream_device_id", field_type=StringType(), required=True),
        NestedField(field_id=2, name="upstream_device_id", field_type=StringType(), required=True),
        NestedField(field_id=3, name="dependency_type", field_type=StringType(), required=True),
    )
    PROPERTIES = PARQUET_PROPERTIES


class CensusBlockGroups:
    TABLE_NAME = "census_block_groups"
    SCHEMA = Schema(
        NestedField(field_id=1, name="block_group_geoid", field_type=StringType(), required=True),
        NestedField(field_id=2, name="population", field_type=IntegerType(), required=True),
        NestedField(field_id=3, name="land_area_sq_miles", field_type=DoubleType(), required=True),
        NestedField(field_id=4, name="population_density_per_sq_mile", field_type=DoubleType(), required=True),
        NestedField(field_id=5, name="internal_latitude", field_type=DoubleType(), required=True),
        NestedField(field_id=6, name="internal_longitude", field_type=DoubleType(), required=True),
    )
    PROPERTIES = PARQUET_PROPERTIES


# Kafka source lineage fields appended to every event table
_LINEAGE_FIELDS_START_ID = 100
_LINEAGE_FIELDS = [
    NestedField(field_id=_LINEAGE_FIELDS_START_ID, name="kafka_topic", field_type=StringType(), required=True),
    NestedField(field_id=_LINEAGE_FIELDS_START_ID + 1, name="kafka_partition", field_type=IntegerType(), required=True),
    NestedField(field_id=_LINEAGE_FIELDS_START_ID + 2, name="kafka_offset", field_type=LongType(), required=True),
    NestedField(field_id=_LINEAGE_FIELDS_START_ID + 3, name="kafka_timestamp", field_type=TimestamptzType(), required=True),
    NestedField(field_id=_LINEAGE_FIELDS_START_ID + 4, name="ingested_at", field_type=TimestamptzType(), required=True),
]

# Common envelope fields shared by all event tables
_ENVELOPE_FIELDS = [
    NestedField(field_id=1, name="event_id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="event_type", field_type=StringType(), required=True),
    NestedField(field_id=3, name="event_timestamp", field_type=TimestamptzType(), required=True),
    NestedField(field_id=4, name="device_session_id", field_type=StringType(), required=True),
    NestedField(field_id=5, name="sequence_number", field_type=IntegerType(), required=True),
    NestedField(field_id=6, name="site_id", field_type=StringType(), required=True),
    NestedField(field_id=7, name="device_id", field_type=StringType(), required=True),
    NestedField(field_id=8, name="schema_version", field_type=IntegerType(), required=True),
]

_EVENT_PARTITION_SPEC = PartitionSpec(
    PartitionField(source_id=3, field_id=1000, transform=DayTransform(), name="event_day"),
)

_EVENT_SORT_ORDER = SortOrder(
    SortField(source_id=7, transform=IdentityTransform(), direction=SortDirection.ASC, null_order=NullOrder.NULLS_LAST),
)


class ConnectivityEvents:
    TABLE_NAME = "connectivity_events"
    SCHEMA = Schema(
        *_ENVELOPE_FIELDS,
        NestedField(field_id=20, name="latency_ms", field_type=DoubleType(), required=True),
        NestedField(field_id=21, name="jitter_ms", field_type=DoubleType(), required=True),
        NestedField(field_id=22, name="packet_loss_pct", field_type=DoubleType(), required=True),
        *_LINEAGE_FIELDS,
    )
    PARTITION_SPEC = _EVENT_PARTITION_SPEC
    SORT_ORDER = _EVENT_SORT_ORDER
    PROPERTIES = PARQUET_PROPERTIES


class ThroughputEvents:
    TABLE_NAME = "throughput_events"
    SCHEMA = Schema(
        *_ENVELOPE_FIELDS,
        NestedField(field_id=20, name="download_mbps", field_type=DoubleType(), required=True),
        NestedField(field_id=21, name="upload_mbps", field_type=DoubleType(), required=True),
        *_LINEAGE_FIELDS,
    )
    PARTITION_SPEC = _EVENT_PARTITION_SPEC
    SORT_ORDER = _EVENT_SORT_ORDER
    PROPERTIES = PARQUET_PROPERTIES


class DeviceHealthEvents:
    TABLE_NAME = "device_health_events"
    SCHEMA = Schema(
        *_ENVELOPE_FIELDS,
        NestedField(field_id=20, name="cpu_pct", field_type=DoubleType(), required=True),
        NestedField(field_id=21, name="memory_pct", field_type=DoubleType(), required=True),
        NestedField(field_id=22, name="temperature_c", field_type=DoubleType(), required=True),
        NestedField(field_id=23, name="optical_rx_power_dbm", field_type=DoubleType(), required=False),
        NestedField(field_id=24, name="snr_db", field_type=DoubleType(), required=False),
        NestedField(field_id=25, name="signal_strength_dbm", field_type=DoubleType(), required=False),
        NestedField(field_id=26, name="signal_quality_pct", field_type=DoubleType(), required=False),
        *_LINEAGE_FIELDS,
    )
    PARTITION_SPEC = _EVENT_PARTITION_SPEC
    SORT_ORDER = _EVENT_SORT_ORDER
    PROPERTIES = PARQUET_PROPERTIES


class ConnectivityStateEvents:
    TABLE_NAME = "connectivity_state_events"
    SCHEMA = Schema(
        *_ENVELOPE_FIELDS,
        NestedField(field_id=20, name="previous_state", field_type=StringType(), required=True),
        NestedField(field_id=21, name="new_state", field_type=StringType(), required=True),
        NestedField(field_id=22, name="reason", field_type=StringType(), required=True),
        *_LINEAGE_FIELDS,
    )
    PARTITION_SPEC = _EVENT_PARTITION_SPEC
    SORT_ORDER = _EVENT_SORT_ORDER
    PROPERTIES = PARQUET_PROPERTIES


class InfrastructureEvents:
    TABLE_NAME = "infrastructure_events"
    SCHEMA = Schema(
        *_ENVELOPE_FIELDS,
        NestedField(field_id=20, name="incident_id", field_type=StringType(), required=True),
        NestedField(field_id=21, name="event_kind", field_type=StringType(), required=True),
        NestedField(field_id=22, name="severity", field_type=StringType(), required=True),
        *_LINEAGE_FIELDS,
    )
    PARTITION_SPEC = _EVENT_PARTITION_SPEC
    SORT_ORDER = _EVENT_SORT_ORDER
    PROPERTIES = PARQUET_PROPERTIES


class RejectedEvents:
    TABLE_NAME = "rejected_events"
    SCHEMA = Schema(
        NestedField(field_id=1, name="rejected_at", field_type=TimestamptzType(), required=True),
        NestedField(field_id=2, name="reason", field_type=StringType(), required=True),
        NestedField(field_id=3, name="raw_value", field_type=StringType(), required=True),
        NestedField(field_id=4, name="kafka_topic", field_type=StringType(), required=True),
        NestedField(field_id=5, name="kafka_partition", field_type=IntegerType(), required=True),
        NestedField(field_id=6, name="kafka_offset", field_type=LongType(), required=True),
        NestedField(field_id=7, name="kafka_timestamp", field_type=TimestamptzType(), required=False),
    )
    PARTITION_SPEC = PartitionSpec(
        PartitionField(source_id=1, field_id=1000, transform=DayTransform(), name="rejected_day"),
    )
    PROPERTIES = PARQUET_PROPERTIES


class QualityChecks:
    TABLE_NAME = "quality_checks"
    SCHEMA = Schema(
        NestedField(field_id=1, name="check_name", field_type=StringType(), required=True),
        NestedField(field_id=2, name="check_type", field_type=StringType(), required=True),
        NestedField(field_id=3, name="checked_at", field_type=TimestamptzType(), required=True),
        NestedField(field_id=4, name="window_start", field_type=TimestamptzType(), required=False),
        NestedField(field_id=5, name="window_end", field_type=TimestamptzType(), required=False),
        NestedField(field_id=6, name="violations", field_type=IntegerType(), required=True),
    )
    PARTITION_SPEC = PartitionSpec(
        PartitionField(source_id=3, field_id=1000, transform=DayTransform(), name="checked_day"),
    )
    PROPERTIES = PARQUET_PROPERTIES


def _gold(event_cls):
    """Create a gold-layer copy of an event table class."""
    return type(
        f"{event_cls.__name__}Gold",
        (),
        {
            "TABLE_NAME": f"{event_cls.TABLE_NAME}_gold",
            "SCHEMA": event_cls.SCHEMA,
            "PARTITION_SPEC": event_cls.PARTITION_SPEC,
            "SORT_ORDER": event_cls.SORT_ORDER,
            "PROPERTIES": event_cls.PROPERTIES,
        },
    )


ConnectivityEventsGold = _gold(ConnectivityEvents)
ThroughputEventsGold = _gold(ThroughputEvents)
DeviceHealthEventsGold = _gold(DeviceHealthEvents)
ConnectivityStateEventsGold = _gold(ConnectivityStateEvents)
InfrastructureEventsGold = _gold(InfrastructureEvents)

_EVENT_MODELS = [
    ConnectivityEvents, ThroughputEvents, DeviceHealthEvents,
    ConnectivityStateEvents, InfrastructureEvents,
]

_GOLD_MODELS = [
    ConnectivityEventsGold, ThroughputEventsGold, DeviceHealthEventsGold,
    ConnectivityStateEventsGold, InfrastructureEventsGold,
]

# All models for iteration
ALL_MODELS = [
    Sites, Devices, DeviceDependencies, CensusBlockGroups,
    *_EVENT_MODELS,
    RejectedEvents,
    QualityChecks,
    *_GOLD_MODELS,
]
