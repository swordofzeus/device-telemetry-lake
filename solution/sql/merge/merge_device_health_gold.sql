MERGE INTO iceberg.solution.device_health_events_gold AS g
USING (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY kafka_offset DESC
    ) AS rn
    FROM iceberg.solution.device_health_events
    WHERE ingested_at > current_timestamp - INTERVAL '10' MINUTE
) AS b ON g.event_id = b.event_id
WHEN MATCHED AND b.rn = 1 THEN UPDATE SET
    event_type = b.event_type,
    event_timestamp = b.event_timestamp,
    device_session_id = b.device_session_id,
    sequence_number = b.sequence_number,
    site_id = b.site_id,
    device_id = b.device_id,
    schema_version = b.schema_version,
    cpu_pct = b.cpu_pct,
    memory_pct = b.memory_pct,
    temperature_c = b.temperature_c,
    optical_rx_power_dbm = b.optical_rx_power_dbm,
    snr_db = b.snr_db,
    signal_strength_dbm = b.signal_strength_dbm,
    signal_quality_pct = b.signal_quality_pct,
    kafka_topic = b.kafka_topic,
    kafka_partition = b.kafka_partition,
    kafka_offset = b.kafka_offset,
    kafka_timestamp = b.kafka_timestamp,
    ingested_at = b.ingested_at
WHEN NOT MATCHED AND b.rn = 1 THEN INSERT (
    event_id, event_type, event_timestamp, device_session_id,
    sequence_number, site_id, device_id, schema_version,
    cpu_pct, memory_pct, temperature_c,
    optical_rx_power_dbm, snr_db, signal_strength_dbm, signal_quality_pct,
    kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, ingested_at
) VALUES (
    b.event_id, b.event_type, b.event_timestamp, b.device_session_id,
    b.sequence_number, b.site_id, b.device_id, b.schema_version,
    b.cpu_pct, b.memory_pct, b.temperature_c,
    b.optical_rx_power_dbm, b.snr_db, b.signal_strength_dbm, b.signal_quality_pct,
    b.kafka_topic, b.kafka_partition, b.kafka_offset, b.kafka_timestamp, b.ingested_at
)
