MERGE INTO iceberg.solution.connectivity_events_gold AS g
USING (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY kafka_offset DESC
    ) AS rn
    FROM iceberg.solution.connectivity_events
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
    latency_ms = b.latency_ms,
    jitter_ms = b.jitter_ms,
    packet_loss_pct = b.packet_loss_pct,
    kafka_topic = b.kafka_topic,
    kafka_partition = b.kafka_partition,
    kafka_offset = b.kafka_offset,
    kafka_timestamp = b.kafka_timestamp,
    ingested_at = b.ingested_at
WHEN NOT MATCHED AND b.rn = 1 THEN INSERT (
    event_id, event_type, event_timestamp, device_session_id,
    sequence_number, site_id, device_id, schema_version,
    latency_ms, jitter_ms, packet_loss_pct,
    kafka_topic, kafka_partition, kafka_offset, kafka_timestamp, ingested_at
) VALUES (
    b.event_id, b.event_type, b.event_timestamp, b.device_session_id,
    b.sequence_number, b.site_id, b.device_id, b.schema_version,
    b.latency_ms, b.jitter_ms, b.packet_loss_pct,
    b.kafka_topic, b.kafka_partition, b.kafka_offset, b.kafka_timestamp, b.ingested_at
)
