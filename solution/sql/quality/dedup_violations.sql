-- Duplicate event_ids in gold tables (all tables combined, should be zero)
SELECT COALESCE(SUM(cnt), 0) AS violations,
       current_timestamp - INTERVAL '{{ params.lookback_days }}' DAY AS window_start,
       current_timestamp AS window_end
FROM (
    SELECT COUNT(*) AS cnt FROM (SELECT event_id FROM iceberg.solution.throughput_events_gold e WHERE e.ingested_at > current_timestamp - INTERVAL '{{ params.lookback_days }}' DAY GROUP BY event_id HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT event_id FROM iceberg.solution.connectivity_events_gold e WHERE e.ingested_at > current_timestamp - INTERVAL '{{ params.lookback_days }}' DAY GROUP BY event_id HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT event_id FROM iceberg.solution.device_health_events_gold e WHERE e.ingested_at > current_timestamp - INTERVAL '{{ params.lookback_days }}' DAY GROUP BY event_id HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT event_id FROM iceberg.solution.connectivity_state_events_gold e WHERE e.ingested_at > current_timestamp - INTERVAL '{{ params.lookback_days }}' DAY GROUP BY event_id HAVING COUNT(*) > 1)
    UNION ALL
    SELECT COUNT(*) FROM (SELECT event_id FROM iceberg.solution.infrastructure_events_gold e WHERE e.ingested_at > current_timestamp - INTERVAL '{{ params.lookback_days }}' DAY GROUP BY event_id HAVING COUNT(*) > 1)
)
