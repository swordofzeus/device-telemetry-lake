-- Gold events referencing non-existent sites (all tables combined)
SELECT COALESCE(SUM(cnt), 0) AS violations,
       current_timestamp - INTERVAL '{{ params.lookback_days }}' DAY AS window_start,
       current_timestamp AS window_end
FROM (
    SELECT COUNT(*) AS cnt FROM iceberg.solution.throughput_events_gold e LEFT JOIN iceberg.solution.sites s ON e.site_id = s.site_id WHERE s.site_id IS NULL AND e.ingested_at > current_timestamp - INTERVAL '{{ params.lookback_days }}' DAY
    UNION ALL
    SELECT COUNT(*) FROM iceberg.solution.connectivity_events_gold e LEFT JOIN iceberg.solution.sites s ON e.site_id = s.site_id WHERE s.site_id IS NULL AND e.ingested_at > current_timestamp - INTERVAL '{{ params.lookback_days }}' DAY
    UNION ALL
    SELECT COUNT(*) FROM iceberg.solution.device_health_events_gold e LEFT JOIN iceberg.solution.sites s ON e.site_id = s.site_id WHERE s.site_id IS NULL AND e.ingested_at > current_timestamp - INTERVAL '{{ params.lookback_days }}' DAY
    UNION ALL
    SELECT COUNT(*) FROM iceberg.solution.connectivity_state_events_gold e LEFT JOIN iceberg.solution.sites s ON e.site_id = s.site_id WHERE s.site_id IS NULL AND e.ingested_at > current_timestamp - INTERVAL '{{ params.lookback_days }}' DAY
    UNION ALL
    SELECT COUNT(*) FROM iceberg.solution.infrastructure_events_gold e LEFT JOIN iceberg.solution.sites s ON e.site_id = s.site_id WHERE s.site_id IS NULL AND e.ingested_at > current_timestamp - INTERVAL '{{ params.lookback_days }}' DAY
)
