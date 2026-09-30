-- Connectivity quality: critical infrastructure vs non-critical
-- Grain: one row per critical_infrastructure flag
SELECT
    s.critical_infrastructure,
    COUNT(*) AS event_count,
    ROUND(AVG(e.latency_ms), 2) AS avg_latency_ms,
    ROUND(AVG(e.jitter_ms), 2) AS avg_jitter_ms,
    ROUND(AVG(e.packet_loss_pct), 4) AS avg_packet_loss_pct,
    ROUND(APPROX_PERCENTILE(e.latency_ms, 0.95), 2) AS p95_latency_ms,
    ROUND(APPROX_PERCENTILE(e.jitter_ms, 0.95), 2) AS p95_jitter_ms,
    ROUND(APPROX_PERCENTILE(e.packet_loss_pct, 0.95), 4) AS p95_packet_loss_pct
FROM iceberg.solution.connectivity_events_gold e
JOIN iceberg.solution.devices d ON e.device_id = d.device_id
JOIN iceberg.solution.sites s ON d.site_id = s.site_id
GROUP BY s.critical_infrastructure
ORDER BY s.critical_infrastructure DESC
