-- Device health: critical infrastructure vs non-critical
-- Grain: one row per critical_infrastructure flag
SELECT
    s.critical_infrastructure,
    COUNT(*) AS event_count,
    ROUND(AVG(e.cpu_pct), 2) AS avg_cpu_pct,
    ROUND(AVG(e.memory_pct), 2) AS avg_memory_pct,
    ROUND(AVG(e.temperature_c), 2) AS avg_temperature_c,
    ROUND(APPROX_PERCENTILE(e.cpu_pct, 0.95), 2) AS p95_cpu_pct,
    ROUND(APPROX_PERCENTILE(e.memory_pct, 0.95), 2) AS p95_memory_pct,
    ROUND(APPROX_PERCENTILE(e.temperature_c, 0.95), 2) AS p95_temperature_c
FROM iceberg.solution.device_health_events_gold e
JOIN iceberg.solution.devices d ON e.device_id = d.device_id
JOIN iceberg.solution.sites s ON d.site_id = s.site_id
GROUP BY s.critical_infrastructure
ORDER BY s.critical_infrastructure DESC
