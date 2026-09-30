-- Connectivity statistics by access technology
-- Grain: one row per technology
-- Includes min, max, median, and P95 percentiles
SELECT
    d.technology,
    COUNT(DISTINCT d.device_id) AS device_count,
    COUNT(*) AS event_count,
    ROUND(MIN(c.latency_ms), 2) AS min_latency_ms,
    ROUND(MAX(c.latency_ms), 2) AS max_latency_ms,
    ROUND(AVG(c.latency_ms), 2) AS avg_latency_ms,
    ROUND(APPROX_PERCENTILE(c.latency_ms, 0.5), 2) AS median_latency_ms,
    ROUND(APPROX_PERCENTILE(c.latency_ms, 0.95), 2) AS p95_latency_ms,
    ROUND(MIN(c.jitter_ms), 2) AS min_jitter_ms,
    ROUND(MAX(c.jitter_ms), 2) AS max_jitter_ms,
    ROUND(AVG(c.jitter_ms), 2) AS avg_jitter_ms,
    ROUND(APPROX_PERCENTILE(c.jitter_ms, 0.5), 2) AS median_jitter_ms,
    ROUND(APPROX_PERCENTILE(c.jitter_ms, 0.95), 2) AS p95_jitter_ms,
    ROUND(AVG(c.packet_loss_pct), 4) AS avg_packet_loss_pct,
    ROUND(APPROX_PERCENTILE(c.packet_loss_pct, 0.95), 4) AS p95_packet_loss_pct
FROM iceberg.solution.devices d
JOIN iceberg.solution.connectivity_events_gold c ON d.device_id = c.device_id
GROUP BY d.technology
ORDER BY median_latency_ms ASC
