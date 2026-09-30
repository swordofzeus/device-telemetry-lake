-- Throughput performance: critical infrastructure vs non-critical
-- Grain: one row per critical_infrastructure flag
SELECT
    s.critical_infrastructure,
    COUNT(*) AS event_count,
    ROUND(AVG(e.download_mbps), 2) AS avg_download_mbps,
    ROUND(AVG(e.upload_mbps), 2) AS avg_upload_mbps,
    ROUND(APPROX_PERCENTILE(e.download_mbps, 0.5), 2) AS median_download_mbps,
    ROUND(APPROX_PERCENTILE(e.upload_mbps, 0.5), 2) AS median_upload_mbps,
    ROUND(APPROX_PERCENTILE(e.download_mbps, 0.05), 2) AS p5_download_mbps,
    ROUND(APPROX_PERCENTILE(e.upload_mbps, 0.05), 2) AS p5_upload_mbps
FROM iceberg.solution.throughput_events_gold e
JOIN iceberg.solution.devices d ON e.device_id = d.device_id
JOIN iceberg.solution.sites s ON d.site_id = s.site_id
GROUP BY s.critical_infrastructure
ORDER BY s.critical_infrastructure DESC
