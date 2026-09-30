-- Throughput statistics by access technology
-- Grain: one row per technology
-- Includes min, max, median, and P95/P5 percentiles
SELECT
    d.technology,
    COUNT(DISTINCT d.device_id) AS device_count,
    COUNT(*) AS event_count,
    ROUND(MIN(t.download_mbps), 2) AS min_download_mbps,
    ROUND(MAX(t.download_mbps), 2) AS max_download_mbps,
    ROUND(AVG(t.download_mbps), 2) AS avg_download_mbps,
    ROUND(APPROX_PERCENTILE(t.download_mbps, 0.5), 2) AS median_download_mbps,
    ROUND(APPROX_PERCENTILE(t.download_mbps, 0.05), 2) AS p5_download_mbps,
    ROUND(APPROX_PERCENTILE(t.download_mbps, 0.95), 2) AS p95_download_mbps,
    ROUND(MIN(t.upload_mbps), 2) AS min_upload_mbps,
    ROUND(MAX(t.upload_mbps), 2) AS max_upload_mbps,
    ROUND(AVG(t.upload_mbps), 2) AS avg_upload_mbps,
    ROUND(APPROX_PERCENTILE(t.upload_mbps, 0.5), 2) AS median_upload_mbps,
    ROUND(APPROX_PERCENTILE(t.upload_mbps, 0.05), 2) AS p5_upload_mbps,
    ROUND(APPROX_PERCENTILE(t.upload_mbps, 0.95), 2) AS p95_upload_mbps
FROM iceberg.solution.devices d
JOIN iceberg.solution.throughput_events_gold t ON d.device_id = t.device_id
GROUP BY d.technology
ORDER BY median_download_mbps DESC
