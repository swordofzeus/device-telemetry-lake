-- Devices underperforming their peer group (provider + technology)
-- Grain: one row per underperforming device
-- Peer group: devices sharing the same provider and technology
-- Threshold: device median download below 50% of peer group median
WITH device_performance AS (
    SELECT
        d.device_id,
        d.provider_name,
        d.technology,
        d.site_id,
        APPROX_PERCENTILE(t.download_mbps, 0.5) AS median_download,
        APPROX_PERCENTILE(t.upload_mbps, 0.5) AS median_upload,
        COUNT(*) AS event_count
    FROM iceberg.solution.devices d
    JOIN iceberg.solution.throughput_events_gold t ON d.device_id = t.device_id
    GROUP BY d.device_id, d.provider_name, d.technology, d.site_id
),
peer_stats AS (
    SELECT
        provider_name,
        technology,
        COUNT(*) AS peer_count,
        APPROX_PERCENTILE(median_download, 0.5) AS peer_median_download,
        APPROX_PERCENTILE(median_upload, 0.5) AS peer_median_upload
    FROM device_performance
    GROUP BY provider_name, technology
    HAVING COUNT(*) >= 5
)
SELECT
    dp.device_id,
    dp.provider_name,
    dp.technology,
    dp.site_id,
    dp.event_count,
    ROUND(dp.median_download, 2) AS device_median_download_mbps,
    ROUND(dp.median_upload, 2) AS device_median_upload_mbps,
    ROUND(ps.peer_median_download, 2) AS peer_median_download_mbps,
    ROUND(ps.peer_median_download * 0.5, 2) AS threshold_mbps,
    ps.peer_count,
    ROUND((dp.median_download / ps.peer_median_download) * 100, 1) AS pct_of_peer_median
FROM device_performance dp
JOIN peer_stats ps ON dp.provider_name = ps.provider_name AND dp.technology = ps.technology
WHERE dp.median_download < ps.peer_median_download * 0.5
ORDER BY pct_of_peer_median ASC
