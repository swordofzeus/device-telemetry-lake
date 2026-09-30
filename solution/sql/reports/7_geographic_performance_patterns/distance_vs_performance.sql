-- Performance degradation by distance from nearest urban area
-- Grain: one row per distance bucket
-- Shows whether throughput and connectivity degrade further from cities
-- Pre-aggregates each event type per device to avoid many-to-many fan-out
WITH device_sites AS (
    SELECT
        d.device_id,
        s.site_id,
        CASE
            WHEN s.distance_to_nearest_urban_area_km < 5 THEN '0-5 km'
            WHEN s.distance_to_nearest_urban_area_km < 15 THEN '5-15 km'
            WHEN s.distance_to_nearest_urban_area_km < 30 THEN '15-30 km'
            WHEN s.distance_to_nearest_urban_area_km < 50 THEN '30-50 km'
            ELSE '50+ km'
        END AS distance_bucket,
        CASE
            WHEN s.distance_to_nearest_urban_area_km < 5 THEN 1
            WHEN s.distance_to_nearest_urban_area_km < 15 THEN 2
            WHEN s.distance_to_nearest_urban_area_km < 30 THEN 3
            WHEN s.distance_to_nearest_urban_area_km < 50 THEN 4
            ELSE 5
        END AS sort_order
    FROM iceberg.solution.devices d
    JOIN iceberg.solution.sites s ON d.site_id = s.site_id
),
throughput_by_device AS (
    SELECT
        device_id,
        AVG(download_mbps) AS avg_download_mbps,
        AVG(upload_mbps) AS avg_upload_mbps,
        APPROX_PERCENTILE(download_mbps, 0.5) AS median_download_mbps
    FROM iceberg.solution.throughput_events_gold
    GROUP BY device_id
),
connectivity_by_device AS (
    SELECT
        device_id,
        AVG(latency_ms) AS avg_latency_ms,
        AVG(jitter_ms) AS avg_jitter_ms,
        AVG(packet_loss_pct) AS avg_packet_loss_pct,
        APPROX_PERCENTILE(latency_ms, 0.95) AS p95_latency_ms
    FROM iceberg.solution.connectivity_events_gold
    GROUP BY device_id
)
SELECT
    ds.distance_bucket,
    COUNT(DISTINCT ds.device_id) AS device_count,
    COUNT(DISTINCT ds.site_id) AS site_count,
    ROUND(AVG(t.avg_download_mbps), 2) AS avg_download_mbps,
    ROUND(AVG(t.avg_upload_mbps), 2) AS avg_upload_mbps,
    ROUND(AVG(t.median_download_mbps), 2) AS median_download_mbps,
    ROUND(AVG(c.avg_latency_ms), 2) AS avg_latency_ms,
    ROUND(AVG(c.avg_jitter_ms), 2) AS avg_jitter_ms,
    ROUND(AVG(c.avg_packet_loss_pct), 4) AS avg_packet_loss_pct,
    ROUND(AVG(c.p95_latency_ms), 2) AS p95_latency_ms
FROM device_sites ds
LEFT JOIN throughput_by_device t ON ds.device_id = t.device_id
LEFT JOIN connectivity_by_device c ON ds.device_id = c.device_id
GROUP BY ds.distance_bucket, ds.sort_order
ORDER BY ds.sort_order
