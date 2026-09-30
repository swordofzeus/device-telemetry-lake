-- Urban vs rural performance divide
-- Grain: one row per urban/rural classification
-- Compares throughput and connectivity quality between urban and rural areas
-- Uses inside_census_urban_area from sites and population density from census
-- Pre-aggregates each event type per device to avoid many-to-many fan-out
WITH throughput_by_device AS (
    SELECT
        device_id,
        AVG(download_mbps) AS avg_download_mbps,
        AVG(upload_mbps) AS avg_upload_mbps,
        APPROX_PERCENTILE(download_mbps, 0.5) AS median_download_mbps,
        APPROX_PERCENTILE(download_mbps, 0.05) AS p5_download_mbps
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
    CASE WHEN s.inside_census_urban_area THEN 'Urban' ELSE 'Rural' END AS area_type,
    COUNT(DISTINCT d.device_id) AS device_count,
    COUNT(DISTINCT s.site_id) AS site_count,
    ROUND(AVG(cbg.population_density_per_sq_mile), 0) AS avg_population_density,
    -- Throughput (device-level averages, then averaged across area)
    ROUND(AVG(t.avg_download_mbps), 2) AS avg_download_mbps,
    ROUND(AVG(t.avg_upload_mbps), 2) AS avg_upload_mbps,
    ROUND(AVG(t.median_download_mbps), 2) AS median_download_mbps,
    ROUND(AVG(t.p5_download_mbps), 2) AS p5_download_mbps,
    -- Connectivity (device-level averages, then averaged across area)
    ROUND(AVG(c.avg_latency_ms), 2) AS avg_latency_ms,
    ROUND(AVG(c.avg_jitter_ms), 2) AS avg_jitter_ms,
    ROUND(AVG(c.avg_packet_loss_pct), 4) AS avg_packet_loss_pct,
    ROUND(AVG(c.p95_latency_ms), 2) AS p95_latency_ms
FROM iceberg.solution.devices d
JOIN iceberg.solution.sites s ON d.site_id = s.site_id
JOIN iceberg.solution.census_block_groups cbg ON s.block_group_geoid = cbg.block_group_geoid
LEFT JOIN throughput_by_device t ON d.device_id = t.device_id
LEFT JOIN connectivity_by_device c ON d.device_id = c.device_id
GROUP BY s.inside_census_urban_area
ORDER BY area_type
