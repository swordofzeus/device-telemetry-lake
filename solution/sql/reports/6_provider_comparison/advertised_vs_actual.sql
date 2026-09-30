-- Provider advertised vs actual throughput delivery
-- Grain: one row per provider + technology
-- Ordered by worst delivery ratio (providers least delivering on their promise)
-- Pre-aggregates to one row per device so each device gets equal weight
WITH device_delivery AS (
    SELECT
        d.device_id,
        d.provider_name,
        d.technology,
        d.advertised_download_mbps,
        d.advertised_upload_mbps,
        AVG(t.download_mbps) AS avg_download_mbps,
        AVG(t.upload_mbps) AS avg_upload_mbps,
        AVG(t.download_mbps) / d.advertised_download_mbps * 100 AS download_delivery_pct,
        AVG(t.upload_mbps) / d.advertised_upload_mbps * 100 AS upload_delivery_pct
    FROM iceberg.solution.devices d
    JOIN iceberg.solution.throughput_events_gold t ON d.device_id = t.device_id
    WHERE d.advertised_download_mbps IS NOT NULL
      AND d.advertised_upload_mbps IS NOT NULL
    GROUP BY d.device_id, d.provider_name, d.technology,
             d.advertised_download_mbps, d.advertised_upload_mbps
)
SELECT
    provider_name,
    technology,
    COUNT(*) AS device_count,
    ROUND(AVG(advertised_download_mbps), 2) AS avg_advertised_download_mbps,
    ROUND(AVG(advertised_upload_mbps), 2) AS avg_advertised_upload_mbps,
    ROUND(AVG(avg_download_mbps), 2) AS avg_actual_download_mbps,
    ROUND(AVG(avg_upload_mbps), 2) AS avg_actual_upload_mbps,
    ROUND(AVG(download_delivery_pct), 1) AS avg_download_delivery_pct,
    ROUND(AVG(upload_delivery_pct), 1) AS avg_upload_delivery_pct,
    ROUND(APPROX_PERCENTILE(download_delivery_pct, 0.05), 1) AS p5_download_delivery_pct
FROM device_delivery
GROUP BY provider_name, technology
ORDER BY avg_download_delivery_pct ASC
