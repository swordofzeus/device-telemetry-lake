-- Devices referencing non-existent sites
SELECT COUNT(*) AS violations,
       CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS window_start,
       CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS window_end
FROM iceberg.solution.devices d
LEFT JOIN iceberg.solution.sites s ON d.site_id = s.site_id
WHERE s.site_id IS NULL
