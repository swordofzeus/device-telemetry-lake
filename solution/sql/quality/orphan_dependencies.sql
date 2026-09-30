-- Device dependencies referencing non-existent devices
SELECT COUNT(*) AS violations,
       CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS window_start,
       CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS window_end
FROM iceberg.solution.device_dependencies dd
LEFT JOIN iceberg.solution.devices d1 ON dd.downstream_device_id = d1.device_id
LEFT JOIN iceberg.solution.devices d2 ON dd.upstream_device_id = d2.device_id
WHERE d1.device_id IS NULL OR d2.device_id IS NULL
