-- Duplicate dependency pairs in device_dependencies (should be zero)
SELECT COUNT(*) AS violations,
       CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS window_start,
       CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS window_end
FROM (
    SELECT downstream_device_id, upstream_device_id
    FROM iceberg.solution.device_dependencies
    GROUP BY downstream_device_id, upstream_device_id
    HAVING COUNT(*) > 1
)
