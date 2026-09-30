-- Duplicate device_id values in devices reference table (should be zero)
SELECT COUNT(*) AS violations,
       CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS window_start,
       CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS window_end
FROM (
    SELECT device_id
    FROM iceberg.solution.devices
    GROUP BY device_id
    HAVING COUNT(*) > 1
)
