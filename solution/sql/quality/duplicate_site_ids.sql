-- Duplicate site_id values in sites reference table (should be zero)
SELECT COUNT(*) AS violations,
       CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS window_start,
       CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS window_end
FROM (
    SELECT site_id
    FROM iceberg.solution.sites
    GROUP BY site_id
    HAVING COUNT(*) > 1
)
