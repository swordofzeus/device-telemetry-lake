-- Reference tables that are empty (should be zero — all four must have rows)
SELECT COUNT(*) AS violations,
       CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS window_start,
       CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS window_end
FROM (
    SELECT 'sites' AS tbl WHERE (SELECT COUNT(*) FROM iceberg.solution.sites) = 0
    UNION ALL
    SELECT 'devices' WHERE (SELECT COUNT(*) FROM iceberg.solution.devices) = 0
    UNION ALL
    SELECT 'device_dependencies' WHERE (SELECT COUNT(*) FROM iceberg.solution.device_dependencies) = 0
    UNION ALL
    SELECT 'census_block_groups' WHERE (SELECT COUNT(*) FROM iceberg.solution.census_block_groups) = 0
)
