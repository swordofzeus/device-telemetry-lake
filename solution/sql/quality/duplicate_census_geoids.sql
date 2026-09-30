-- Duplicate block_group_geoid values in census reference table (should be zero)
SELECT COUNT(*) AS violations,
       CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS window_start,
       CAST(NULL AS TIMESTAMP(6) WITH TIME ZONE) AS window_end
FROM (
    SELECT block_group_geoid
    FROM iceberg.solution.census_block_groups
    GROUP BY block_group_geoid
    HAVING COUNT(*) > 1
)
