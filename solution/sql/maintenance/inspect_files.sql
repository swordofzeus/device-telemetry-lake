SELECT IF(
    COUNT(*) > 10 AND AVG(file_size_in_bytes) < 10485760,
    true, false
)
FROM iceberg.solution."{{ params.table_name }}$files"
