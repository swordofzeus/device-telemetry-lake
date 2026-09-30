ALTER TABLE iceberg.solution.{{ params.table_name }}
EXECUTE optimize(file_size_threshold => '100MB')
