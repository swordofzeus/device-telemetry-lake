-- Total rejected events
SELECT COUNT(*) AS violations,
       current_timestamp - INTERVAL '{{ params.lookback_days }}' DAY AS window_start,
       current_timestamp AS window_end
FROM iceberg.solution.rejected_events e
WHERE e.rejected_at > current_timestamp - INTERVAL '{{ params.lookback_days }}' DAY
