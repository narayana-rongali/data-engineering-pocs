-- Remote spill means the warehouse ran out of memory and local disk: size up, or reduce the data the query touches.
-- Queueing means too much concurrency for the warehouse: use multi-cluster scaling or move workloads apart.
SELECT
    warehouse_name,
    warehouse_size,
    COUNT(*)                                                              AS queries,
    COUNT_IF(bytes_spilled_to_remote_storage > 0)                         AS queries_with_remote_spill,
    SUM(bytes_spilled_to_remote_storage) / POWER(1024, 3)                 AS remote_spill_gb,
    COUNT_IF(queued_overload_time > 0)                                    AS queued_queries,
    MEDIAN(NULLIF(queued_overload_time, 0)) / 1000                        AS median_queue_seconds
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND warehouse_name IS NOT NULL
GROUP BY warehouse_name, warehouse_size
ORDER BY remote_spill_gb DESC, queued_queries DESC;
