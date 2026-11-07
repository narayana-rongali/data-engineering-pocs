-- Queries that read most of a large table. Usually a missing filter, a filter on a function of a column,
-- or a clustering key that does not match how the table is queried.
SELECT
    query_id,
    warehouse_name,
    partitions_scanned,
    partitions_total,
    ROUND(partitions_scanned / NULLIF(partitions_total, 0), 3) AS scan_ratio,
    total_elapsed_time / 1000                                  AS seconds,
    LEFT(query_text, 200)                                      AS sample_text
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
  AND partitions_total >= 1000
  AND partitions_scanned / NULLIF(partitions_total, 0) > 0.8
ORDER BY partitions_scanned DESC
LIMIT 50;
