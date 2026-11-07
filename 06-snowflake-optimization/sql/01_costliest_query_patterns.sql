-- Which repeated query shapes use the most warehouse time over the last 14 days.
-- QUERY_PARAMETERIZED_HASH groups queries that differ only in literal values.
SELECT
    query_parameterized_hash,
    ANY_VALUE(LEFT(query_text, 200))                  AS sample_text,
    ANY_VALUE(warehouse_name)                         AS warehouse_name,
    COUNT(*)                                          AS executions,
    SUM(total_elapsed_time) / 1000 / 3600             AS total_hours,
    MEDIAN(total_elapsed_time) / 1000                 AS median_seconds,
    SUM(bytes_spilled_to_remote_storage) / POWER(1024, 3) AS remote_spill_gb
FROM snowflake.account_usage.query_history
WHERE start_time >= DATEADD(day, -14, CURRENT_TIMESTAMP())
  AND warehouse_name IS NOT NULL
  AND execution_status = 'SUCCESS'
GROUP BY query_parameterized_hash
ORDER BY total_hours DESC
LIMIT 25;
