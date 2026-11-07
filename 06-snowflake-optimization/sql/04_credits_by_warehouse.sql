-- Daily credits per warehouse, to see where cost goes and spot warehouses left running.
SELECT
    warehouse_name,
    DATE_TRUNC('day', start_time)    AS day,
    SUM(credits_used_compute)        AS compute_credits,
    SUM(credits_used_cloud_services) AS cloud_services_credits
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY warehouse_name, day
ORDER BY day DESC, compute_credits DESC;
