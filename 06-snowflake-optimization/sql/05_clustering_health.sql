-- Check a candidate clustering key before adding it. Lower average depth means better pruning on that key.
-- Replace the table and key with the column the slow queries actually filter on.
SELECT SYSTEM$CLUSTERING_INFORMATION('analytics.sales.fact_orders', '(order_date, store_id)');
