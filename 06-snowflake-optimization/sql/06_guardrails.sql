-- Cost guardrails. Values are examples to adapt, not recommendations for every workload.
ALTER WAREHOUSE transform_wh SET AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;
ALTER WAREHOUSE transform_wh SET STATEMENT_TIMEOUT_IN_SECONDS = 3600;

CREATE OR REPLACE RESOURCE MONITOR transform_monthly
    WITH CREDIT_QUOTA = 500
    FREQUENCY = MONTHLY
    START_TIMESTAMP = IMMEDIATELY
    TRIGGERS ON 80 PERCENT DO NOTIFY
             ON 100 PERCENT DO SUSPEND;

ALTER WAREHOUSE transform_wh SET RESOURCE_MONITOR = transform_monthly;

-- Repeated identical BI queries can be served from the result cache when it stays on.
ALTER ACCOUNT SET USE_CACHED_RESULT = TRUE;
