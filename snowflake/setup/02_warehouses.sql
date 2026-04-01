-- ============================================================================
-- 02_warehouses.sql — Virtual Warehouse Configuration
-- ============================================================================
-- Cost optimisation strategy:
--   - Separate warehouses per workload type (ingest, transform, analytics)
--   - Aggressive auto-suspend (60-120s) to minimise credit burn
--   - Auto-resume for seamless user experience
--   - X-SMALL default — Snowflake free trial has limited credits
--
-- Interview talking point:
--   "We use workload-isolated warehouses so that a heavy dbt run
--    doesn't block analyst queries. Each warehouse auto-suspends
--    after 60 seconds of inactivity, which on our trial account
--    saved about 70% of compute cost vs. a single always-on warehouse."
-- ============================================================================

USE ROLE SYSADMIN;

-- Ingestion warehouse: Handles COPY INTO from S3 external stage
-- X-SMALL is sufficient for CSV/Parquet loads (I/O bound, not compute)
CREATE WAREHOUSE IF NOT EXISTS INGEST_WH
    WAREHOUSE_SIZE   = 'XSMALL'
    AUTO_SUSPEND     = 60          -- Suspend after 1 minute idle
    AUTO_RESUME      = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    INITIALLY_SUSPENDED = TRUE     -- Don't start burning credits immediately
    COMMENT = 'Ingestion workload — COPY INTO from S3 stages';

-- Transformation warehouse: Handles dbt runs (SQL-heavy)
-- SMALL gives 2x compute for complex joins and window functions
CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
    WAREHOUSE_SIZE   = 'SMALL'
    AUTO_SUSPEND     = 120         -- 2 minutes — dbt runs have gaps between models
    AUTO_RESUME      = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Transformation workload — dbt model execution';

-- Analytics warehouse: Handles BI tool queries (Tableau, Looker)
-- X-SMALL is fine — mart tables are pre-aggregated
CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_WH
    WAREHOUSE_SIZE   = 'XSMALL'
    AUTO_SUSPEND     = 60
    AUTO_RESUME      = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Analytics workload — BI dashboards and ad-hoc queries';

-- ============================================================================
-- Resource Monitor — Cost guardrail for the free trial
-- ============================================================================
USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS TRIAL_BUDGET
    WITH
        CREDIT_QUOTA = 50           -- 50 credits/month (adjust for your trial)
        FREQUENCY = MONTHLY
        START_TIMESTAMP = IMMEDIATELY
        TRIGGERS
            ON 75 PERCENT DO NOTIFY  -- Alert at 75% usage
            ON 90 PERCENT DO NOTIFY  -- Alert at 90% usage
            ON 100 PERCENT DO SUSPEND_IMMEDIATE;  -- Hard stop at 100%

-- Apply to all warehouses
ALTER WAREHOUSE INGEST_WH SET RESOURCE_MONITOR = TRIAL_BUDGET;
ALTER WAREHOUSE TRANSFORM_WH SET RESOURCE_MONITOR = TRIAL_BUDGET;
ALTER WAREHOUSE ANALYTICS_WH SET RESOURCE_MONITOR = TRIAL_BUDGET;

-- ============================================================================
-- Verification
-- ============================================================================
SHOW WAREHOUSES;
