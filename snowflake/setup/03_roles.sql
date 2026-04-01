-- ============================================================================
-- 03_roles.sql — Role-Based Access Control (RBAC)
-- ============================================================================
-- Implements least-privilege access following Snowflake best practices.
-- Role hierarchy:
--
--   ACCOUNTADMIN
--       └── SYSADMIN
--             ├── DATA_ENGINEER     — Full access, runs pipelines
--             ├── DATA_GOVERNANCE   — PII access for compliance audits
--             ├── TRANSFORMER       — dbt service account role
--             └── ANALYST           — Read-only, PII masked
--
-- Interview talking point:
--   "We separate DATA_ENGINEER from ANALYST roles so that the
--    masking policies in script 05 can differentiate access levels.
--    The TRANSFORMER role is used by dbt's service account — it
--    needs write access to staging/marts but not to raw bronze data."
-- ============================================================================

USE ROLE SECURITYADMIN;

-- ============================================================================
-- Create custom roles
-- ============================================================================

CREATE ROLE IF NOT EXISTS DATA_ENGINEER
    COMMENT = 'Pipeline engineers — full access including PII';

CREATE ROLE IF NOT EXISTS DATA_GOVERNANCE
    COMMENT = 'Compliance team — PII access for audits and masking validation';

CREATE ROLE IF NOT EXISTS TRANSFORMER
    COMMENT = 'dbt service account — write to staging, intermediate, marts';

CREATE ROLE IF NOT EXISTS ANALYST
    COMMENT = 'Business analysts — read-only, PII masked';

-- ============================================================================
-- Role hierarchy — grant custom roles to SYSADMIN
-- ============================================================================

GRANT ROLE DATA_ENGINEER TO ROLE SYSADMIN;
GRANT ROLE DATA_GOVERNANCE TO ROLE SYSADMIN;
GRANT ROLE TRANSFORMER TO ROLE SYSADMIN;
GRANT ROLE ANALYST TO ROLE SYSADMIN;

-- ============================================================================
-- Database-level grants
-- ============================================================================

-- DATA_ENGINEER: Full control
GRANT USAGE ON DATABASE UK_REAL_ESTATE TO ROLE DATA_ENGINEER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE UK_REAL_ESTATE TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON ALL SCHEMAS IN DATABASE UK_REAL_ESTATE TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON ALL TABLES IN DATABASE UK_REAL_ESTATE TO ROLE DATA_ENGINEER;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN DATABASE UK_REAL_ESTATE TO ROLE DATA_ENGINEER;

-- DATA_GOVERNANCE: Read all, needed for masking validation
GRANT USAGE ON DATABASE UK_REAL_ESTATE TO ROLE DATA_GOVERNANCE;
GRANT USAGE ON ALL SCHEMAS IN DATABASE UK_REAL_ESTATE TO ROLE DATA_GOVERNANCE;
GRANT SELECT ON ALL TABLES IN DATABASE UK_REAL_ESTATE TO ROLE DATA_GOVERNANCE;
GRANT SELECT ON FUTURE TABLES IN DATABASE UK_REAL_ESTATE TO ROLE DATA_GOVERNANCE;

-- TRANSFORMER: Write to staging, intermediate, marts only
GRANT USAGE ON DATABASE UK_REAL_ESTATE TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA UK_REAL_ESTATE.STAGING TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA UK_REAL_ESTATE.INTERMEDIATE TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA UK_REAL_ESTATE.MARTS TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA UK_REAL_ESTATE.ANALYTICS TO ROLE TRANSFORMER;
GRANT CREATE TABLE ON SCHEMA UK_REAL_ESTATE.STAGING TO ROLE TRANSFORMER;
GRANT CREATE TABLE ON SCHEMA UK_REAL_ESTATE.INTERMEDIATE TO ROLE TRANSFORMER;
GRANT CREATE TABLE ON SCHEMA UK_REAL_ESTATE.MARTS TO ROLE TRANSFORMER;
GRANT CREATE VIEW ON SCHEMA UK_REAL_ESTATE.STAGING TO ROLE TRANSFORMER;
GRANT CREATE VIEW ON SCHEMA UK_REAL_ESTATE.INTERMEDIATE TO ROLE TRANSFORMER;
GRANT CREATE VIEW ON SCHEMA UK_REAL_ESTATE.MARTS TO ROLE TRANSFORMER;
GRANT CREATE VIEW ON SCHEMA UK_REAL_ESTATE.ANALYTICS TO ROLE TRANSFORMER;
-- Read access to bronze/silver for source data
GRANT USAGE ON SCHEMA UK_REAL_ESTATE.BRONZE TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA UK_REAL_ESTATE.SILVER TO ROLE TRANSFORMER;
GRANT SELECT ON ALL TABLES IN SCHEMA UK_REAL_ESTATE.BRONZE TO ROLE TRANSFORMER;
GRANT SELECT ON ALL TABLES IN SCHEMA UK_REAL_ESTATE.SILVER TO ROLE TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA UK_REAL_ESTATE.BRONZE TO ROLE TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA UK_REAL_ESTATE.SILVER TO ROLE TRANSFORMER;

-- ANALYST: Read-only on marts and analytics
GRANT USAGE ON DATABASE UK_REAL_ESTATE TO ROLE ANALYST;
GRANT USAGE ON SCHEMA UK_REAL_ESTATE.MARTS TO ROLE ANALYST;
GRANT USAGE ON SCHEMA UK_REAL_ESTATE.ANALYTICS TO ROLE ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA UK_REAL_ESTATE.MARTS TO ROLE ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA UK_REAL_ESTATE.ANALYTICS TO ROLE ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA UK_REAL_ESTATE.MARTS TO ROLE ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA UK_REAL_ESTATE.ANALYTICS TO ROLE ANALYST;

-- ============================================================================
-- Warehouse grants
-- ============================================================================

GRANT USAGE ON WAREHOUSE INGEST_WH TO ROLE DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE TRANSFORMER;
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE DATA_ENGINEER;
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE DATA_GOVERNANCE;
GRANT USAGE ON WAREHOUSE ANALYTICS_WH TO ROLE ANALYST;

-- ============================================================================
-- Assign roles to your user (replace YOUR_USERNAME)
-- ============================================================================

-- GRANT ROLE DATA_ENGINEER TO USER YOUR_USERNAME;
-- GRANT ROLE ANALYST TO USER YOUR_USERNAME;

-- ============================================================================
-- Verification
-- ============================================================================
SHOW ROLES;
SHOW GRANTS TO ROLE DATA_ENGINEER;
SHOW GRANTS TO ROLE ANALYST;
