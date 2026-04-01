-- ============================================================================
-- 05_masking_policies.sql — GDPR Data Protection
-- ============================================================================
-- Implements UK GDPR compliance for address-level PII fields.
--
-- IMPORTANT: Dynamic Data Masking (CREATE MASKING POLICY) requires
-- Snowflake Enterprise Edition. The free trial is Standard Edition.
--
-- This script provides TWO approaches:
--   (A) SECURE VIEWS — works on ALL editions (Standard, Enterprise, etc.)
--   (B) MASKING POLICIES — Enterprise only (commented out, for reference)
--
-- In interviews, explain BOTH approaches:
--   "On Standard Edition I used Secure Views with role-based CASE logic
--    to achieve the same result. On Enterprise I'd use native Dynamic
--    Data Masking for cleaner separation of concerns — the policy is
--    attached to the column, not the view."
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE UK_REAL_ESTATE;

-- ============================================================================
-- APPROACH A: Secure Views (works on Standard Edition / Free Trial)
-- ============================================================================
-- Secure Views hide the view definition from non-owners, preventing
-- users from reverse-engineering the masking logic via SHOW VIEWS
-- or GET_DDL(). The CASE WHEN CURRENT_ROLE() logic achieves the
-- same role-based masking as native masking policies.
-- ============================================================================

-- First, create a sample table to demonstrate masking
-- (In production, this would be the dbt-generated mart table)
CREATE TABLE IF NOT EXISTS UK_REAL_ESTATE.GOLD.DIM_PROPERTY_BASE (
    property_sk         NUMBER AUTOINCREMENT,
    property_nk         VARCHAR(100),
    property_type       VARCHAR(1),
    old_new             VARCHAR(1),
    duration            VARCHAR(1),
    paon                VARCHAR(200),     -- PII: house number/name
    saon                VARCHAR(200),     -- PII: flat/unit number
    street              VARCHAR(200),     -- PII: street name
    postcode            VARCHAR(10),      -- Partially sensitive
    town_city           VARCHAR(100),
    district            VARCHAR(100),
    county              VARCHAR(100),
    valid_from          DATE,
    valid_to            DATE,
    is_current          BOOLEAN DEFAULT TRUE,
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- Secure View: DIM_PROPERTY (role-based masking via CASE)
-- ============================================================================
-- This is the view that analysts and BI tools query — never the base table.
-- The SECURE keyword prevents users from seeing the view SQL definition.
-- ============================================================================

CREATE OR REPLACE SECURE VIEW UK_REAL_ESTATE.GOLD.DIM_PROPERTY AS
SELECT
    property_sk,
    property_nk,
    property_type,
    old_new,
    duration,

    -- GDPR: Address masking by role
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_GOVERNANCE', 'SYSADMIN', 'ACCOUNTADMIN')
            THEN paon
        WHEN CURRENT_ROLE() IN ('ANALYST', 'TRANSFORMER')
            THEN '***MASKED***'
        ELSE '***REDACTED***'
    END AS paon,

    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_GOVERNANCE', 'SYSADMIN', 'ACCOUNTADMIN')
            THEN saon
        WHEN CURRENT_ROLE() IN ('ANALYST', 'TRANSFORMER')
            THEN '***MASKED***'
        ELSE '***REDACTED***'
    END AS saon,

    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_GOVERNANCE', 'SYSADMIN', 'ACCOUNTADMIN')
            THEN street
        WHEN CURRENT_ROLE() IN ('ANALYST', 'TRANSFORMER')
            THEN '***MASKED***'
        ELSE '***REDACTED***'
    END AS street,

    -- Postcode: partial mask — keep outward code (area), hide inward code
    -- "SW1A 1AA" → "SW1A ***" for analysts
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_GOVERNANCE', 'SYSADMIN', 'ACCOUNTADMIN')
            THEN postcode
        WHEN CURRENT_ROLE() IN ('ANALYST', 'TRANSFORMER')
            THEN CONCAT(SPLIT_PART(postcode, ' ', 1), ' ***')
        ELSE '***REDACTED***'
    END AS postcode,

    town_city,
    district,
    county,
    valid_from,
    valid_to,
    is_current
FROM UK_REAL_ESTATE.GOLD.DIM_PROPERTY_BASE;

-- ============================================================================
-- Grant access: Analysts query the VIEW, never the base table
-- ============================================================================

-- Analysts can only SELECT from the secure view
GRANT SELECT ON VIEW UK_REAL_ESTATE.GOLD.DIM_PROPERTY TO ROLE ANALYST;

-- Engineers and governance can access the base table directly
GRANT SELECT ON TABLE UK_REAL_ESTATE.GOLD.DIM_PROPERTY_BASE TO ROLE DATA_ENGINEER;
GRANT SELECT ON TABLE UK_REAL_ESTATE.GOLD.DIM_PROPERTY_BASE TO ROLE DATA_GOVERNANCE;

-- Schema usage grants
GRANT USAGE ON SCHEMA UK_REAL_ESTATE.GOLD TO ROLE ANALYST;
GRANT USAGE ON SCHEMA UK_REAL_ESTATE.GOLD TO ROLE DATA_ENGINEER;
GRANT USAGE ON SCHEMA UK_REAL_ESTATE.GOLD TO ROLE DATA_GOVERNANCE;

-- ============================================================================
-- Insert sample data for testing
-- ============================================================================

INSERT INTO UK_REAL_ESTATE.GOLD.DIM_PROPERTY_BASE
    (property_nk, property_type, old_new, duration, paon, saon, street, postcode, town_city, district, county, valid_from, valid_to, is_current)
VALUES
    ('PROP-001', 'T', 'N', 'F', '10', '', 'DOWNING STREET', 'SW1A 2AA', 'LONDON', 'CITY OF WESTMINSTER', 'GREATER LONDON', '2024-01-01', '9999-12-31', TRUE),
    ('PROP-002', 'F', 'Y', 'L', '', 'FLAT 3', 'THREADNEEDLE ST', 'EC2R 8AH', 'LONDON', 'CITY OF LONDON', 'GREATER LONDON', '2024-03-15', '9999-12-31', TRUE),
    ('PROP-003', 'D', 'N', 'F', '42', '', 'PICCADILLY', 'M1 1AA', 'MANCHESTER', 'MANCHESTER', 'GREATER MANCHESTER', '2024-06-01', '9999-12-31', TRUE);

-- ============================================================================
-- VERIFICATION: Test masking with different roles
-- ============================================================================

-- Test as SYSADMIN (should see full data):
SELECT 'SYSADMIN sees:' AS test, paon, street, postcode
FROM UK_REAL_ESTATE.GOLD.DIM_PROPERTY
LIMIT 3;

-- Test as ANALYST (should see masked data):
-- Uncomment after granting ANALYST role to your user:
--   GRANT ROLE ANALYST TO USER <your_username>;
--
-- USE ROLE ANALYST;
-- SELECT 'ANALYST sees:' AS test, paon, street, postcode
-- FROM UK_REAL_ESTATE.GOLD.DIM_PROPERTY
-- LIMIT 3;
--
-- Expected output:
-- | test          | paon         | street       | postcode   |
-- |---------------|--------------|--------------|------------|
-- | ANALYST sees: | ***MASKED*** | ***MASKED*** | SW1A ***   |
-- | ANALYST sees: | ***MASKED*** | ***MASKED*** | EC2R ***   |
-- | ANALYST sees: | ***MASKED*** | ***MASKED*** | M1 ***     |

-- ============================================================================
-- APPROACH B: Native Masking Policies (Enterprise Edition only)
-- ============================================================================
-- When you upgrade to Enterprise, uncomment and use these instead.
-- They're cleaner because the policy is attached to the COLUMN,
-- not to a view — any query on the column is automatically masked.
-- ============================================================================

-- CREATE OR REPLACE MASKING POLICY UK_REAL_ESTATE.GOLD.PII_ADDRESS_MASK
--     AS (val STRING) RETURNS STRING ->
--     CASE
--         WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_GOVERNANCE')
--             THEN val
--         WHEN CURRENT_ROLE() IN ('ANALYST', 'TRANSFORMER')
--             THEN '***MASKED***'
--         ELSE '***REDACTED***'
--     END
--     COMMENT = 'GDPR: Masks address-level PII fields by role';

-- CREATE OR REPLACE MASKING POLICY UK_REAL_ESTATE.GOLD.PII_POSTCODE_MASK
--     AS (val STRING) RETURNS STRING ->
--     CASE
--         WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'DATA_GOVERNANCE')
--             THEN val
--         WHEN CURRENT_ROLE() IN ('ANALYST', 'TRANSFORMER')
--             THEN CONCAT(SPLIT_PART(val, ' ', 1), ' ***')
--         ELSE '***REDACTED***'
--     END
--     COMMENT = 'GDPR: Partial postcode masking — outward code only';

-- ALTER TABLE dim_property_base MODIFY COLUMN paon
--     SET MASKING POLICY UK_REAL_ESTATE.GOLD.PII_ADDRESS_MASK;
-- ALTER TABLE dim_property_base MODIFY COLUMN saon
--     SET MASKING POLICY UK_REAL_ESTATE.GOLD.PII_ADDRESS_MASK;
-- ALTER TABLE dim_property_base MODIFY COLUMN street
--     SET MASKING POLICY UK_REAL_ESTATE.GOLD.PII_ADDRESS_MASK;
-- ALTER TABLE dim_property_base MODIFY COLUMN postcode
--     SET MASKING POLICY UK_REAL_ESTATE.GOLD.PII_POSTCODE_MASK;
