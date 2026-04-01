-- ============================================================================
-- 01_databases.sql — Snowflake Database and Schema Setup
-- ============================================================================
-- UK Real Estate Analytics — Medallion Architecture
--
-- Run this script with SYSADMIN or ACCOUNTADMIN role.
-- This creates the database structure mirroring the medallion layers.
-- ============================================================================

USE ROLE SYSADMIN;

-- Create the main database
CREATE DATABASE IF NOT EXISTS UK_REAL_ESTATE
    COMMENT = 'UK Real Estate Market Analytics — Portfolio Project';

-- ============================================================================
-- Schemas — Medallion Architecture
-- ============================================================================

-- Bronze: Raw data as-is from source (Parquet files from S3)
CREATE SCHEMA IF NOT EXISTS UK_REAL_ESTATE.BRONZE
    COMMENT = 'Raw ingested data — no transformations applied';

-- Silver: Cleaned, typed, deduplicated data
CREATE SCHEMA IF NOT EXISTS UK_REAL_ESTATE.SILVER
    COMMENT = 'Cleaned and conformed data — business entities';

-- Gold: Business-ready aggregates and star schema
CREATE SCHEMA IF NOT EXISTS UK_REAL_ESTATE.GOLD
    COMMENT = 'Analytical datasets — star schema for consumption';

-- Staging: dbt staging models (1:1 source mapping)
CREATE SCHEMA IF NOT EXISTS UK_REAL_ESTATE.STAGING
    COMMENT = 'dbt staging layer — source-conformed models';

-- Intermediate: dbt intermediate models (joins, enrichment)
CREATE SCHEMA IF NOT EXISTS UK_REAL_ESTATE.INTERMEDIATE
    COMMENT = 'dbt intermediate layer — business logic';

-- Marts: dbt mart models (star schema dimensions and facts)
CREATE SCHEMA IF NOT EXISTS UK_REAL_ESTATE.MARTS
    COMMENT = 'dbt marts — star schema for BI consumption';

-- Analytics: Views and materialised tables for Tableau/Looker
CREATE SCHEMA IF NOT EXISTS UK_REAL_ESTATE.ANALYTICS
    COMMENT = 'BI-ready views and aggregation tables';

-- Audit: Data quality logs, pipeline metadata
CREATE SCHEMA IF NOT EXISTS UK_REAL_ESTATE.AUDIT
    COMMENT = 'Pipeline audit logs and data quality results';

-- ============================================================================
-- Verification
-- ============================================================================
SHOW SCHEMAS IN DATABASE UK_REAL_ESTATE;
