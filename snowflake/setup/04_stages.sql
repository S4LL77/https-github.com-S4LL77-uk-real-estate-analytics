-- ============================================================================
-- 04_stages.sql — External Stages and File Formats
-- ============================================================================
-- External stages connect Snowflake to the S3 data lake.
-- For local development, we can also use internal stages with PUT/COPY.
--
-- NOTE: If using S3, replace <YOUR_S3_BUCKET> and configure
-- the storage integration with your AWS IAM role.
-- ============================================================================

USE ROLE SYSADMIN;
USE DATABASE UK_REAL_ESTATE;
USE SCHEMA BRONZE;

-- ============================================================================
-- File Formats
-- ============================================================================

-- Parquet format for bronze layer files
CREATE FILE FORMAT IF NOT EXISTS PARQUET_FORMAT
    TYPE = PARQUET
    COMPRESSION = SNAPPY;

-- CSV format for direct Land Registry file loading (alternative path)
CREATE FILE FORMAT IF NOT EXISTS LAND_REGISTRY_CSV_FORMAT
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 0              -- Land Registry CSVs have NO headers
    NULL_IF = ('', 'NULL')
    EMPTY_FIELD_AS_NULL = TRUE
    COMPRESSION = NONE;

-- CSV format for Bank of England data
CREATE FILE FORMAT IF NOT EXISTS BOE_CSV_FORMAT
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1              -- BoE CSV has headers
    NULL_IF = ('', 'NULL')
    COMPRESSION = NONE;

-- ============================================================================
-- Storage Integration (S3) — Uncomment when AWS is configured
-- ============================================================================

-- CREATE STORAGE INTEGRATION IF NOT EXISTS S3_INTEGRATION
--     TYPE = EXTERNAL_STAGE
--     STORAGE_PROVIDER = 'S3'
--     STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT_ID>:role/snowflake-s3-access'
--     STORAGE_ALLOWED_LOCATIONS = ('s3://<YOUR_S3_BUCKET>/bronze/',
--                                  's3://<YOUR_S3_BUCKET>/silver/',
--                                  's3://<YOUR_S3_BUCKET>/gold/')
--     ENABLED = TRUE;
--
-- DESC INTEGRATION S3_INTEGRATION;
-- -- Note: Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
-- -- to configure the AWS IAM trust policy.

-- ============================================================================
-- External Stages — Uncomment when S3 integration is ready
-- ============================================================================

-- CREATE STAGE IF NOT EXISTS BRONZE_LAND_REGISTRY_STAGE
--     STORAGE_INTEGRATION = S3_INTEGRATION
--     URL = 's3://<YOUR_S3_BUCKET>/bronze/land_registry/'
--     FILE_FORMAT = PARQUET_FORMAT;

-- CREATE STAGE IF NOT EXISTS BRONZE_BOE_RATES_STAGE
--     STORAGE_INTEGRATION = S3_INTEGRATION
--     URL = 's3://<YOUR_S3_BUCKET>/bronze/boe_rates/'
--     FILE_FORMAT = PARQUET_FORMAT;

-- CREATE STAGE IF NOT EXISTS BRONZE_ONS_STAGE
--     STORAGE_INTEGRATION = S3_INTEGRATION
--     URL = 's3://<YOUR_S3_BUCKET>/bronze/ons_demographics/'
--     FILE_FORMAT = PARQUET_FORMAT;

-- ============================================================================
-- Internal Stage (for local development — upload files with PUT)
-- ============================================================================

CREATE STAGE IF NOT EXISTS BRONZE_INTERNAL_STAGE
    FILE_FORMAT = PARQUET_FORMAT
    COMMENT = 'Internal stage for local dev — use PUT to upload Parquet files';

-- ============================================================================
-- Bronze Tables (landing zone for COPY INTO)
-- ============================================================================

CREATE TABLE IF NOT EXISTS UK_REAL_ESTATE.BRONZE.LAND_REGISTRY_RAW (
    transaction_id      VARCHAR(50),
    price_paid          NUMBER(12, 0),
    date_of_transfer    VARCHAR(30),
    postcode            VARCHAR(10),
    property_type       VARCHAR(1),
    old_new             VARCHAR(1),
    duration            VARCHAR(1),
    paon                VARCHAR(200),
    saon                VARCHAR(200),
    street              VARCHAR(200),
    locality            VARCHAR(200),
    town_city           VARCHAR(100),
    district            VARCHAR(100),
    county              VARCHAR(100),
    ppd_category_type   VARCHAR(1),
    record_status       VARCHAR(1),
    -- Metadata columns added during ingestion
    _ingested_at        TIMESTAMP_NTZ,
    _source_file        VARCHAR(50),
    _batch_id           VARCHAR(30),
    -- Snowflake metadata
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS UK_REAL_ESTATE.BRONZE.BOE_RATES_RAW (
    rate_date           DATE,
    rate_value          NUMBER(5, 2),
    _ingested_at        TIMESTAMP_NTZ,
    _source_file        VARCHAR(100),
    _batch_id           VARCHAR(30),
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================================
-- Example: COPY INTO from internal stage
-- ============================================================================

-- PUT file://data/bronze/land_registry/year=2024/data.parquet @BRONZE_INTERNAL_STAGE/land_registry/;
-- COPY INTO BRONZE.LAND_REGISTRY_RAW
--     FROM @BRONZE_INTERNAL_STAGE/land_registry/
--     FILE_FORMAT = PARQUET_FORMAT
--     MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

-- ============================================================================
-- Verification
-- ============================================================================
SHOW STAGES;
SHOW FILE FORMATS;
SHOW TABLES IN SCHEMA BRONZE;
