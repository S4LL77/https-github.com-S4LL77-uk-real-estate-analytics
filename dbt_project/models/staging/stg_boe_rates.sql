-- models/staging/stg_boe_rates.sql

WITH raw AS (
    SELECT * FROM {{ source('uk_real_estate_bronze', 'boe_rates_raw') }}
),

renamed_and_casted AS (
    SELECT
        -- The Python ingestion handled the basic Date string conversion, 
        -- but Snowflake needs a strict cast
        CAST(rate_date AS DATE) AS rate_date,
        
        -- Storing percentages explicitly with appropriate scale
        CAST(rate_value AS NUMBER(8, 4)) AS rate_value_percent,
        
        -- Derive the decimal version for easier downstream multiplication
        CAST(rate_value / 100.0 AS NUMBER(8, 6)) AS rate_value_decimal,
        
        _ingested_at,
        _batch_id
    FROM raw
)

SELECT * FROM renamed_and_casted
