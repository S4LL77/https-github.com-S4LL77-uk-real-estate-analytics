-- models/staging/stg_land_registry.sql

WITH raw AS (
    -- Reference the Snowflake bronze Table populated by python/COPY INTO
    SELECT * FROM {{ source('uk_real_estate_bronze', 'land_registry_raw') }}
),

renamed_and_casted AS (
    SELECT
        -- Using MD5 to ensure transaction_id is standard binary/string length 
        -- even if HM Land Registry changes formatting
        MD5(transaction_id) AS transaction_sk,
        transaction_id AS original_transaction_id,

        -- Transaction details
        CAST(price_paid AS NUMBER(18, 2)) AS price_paid,
        CAST(date_of_transfer AS DATE) AS date_of_transfer,

        -- Property categorization mapped to English rather than codes
        CASE property_type
            WHEN 'D' THEN 'Detached'
            WHEN 'S' THEN 'Semi-Detached'
            WHEN 'T' THEN 'Terraced'
            WHEN 'F' THEN 'Flat/Maisonette'
            WHEN 'O' THEN 'Other'
            ELSE 'Unknown'
        END AS property_type,

        CASE old_new
            WHEN 'Y' THEN TRUE
            WHEN 'N' THEN FALSE
            ELSE NULL
        END AS is_new_build,

        CASE duration
            WHEN 'F' THEN 'Freehold'
            WHEN 'L' THEN 'Leasehold'
            ELSE 'Unknown'
        END AS estate_type,

        -- Address Details 
        -- Using NULLIF to clean up blanks inserted by parsing tools
        NULLIF(TRIM(paon), '') AS paon,
        NULLIF(TRIM(saon), '') AS saon,
        NULLIF(TRIM(street), '') AS street,
        NULLIF(TRIM(locality), '') AS locality,
        NULLIF(TRIM(town_city), '') AS town_city,
        NULLIF(TRIM(district), '') AS district,
        NULLIF(TRIM(county), '') AS county,

        -- Location standardization
        UPPER(NULLIF(TRIM(postcode), '')) AS postcode,

        -- Metadata 
        -- Retaining metadata from ingestion helps with Data Lineage
        _ingested_at,
        _batch_id
    FROM raw
)

SELECT * FROM renamed_and_casted
