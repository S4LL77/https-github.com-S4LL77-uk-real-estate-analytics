-- models/marts/fct_transactions.sql

{{
    config(
        materialized='table',
        tags=['fact', 'core']
    )
}}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_land_registry') }}
),

boe_rates AS (
    SELECT * FROM {{ ref('stg_boe_rates') }}
),

-- Finding the correct interest rate for a specific transaction date.
-- A simple JOIN inequality can cause Cartesian explosions in big datasets.
-- Modern Snowflake optimizes ASOF JOINs (Lateral Joins) nicely:
enriched_facts AS (
    SELECT 
        tx.transaction_sk,
        tx.original_transaction_id,
        tx.date_of_transfer,
        tx.price_paid,
        
        -- Creating Foreign Keys to Dimensions
        -- 1. Location SK
        MD5(tx.postcode) AS location_sk,
        
        -- 2. Property SK (From our SCD Type 2 logic)
        MD5(
            COALESCE(tx.paon, '') || '-' ||
            COALESCE(tx.saon, '') || '-' ||
            COALESCE(tx.street, '') || '-' ||
            COALESCE(tx.postcode, '')
        ) AS property_nk,
        
        -- Retrieve the Bank of England rate that was active on the tx date
        (
            SELECT rate_value_decimal 
            FROM boe_rates 
            WHERE rate_date <= tx.date_of_transfer 
            ORDER BY rate_date DESC 
            LIMIT 1
        ) AS boe_rate_at_sale_decimal,
        
        tx._batch_id

    FROM transactions tx

    -- Filter out edge-cases where dates are NULL
    WHERE tx.date_of_transfer IS NOT NULL
)

SELECT * FROM enriched_facts
