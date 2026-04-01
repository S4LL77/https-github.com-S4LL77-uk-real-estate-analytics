-- models/marts/dim_location.sql

{{
    config(
        materialized='table',
        tags=['dimension', 'core']
    )
}}

WITH unique_locations AS (
    -- Extract the unique set of outcodes (postcode sectors) from property transactions
    -- Since we only have historical transactions, we derive the dimension from facts.
    SELECT DISTINCT
        postcode,
        SPLIT_PART(postcode, ' ', 1) AS outward_code,
        town_city,
        locality,
        district,
        county
    FROM {{ ref('stg_land_registry') }}
    WHERE postcode IS NOT NULL
)

SELECT 
    -- MD5 hashing forms a sturdy SK out of natural keys 
    -- for SCD1 (overwrite) dimension behavior
    MD5(postcode) AS location_sk,
    
    postcode,
    outward_code,
    town_city,
    locality,
    district,
    county
    
    -- In a real scenario, this is where we would LEFT JOIN
    -- the ONS Demographics data to provide regional enrichment:
    -- LEFT JOIN stg_ons_demographics ons 
    --     ON unique_locations.outward_code = ons.postcode_outward

FROM unique_locations
