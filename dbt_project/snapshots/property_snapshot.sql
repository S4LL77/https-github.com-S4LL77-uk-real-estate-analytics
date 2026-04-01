{% snapshot property_snapshot %}

{{
    config(
        target_database='UK_REAL_ESTATE',
        target_schema='MARTS',
        unique_key='property_nk',
        strategy='check',
        check_cols=['estate_type', 'is_new_build', 'paon', 'postcode'],
        alias='dim_property_scd2'
    )
}}

WITH latest_transactions AS (
    -- The Land Registry data isn't a property list, it's a transaction list.
    -- To create a dimension representing physical properties, we need to
    -- deduplicate transactions that hit the exact same address
    
    SELECT 
        -- Creating a Natural Key for the property using its address components
        MD5(
            COALESCE(paon, '') || '-' ||
            COALESCE(saon, '') || '-' ||
            COALESCE(street, '') || '-' ||
            COALESCE(postcode, '')
        ) AS property_nk,
        
        -- Take the latest information about this property based on transfer date
        property_type,
        is_new_build,
        estate_type,
        paon,
        saon,
        street,
        locality,
        town_city,
        district,
        county,
        postcode,
        price_paid,
        
        -- Used by standard Snapshot patterns
        date_of_transfer AS update_date
    FROM {{ ref('stg_land_registry') }}
    
    -- In a real scenario you would qualify by ROW_NUMBER over date_of_transfer
    -- Qualify limits our set to the latest state of each unique property
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY 
            COALESCE(paon, ''), 
            COALESCE(saon, ''), 
            COALESCE(street, ''), 
            COALESCE(postcode, '') 
        ORDER BY date_of_transfer DESC
    ) = 1
)

SELECT * FROM latest_transactions

{% endsnapshot %}
