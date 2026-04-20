{{
  config(
    materialized='incremental',
    unique_key='customer_id',
    on_schema_change='append_new_columns'
  )
}}

-- Initialize DuckDB extensions and configure S3 connection to MinIO.
{% if execute %}
  {{ init_iceberg() }}
{% endif %}

WITH source AS (
    -- Read all Parquet files from the Bronze customers path.
    -- On incremental runs, only fetch records newer than the last Silver load.
    SELECT * FROM read_parquet('s3://bronze/customers/**/*.parquet')
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        -- Clean and type cast primary key
        CAST(customer_id AS VARCHAR) AS customer_id,
        
        -- Clean and trim string fields
        TRIM(customer_name) AS customer_name,
        
        -- Handle empty strings -> NULL and trim email
        CASE 
            WHEN TRIM(email) = '' THEN NULL 
            ELSE LOWER(TRIM(email))
        END AS email,
        
        -- Type cast date fields to ISO 8601 format
        CAST(signup_date AS DATE) AS signup_date,
        
        -- Handle empty strings -> NULL for country
        CASE 
            WHEN TRIM(country) = '' THEN NULL 
            ELSE TRIM(country) 
        END AS country,
        
        -- Preserve Bronze metadata
        CAST(_ingested_at AS TIMESTAMP) AS _ingested_at,
        
        -- Add Silver processing timestamp
        CURRENT_TIMESTAMP AS _silver_processed_at
    FROM source
    WHERE customer_id IS NOT NULL
),

-- Deduplicate: keep latest record by _ingested_at for each customer_id
deduplicated AS (
    SELECT * FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY customer_id 
                   ORDER BY _ingested_at DESC
               ) AS row_num
        FROM cleaned
    ) ranked
    WHERE row_num = 1
)

SELECT
    customer_id,
    customer_name,
    email,
    signup_date,
    country,
    _ingested_at,
    _silver_processed_at
FROM deduplicated
