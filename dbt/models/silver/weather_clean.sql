{{
  config(
    materialized='incremental',
    unique_key='location_date',
    on_schema_change='append_new_columns'
  )
}}

-- Initialize DuckDB extensions and configure S3 connection to MinIO.
-- This macro loads the httpfs extension and sets s3_endpoint, credentials, etc.
{% if execute %}
  {{ init_iceberg() }}
{% endif %}

WITH source AS (
    -- Read all Parquet files from the Bronze weather_data path.
    -- The ** glob matches any subdirectory (e.g. date partitions written by Airbyte).
    -- On incremental runs, only fetch records newer than the last Silver load
    -- to avoid reprocessing the entire Bronze history.
    SELECT * FROM read_parquet('s3://bronze/weather_data/**/*.parquet')
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        -- Clean and standardize string fields
        TRIM(location) AS location,
        
        -- Type cast date and timestamp fields to ISO 8601 format
        CAST(date AS DATE) AS date,
        
        -- Type cast numeric fields
        CAST(temperature AS DOUBLE) AS temperature_celsius,
        CAST(humidity AS DOUBLE) AS humidity_percent,
        
        -- Handle empty strings -> NULL
        CASE 
            WHEN TRIM(condition) = '' THEN NULL 
            ELSE TRIM(condition) 
        END AS condition,
        
        -- Preserve Bronze metadata
        CAST(_ingested_at AS TIMESTAMP) AS _ingested_at,
        
        -- Add Silver processing timestamp
        CURRENT_TIMESTAMP AS _silver_processed_at,
        
        -- Create composite key for deduplication
        TRIM(location) || '_' || CAST(date AS VARCHAR) AS location_date
    FROM source
    WHERE location IS NOT NULL
      AND date IS NOT NULL
),

-- Deduplicate: keep latest record by _ingested_at for each location_date
deduplicated AS (
    SELECT * FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY location_date 
                   ORDER BY _ingested_at DESC
               ) AS row_num
        FROM cleaned
    ) ranked
    WHERE row_num = 1
)

SELECT
    location,
    date,
    temperature_celsius,
    humidity_percent,
    condition,
    _ingested_at,
    _silver_processed_at,
    location_date
FROM deduplicated
