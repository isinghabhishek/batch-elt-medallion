{{
  config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='append_new_columns'
  )
}}

-- Initialize DuckDB extensions and configure S3 connection to MinIO.
{% if execute %}
  {{ init_iceberg() }}
{% endif %}

WITH source AS (
    -- Read all Parquet files from the Bronze orders path.
    -- On incremental runs, only fetch records newer than the last Silver load.
    SELECT * FROM read_parquet('s3://bronze/orders/**/*.parquet')
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        -- Clean and type cast primary key
        CAST(order_id AS VARCHAR) AS order_id,
        
        -- Clean foreign keys
        CAST(customer_id AS VARCHAR) AS customer_id,
        
        -- Type cast date fields to ISO 8601 format
        CAST(order_date AS DATE) AS order_date,
        
        -- Type cast numeric fields
        CAST(order_amount AS DOUBLE) AS order_amount,
        
        -- Handle empty strings -> NULL for status
        CASE 
            WHEN TRIM(status) = '' THEN NULL 
            ELSE TRIM(status) 
        END AS status,
        
        -- Preserve Bronze metadata
        CAST(_ingested_at AS TIMESTAMP) AS _ingested_at,
        
        -- Add Silver processing timestamp
        CURRENT_TIMESTAMP AS _silver_processed_at
    FROM source
    WHERE order_id IS NOT NULL
),

-- Deduplicate: keep latest record by _ingested_at for each order_id
deduplicated AS (
    SELECT * FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY order_id 
                   ORDER BY _ingested_at DESC
               ) AS row_num
        FROM cleaned
    ) ranked
    WHERE row_num = 1
)

SELECT
    order_id,
    customer_id,
    order_date,
    order_amount,
    status,
    _ingested_at,
    _silver_processed_at
FROM deduplicated
