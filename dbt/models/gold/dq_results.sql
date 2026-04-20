{{
  config(
    materialized='table',
    on_schema_change='fail'
  )
}}

-- Initialize Iceberg connection on first run
{% if execute %}
  {{ init_iceberg() }}
{% endif %}

-- Data Quality Results Observability Table
-- This table tracks data quality check results across all layers
-- Populated by Airflow tasks and dbt test results, not by dbt transformations

SELECT
    CAST(NULL AS VARCHAR) AS check_id,
    CAST(NULL AS VARCHAR) AS check_name,
    CAST(NULL AS VARCHAR) AS layer,
    CAST(NULL AS VARCHAR) AS table_name,
    CAST(NULL AS TIMESTAMP) AS check_timestamp,
    CAST(NULL AS VARCHAR) AS status,
    CAST(NULL AS INTEGER) AS row_count,
    CAST(NULL AS VARCHAR) AS error_message
WHERE 1 = 0  -- Empty table, will be populated by Airflow and dbt tests
