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

-- Pipeline Runs Observability Table
-- This table tracks DAG run metadata for pipeline monitoring
-- Populated by Airflow tasks, not by dbt transformations

SELECT
    CAST(NULL AS VARCHAR) AS dag_run_id,
    CAST(NULL AS TIMESTAMP) AS start_time,
    CAST(NULL AS TIMESTAMP) AS end_time,
    CAST(NULL AS VARCHAR) AS status,
    CAST(NULL AS INTEGER) AS bronze_row_count,
    CAST(NULL AS INTEGER) AS silver_row_count,
    CAST(NULL AS INTEGER) AS gold_row_count,
    CAST(NULL AS INTEGER) AS duration_seconds
WHERE 1 = 0  -- Empty table, will be populated by Airflow
