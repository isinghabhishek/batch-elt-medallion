{{
  config(
    materialized='incremental',
    unique_key='order_date',
    on_schema_change='fail'
  )
}}

-- Initialize DuckDB extensions and configure S3 connection to MinIO.
{% if execute %}
  {{ init_iceberg() }}
{% endif %}

WITH silver_orders AS (
    -- ref() creates a dbt dependency on orders_clean, ensuring it runs first.
    -- On incremental runs, only process orders for dates not yet in Gold.
    SELECT * FROM {{ ref('orders_clean') }}
    {% if is_incremental() %}
    WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
    {% endif %}
),

daily_aggregates AS (
    -- Aggregate to daily grain: one row per order_date.
    -- These metrics feed the "Business KPIs" Superset dashboard.
    SELECT
        order_date,
        COUNT(*) AS total_orders,
        SUM(order_amount) AS total_revenue,
        AVG(order_amount) AS avg_order_value,
        COUNT(DISTINCT customer_id) AS unique_customers,
        CURRENT_TIMESTAMP AS _gold_updated_at
    FROM silver_orders
    GROUP BY order_date
)

SELECT
    order_date,
    total_orders,
    total_revenue,
    avg_order_value,
    unique_customers,
    _gold_updated_at
FROM daily_aggregates
