{{
  config(
    materialized='incremental',
    unique_key='customer_id',
    on_schema_change='fail'
  )
}}

-- Initialize DuckDB extensions and configure S3 connection to MinIO.
{% if execute %}
  {{ init_iceberg() }}
{% endif %}

WITH silver_orders AS (
    -- Only process orders processed after the last Gold update (incremental).
    -- This avoids recalculating LTV for all customers on every run.
    SELECT * FROM {{ ref('orders_clean') }}
    {% if is_incremental() %}
    WHERE _silver_processed_at > (SELECT MAX(_gold_updated_at) FROM {{ this }})
    {% endif %}
),

silver_customers AS (
    -- Full customer dimension — always join the latest customer attributes.
    SELECT * FROM {{ ref('customers_clean') }}
),

customer_order_metrics AS (
    -- Aggregate order history per customer to compute lifetime value metrics.
    SELECT
        o.customer_id,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS last_order_date,
        COUNT(*) AS total_orders,
        SUM(o.order_amount) AS total_spent,
        AVG(o.order_amount) AS avg_order_value,
        -- Days since last order: useful for churn risk analysis
        CURRENT_DATE - MAX(o.order_date) AS days_since_last_order
    FROM silver_orders o
    GROUP BY o.customer_id
)

SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.signup_date,
    c.country,
    m.first_order_date,
    m.last_order_date,
    m.total_orders,
    m.total_spent,
    m.avg_order_value,
    m.days_since_last_order,
    CURRENT_TIMESTAMP AS _gold_updated_at
FROM customer_order_metrics m
INNER JOIN silver_customers c ON m.customer_id = c.customer_id
