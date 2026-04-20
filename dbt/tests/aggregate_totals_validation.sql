-- Custom test to validate aggregate totals between order_metrics and customer_lifetime_value
-- This test ensures that the sum of daily order metrics matches the sum of customer lifetime values

WITH order_metrics_totals AS (
    SELECT
        SUM(total_orders) AS total_orders_sum,
        SUM(total_revenue) AS total_revenue_sum
    FROM {{ ref('order_metrics') }}
),

customer_ltv_totals AS (
    SELECT
        SUM(total_orders) AS total_orders_sum,
        SUM(total_spent) AS total_spent_sum
    FROM {{ ref('customer_lifetime_value') }}
),

comparison AS (
    SELECT
        om.total_orders_sum AS order_metrics_orders,
        cl.total_orders_sum AS customer_ltv_orders,
        om.total_revenue_sum AS order_metrics_revenue,
        cl.total_spent_sum AS customer_ltv_revenue,
        ABS(om.total_orders_sum - cl.total_orders_sum) AS orders_diff,
        ABS(om.total_revenue_sum - cl.total_spent_sum) AS revenue_diff
    FROM order_metrics_totals om
    CROSS JOIN customer_ltv_totals cl
)

-- Test fails if differences exceed tolerance (0.01 for rounding)
SELECT *
FROM comparison
WHERE orders_diff > 0.01
   OR revenue_diff > 0.01
