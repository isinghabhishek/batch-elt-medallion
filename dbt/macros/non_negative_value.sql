{#
  non_negative_value(model, column_name)
  ======================================
  Custom dbt test that asserts a column contains only non-negative values (>= 0).
  Also fails if the column contains NULL values.

  Usage in schema.yml:
    columns:
      - name: total_revenue
        tests:
          - non_negative_value

  Returns rows that violate the constraint (value < 0 or NULL).
  A test passes when this query returns zero rows.

  Use this test for:
    - Revenue and monetary amounts
    - Count columns (total_orders, unique_customers)
    - Duration columns (duration_seconds)
    - Any metric that should never be negative
#}
{% test non_negative_value(model, column_name) %}

-- Return rows where the column is negative or NULL.
-- dbt treats a test as failed when this query returns any rows.
SELECT
    {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} < 0
   OR {{ column_name }} IS NULL

{% endtest %}
