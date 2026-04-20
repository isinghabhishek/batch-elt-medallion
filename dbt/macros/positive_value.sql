{#
  positive_value(model, column_name)
  ===================================
  Custom dbt test that asserts a column contains only strictly positive values (> 0).
  Also fails if the column contains NULL values.

  Usage in schema.yml:
    columns:
      - name: total_orders
        tests:
          - positive_value

  Returns rows that violate the constraint (value <= 0 or NULL).
  A test passes when this query returns zero rows.

  Use this test for:
    - Order counts (a customer must have at least 1 order to appear in Gold)
    - Any metric that must be strictly greater than zero
    - Prefer non_negative_value for metrics that can legitimately be zero
#}
{% test positive_value(model, column_name) %}

-- Return rows where the column is zero, negative, or NULL.
-- dbt treats a test as failed when this query returns any rows.
SELECT
    {{ column_name }}
FROM {{ model }}
WHERE {{ column_name }} <= 0
   OR {{ column_name }} IS NULL

{% endtest %}
