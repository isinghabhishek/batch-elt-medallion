{#
  row_count_greater_than_zero(model)
  ===================================
  Custom dbt test that asserts a model contains at least one row.
  Useful for catching empty Gold tables that would silently produce blank dashboards.

  Usage in schema.yml (model-level test, not column-level):
    models:
      - name: order_metrics
        tests:
          - row_count_greater_than_zero

  Returns a row if the model is empty (row count = 0).
  A test passes when this query returns zero rows.

  Note: This test is applied at the model level, not the column level.
  It does not require a column_name argument.
#}
{% test row_count_greater_than_zero(model) %}

-- Count all rows in the model.
-- Return a row (causing test failure) if the count is zero.
WITH row_count AS (
    SELECT COUNT(*) AS cnt
    FROM {{ model }}
)

SELECT *
FROM row_count
WHERE cnt = 0

{% endtest %}
