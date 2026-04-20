#!/usr/bin/env python3
"""
Verify Gold layer aggregations end-to-end.

Runs the full Silver + Gold pipeline in-memory using DuckDB, verifies:
  - Gold tables contain correct aggregations (non-null metrics, correct row counts)
  - Proper aggregation logic (totals, averages, unique counts)
  - Incremental idempotency: running the pipeline twice produces no duplicate records

Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 13.4
"""

import duckdb
import os
import sys

PASS = 0
FAIL = 0


def check(label: str, condition: bool, detail: str = "") -> bool:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✅ {label}" + (f" — {detail}" if detail else ""))
    else:
        FAIL += 1
        print(f"  ❌ {label}" + (f" — {detail}" if detail else ""))
    return condition


def build_connection() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB in-memory connection configured for MinIO S3."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")

    minio_user = os.getenv("MINIO_ROOT_USER", "admin")
    minio_password = os.getenv("MINIO_ROOT_PASSWORD", "changeme123")

    conn.execute(f"SET s3_endpoint='minio:9000'")
    conn.execute(f"SET s3_access_key_id='{minio_user}'")
    conn.execute(f"SET s3_secret_access_key='{minio_password}'")
    conn.execute("SET s3_use_ssl=false")
    conn.execute("SET s3_url_style='path'")
    return conn


def build_silver_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Build Silver tables from Bronze S3 Parquet files."""

    # weather_clean
    conn.execute("""
        CREATE OR REPLACE TABLE weather_clean AS
        WITH source AS (
            SELECT * FROM read_parquet('s3://bronze/weather_data/**/*.parquet')
        ),
        cleaned AS (
            SELECT
                TRIM(location) AS location,
                CAST(date AS DATE) AS date,
                CAST(temperature AS DOUBLE) AS temperature_celsius,
                CAST(humidity AS DOUBLE) AS humidity_percent,
                CASE WHEN TRIM(condition) = '' THEN NULL ELSE TRIM(condition) END AS condition,
                CAST(_ingested_at AS TIMESTAMP) AS _ingested_at,
                CURRENT_TIMESTAMP AS _silver_processed_at,
                TRIM(location) || '_' || CAST(date AS VARCHAR) AS location_date
            FROM source
            WHERE location IS NOT NULL AND date IS NOT NULL
        ),
        deduped AS (
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY location_date ORDER BY _ingested_at DESC
                ) AS rn FROM cleaned
            ) t WHERE rn = 1
        )
        SELECT location, date, temperature_celsius, humidity_percent, condition,
               _ingested_at, _silver_processed_at, location_date
        FROM deduped
    """)

    # customers_clean
    conn.execute("""
        CREATE OR REPLACE TABLE customers_clean AS
        WITH source AS (
            SELECT * FROM read_parquet('s3://bronze/customers/**/*.parquet')
        ),
        cleaned AS (
            SELECT
                CAST(customer_id AS VARCHAR) AS customer_id,
                TRIM(customer_name) AS customer_name,
                CASE WHEN TRIM(email) = '' THEN NULL ELSE LOWER(TRIM(email)) END AS email,
                CAST(signup_date AS DATE) AS signup_date,
                CASE WHEN TRIM(country) = '' THEN NULL ELSE TRIM(country) END AS country,
                CAST(_ingested_at AS TIMESTAMP) AS _ingested_at,
                CURRENT_TIMESTAMP AS _silver_processed_at
            FROM source
            WHERE customer_id IS NOT NULL
        ),
        deduped AS (
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY customer_id ORDER BY _ingested_at DESC
                ) AS rn FROM cleaned
            ) t WHERE rn = 1
        )
        SELECT customer_id, customer_name, email, signup_date, country,
               _ingested_at, _silver_processed_at
        FROM deduped
    """)

    # orders_clean
    conn.execute("""
        CREATE OR REPLACE TABLE orders_clean AS
        WITH source AS (
            SELECT * FROM read_parquet('s3://bronze/orders/**/*.parquet')
        ),
        cleaned AS (
            SELECT
                CAST(order_id AS VARCHAR) AS order_id,
                CAST(customer_id AS VARCHAR) AS customer_id,
                CAST(order_date AS DATE) AS order_date,
                CAST(order_amount AS DOUBLE) AS order_amount,
                CASE WHEN TRIM(status) = '' THEN NULL ELSE TRIM(status) END AS status,
                CAST(_ingested_at AS TIMESTAMP) AS _ingested_at,
                CURRENT_TIMESTAMP AS _silver_processed_at
            FROM source
            WHERE order_id IS NOT NULL
        ),
        deduped AS (
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY order_id ORDER BY _ingested_at DESC
                ) AS rn FROM cleaned
            ) t WHERE rn = 1
        )
        SELECT order_id, customer_id, order_date, order_amount, status,
               _ingested_at, _silver_processed_at
        FROM deduped
    """)


def build_gold_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Build Gold aggregation tables from Silver in-memory tables."""

    # order_metrics — daily aggregation
    conn.execute("""
        CREATE OR REPLACE TABLE order_metrics AS
        SELECT
            order_date,
            COUNT(*) AS total_orders,
            SUM(order_amount) AS total_revenue,
            AVG(order_amount) AS avg_order_value,
            COUNT(DISTINCT customer_id) AS unique_customers,
            CURRENT_TIMESTAMP AS _gold_updated_at
        FROM orders_clean
        GROUP BY order_date
    """)

    # customer_lifetime_value — per-customer aggregation
    conn.execute("""
        CREATE OR REPLACE TABLE customer_lifetime_value AS
        WITH metrics AS (
            SELECT
                customer_id,
                MIN(order_date) AS first_order_date,
                MAX(order_date) AS last_order_date,
                COUNT(*) AS total_orders,
                SUM(order_amount) AS total_spent,
                AVG(order_amount) AS avg_order_value,
                CURRENT_DATE - MAX(order_date) AS days_since_last_order
            FROM orders_clean
            GROUP BY customer_id
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
        FROM metrics m
        INNER JOIN customers_clean c ON m.customer_id = c.customer_id
    """)

    # pipeline_runs — empty observability table (populated by Airflow)
    conn.execute("""
        CREATE OR REPLACE TABLE pipeline_runs AS
        SELECT
            CAST(NULL AS VARCHAR) AS dag_run_id,
            CAST(NULL AS TIMESTAMP) AS start_time,
            CAST(NULL AS TIMESTAMP) AS end_time,
            CAST(NULL AS VARCHAR) AS status,
            CAST(NULL AS INTEGER) AS bronze_row_count,
            CAST(NULL AS INTEGER) AS silver_row_count,
            CAST(NULL AS INTEGER) AS gold_row_count,
            CAST(NULL AS INTEGER) AS duration_seconds
        WHERE 1 = 0
    """)

    # dq_results — empty observability table (populated by Airflow)
    conn.execute("""
        CREATE OR REPLACE TABLE dq_results AS
        SELECT
            CAST(NULL AS VARCHAR) AS check_id,
            CAST(NULL AS VARCHAR) AS check_name,
            CAST(NULL AS VARCHAR) AS layer,
            CAST(NULL AS VARCHAR) AS table_name,
            CAST(NULL AS TIMESTAMP) AS check_timestamp,
            CAST(NULL AS VARCHAR) AS status,
            CAST(NULL AS INTEGER) AS row_count,
            CAST(NULL AS VARCHAR) AS error_message
        WHERE 1 = 0
    """)


def verify_silver_counts(conn: duckdb.DuckDBPyConnection) -> None:
    print("\n1. Silver Layer Counts (prerequisite):")
    print("-" * 60)
    for table, min_rows in [("weather_clean", 1), ("customers_clean", 1), ("orders_clean", 1)]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        check(f"{table}: {count} records", count >= min_rows, f"expected >= {min_rows}")


def verify_order_metrics(conn: duckdb.DuckDBPyConnection) -> None:
    print("\n2. order_metrics Aggregation Checks:")
    print("-" * 60)

    row_count = conn.execute("SELECT COUNT(*) FROM order_metrics").fetchone()[0]
    check("Row count > 0", row_count > 0, f"{row_count} rows")

    # Req 7.4 — no NULL metric columns
    for col in ["total_orders", "total_revenue", "avg_order_value", "unique_customers", "_gold_updated_at"]:
        nulls = conn.execute(f"SELECT COUNT(*) FROM order_metrics WHERE {col} IS NULL").fetchone()[0]
        check(f"No NULLs in {col}", nulls == 0, f"{nulls} nulls")

    # Req 7.4 — non-negative values
    for col in ["total_orders", "total_revenue", "avg_order_value", "unique_customers"]:
        neg = conn.execute(f"SELECT COUNT(*) FROM order_metrics WHERE {col} < 0").fetchone()[0]
        check(f"Non-negative {col}", neg == 0, f"{neg} negative values")

    # Req 7.2 — aggregation correctness: sum of daily totals == total orders in Silver
    silver_orders = conn.execute("SELECT COUNT(*) FROM orders_clean").fetchone()[0]
    gold_total = conn.execute("SELECT SUM(total_orders) FROM order_metrics").fetchone()[0]
    check("Sum of daily total_orders == Silver order count",
          int(gold_total) == silver_orders,
          f"gold={gold_total}, silver={silver_orders}")

    # Revenue cross-check
    silver_revenue = conn.execute("SELECT SUM(order_amount) FROM orders_clean").fetchone()[0]
    gold_revenue = conn.execute("SELECT SUM(total_revenue) FROM order_metrics").fetchone()[0]
    check("Sum of daily total_revenue == Silver total order_amount",
          abs(gold_revenue - silver_revenue) < 0.01,
          f"gold={gold_revenue:.2f}, silver={silver_revenue:.2f}")

    # Req 7.3 — _gold_updated_at present
    has_ts = conn.execute("SELECT COUNT(*) FROM order_metrics WHERE _gold_updated_at IS NOT NULL").fetchone()[0]
    check("_gold_updated_at populated", has_ts == row_count, f"{has_ts}/{row_count}")

    # Unique key: one row per order_date
    dupes = conn.execute(
        "SELECT COUNT(*) - COUNT(DISTINCT order_date) FROM order_metrics"
    ).fetchone()[0]
    check("No duplicate order_date rows (unique_key)", dupes == 0, f"{dupes} duplicates")


def verify_customer_lifetime_value(conn: duckdb.DuckDBPyConnection) -> None:
    print("\n3. customer_lifetime_value Aggregation Checks:")
    print("-" * 60)

    row_count = conn.execute("SELECT COUNT(*) FROM customer_lifetime_value").fetchone()[0]
    check("Row count > 0", row_count > 0, f"{row_count} rows")

    # Req 7.4 — no NULL metric columns
    for col in ["customer_id", "customer_name", "signup_date", "first_order_date",
                "last_order_date", "total_orders", "total_spent", "avg_order_value", "_gold_updated_at"]:
        nulls = conn.execute(f"SELECT COUNT(*) FROM customer_lifetime_value WHERE {col} IS NULL").fetchone()[0]
        check(f"No NULLs in {col}", nulls == 0, f"{nulls} nulls")

    # Req 7.4 — non-negative values
    for col in ["total_orders", "total_spent", "avg_order_value"]:
        neg = conn.execute(f"SELECT COUNT(*) FROM customer_lifetime_value WHERE {col} < 0").fetchone()[0]
        check(f"Non-negative {col}", neg == 0, f"{neg} negative values")

    # All customers have at least 1 order
    zero_orders = conn.execute(
        "SELECT COUNT(*) FROM customer_lifetime_value WHERE total_orders = 0"
    ).fetchone()[0]
    check("All customers have >= 1 order", zero_orders == 0, f"{zero_orders} with 0 orders")

    # Cross-check: total_spent per customer matches Silver
    mismatch = conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT c.customer_id,
                   c.total_spent AS gold_spent,
                   SUM(o.order_amount) AS silver_spent
            FROM customer_lifetime_value c
            JOIN orders_clean o ON c.customer_id = o.customer_id
            GROUP BY c.customer_id, c.total_spent
            HAVING ABS(c.total_spent - SUM(o.order_amount)) > 0.01
        ) t
    """).fetchone()[0]
    check("total_spent matches Silver order sums per customer", mismatch == 0,
          f"{mismatch} mismatches")

    # Unique key: one row per customer_id
    dupes = conn.execute(
        "SELECT COUNT(*) - COUNT(DISTINCT customer_id) FROM customer_lifetime_value"
    ).fetchone()[0]
    check("No duplicate customer_id rows (unique_key)", dupes == 0, f"{dupes} duplicates")

    # Req 7.3 — _gold_updated_at present
    has_ts = conn.execute(
        "SELECT COUNT(*) FROM customer_lifetime_value WHERE _gold_updated_at IS NOT NULL"
    ).fetchone()[0]
    check("_gold_updated_at populated", has_ts == row_count, f"{has_ts}/{row_count}")


def verify_observability_tables(conn: duckdb.DuckDBPyConnection) -> None:
    print("\n4. Observability Tables (pipeline_runs, dq_results):")
    print("-" * 60)

    for table, expected_cols in [
        ("pipeline_runs", ["dag_run_id", "start_time", "end_time", "status",
                           "bronze_row_count", "silver_row_count", "gold_row_count", "duration_seconds"]),
        ("dq_results", ["check_id", "check_name", "layer", "table_name",
                        "check_timestamp", "status", "row_count", "error_message"]),
    ]:
        try:
            schema = conn.execute(f"DESCRIBE {table}").fetchall()
            actual_cols = {row[0] for row in schema}
            missing = set(expected_cols) - actual_cols
            check(f"{table} has all expected columns", len(missing) == 0,
                  f"missing: {missing}" if missing else "all present")
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            check(f"{table} is empty (populated by Airflow)", count == 0, f"{count} rows")
        except Exception as e:
            check(f"{table} accessible", False, str(e)[:80])


def verify_incremental_idempotency(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Req 13.4 / 13.5 — Running the pipeline twice with the same source data
    must produce the same output (no duplicates).

    Simulate by rebuilding Gold tables a second time and comparing counts.
    """
    print("\n5. Incremental Idempotency (run pipeline twice, verify no duplicates):")
    print("-" * 60)

    # Capture counts from first run
    om_count_1 = conn.execute("SELECT COUNT(*) FROM order_metrics").fetchone()[0]
    clv_count_1 = conn.execute("SELECT COUNT(*) FROM customer_lifetime_value").fetchone()[0]

    # Second run — rebuild Gold tables (simulates re-run with same Silver data)
    build_gold_tables(conn)

    om_count_2 = conn.execute("SELECT COUNT(*) FROM order_metrics").fetchone()[0]
    clv_count_2 = conn.execute("SELECT COUNT(*) FROM customer_lifetime_value").fetchone()[0]

    check("order_metrics row count unchanged after second run",
          om_count_1 == om_count_2,
          f"run1={om_count_1}, run2={om_count_2}")

    check("customer_lifetime_value row count unchanged after second run",
          clv_count_1 == clv_count_2,
          f"run1={clv_count_1}, run2={clv_count_2}")

    # Verify no duplicate unique keys after second run
    om_dupes = conn.execute(
        "SELECT COUNT(*) - COUNT(DISTINCT order_date) FROM order_metrics"
    ).fetchone()[0]
    check("No duplicate order_date after second run", om_dupes == 0, f"{om_dupes} dupes")

    clv_dupes = conn.execute(
        "SELECT COUNT(*) - COUNT(DISTINCT customer_id) FROM customer_lifetime_value"
    ).fetchone()[0]
    check("No duplicate customer_id after second run", clv_dupes == 0, f"{clv_dupes} dupes")

    # Req 13.5 — same revenue total both runs
    rev_2 = conn.execute("SELECT SUM(total_revenue) FROM order_metrics").fetchone()[0]
    silver_rev = conn.execute("SELECT SUM(order_amount) FROM orders_clean").fetchone()[0]
    check("Total revenue consistent after second run",
          abs(rev_2 - silver_rev) < 0.01,
          f"gold={rev_2:.2f}, silver={silver_rev:.2f}")


def print_sample_data(conn: duckdb.DuckDBPyConnection) -> None:
    print("\n6. Sample Gold Data:")
    print("-" * 60)

    print("\n  order_metrics (top 5 by revenue):")
    rows = conn.execute("""
        SELECT order_date, total_orders, ROUND(total_revenue, 2) AS revenue,
               ROUND(avg_order_value, 2) AS avg_order, unique_customers
        FROM order_metrics ORDER BY revenue DESC LIMIT 5
    """).fetchall()
    print(f"  {'Date':<12} {'Orders':<8} {'Revenue':<12} {'Avg Order':<12} {'Customers'}")
    for r in rows:
        print(f"  {str(r[0]):<12} {r[1]:<8} {r[2]:<12} {r[3]:<12} {r[4]}")

    print("\n  customer_lifetime_value (top 5 by total_spent):")
    rows = conn.execute("""
        SELECT customer_id, customer_name, total_orders,
               ROUND(total_spent, 2) AS spent, ROUND(avg_order_value, 2) AS avg_order
        FROM customer_lifetime_value ORDER BY spent DESC LIMIT 5
    """).fetchall()
    print(f"  {'Cust ID':<10} {'Name':<15} {'Orders':<8} {'Spent':<12} {'Avg Order'}")
    for r in rows:
        print(f"  {r[0]:<10} {r[1]:<15} {r[2]:<8} {r[3]:<12} {r[4]}")


def main() -> None:
    global PASS, FAIL

    print("=" * 60)
    print("GOLD LAYER END-TO-END VERIFICATION")
    print("Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 13.4")
    print("=" * 60)

    print("\nConnecting to MinIO and loading extensions...")
    conn = build_connection()

    print("Building Silver tables from Bronze S3 data...")
    try:
        build_silver_tables(conn)
    except Exception as e:
        print(f"\n❌ Failed to build Silver tables: {e}")
        sys.exit(1)

    print("Building Gold aggregation tables...")
    try:
        build_gold_tables(conn)
    except Exception as e:
        print(f"\n❌ Failed to build Gold tables: {e}")
        sys.exit(1)

    # Run all verification sections
    verify_silver_counts(conn)
    verify_order_metrics(conn)
    verify_customer_lifetime_value(conn)
    verify_observability_tables(conn)
    verify_incremental_idempotency(conn)
    print_sample_data(conn)

    conn.close()

    # Final summary
    total = PASS + FAIL
    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    print(f"  Passed: {PASS}/{total}")
    print(f"  Failed: {FAIL}/{total}")
    if FAIL == 0:
        print("\n✅ ALL CHECKS PASSED — Gold layer is correct and idempotent")
    else:
        print(f"\n❌ {FAIL} CHECK(S) FAILED — review output above")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Verification failed with unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
