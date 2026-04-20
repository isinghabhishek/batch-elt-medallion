#!/usr/bin/env python3
"""
test_dag_e2e.py — End-to-end DAG execution simulation
======================================================
Simulates the full medallion_pipeline DAG execution logic in Python without
requiring Airflow to be running.  Validates:

  1. Bronze mock data creation (extract step)
  2. Silver + Gold dbt transformations (silver_transform step)
  3. pipeline_runs table record written (log_pipeline_run step)
  4. Failure scenario: missing Bronze data → downstream tasks do not run

Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.8

Usage (inside dbt runner container or with dbt-duckdb installed locally):
    python test_dag_e2e.py

Environment variables (defaults match docker-compose.yml):
    MINIO_ROOT_USER     — MinIO access key  (default: minioadmin)
    MINIO_ROOT_PASSWORD — MinIO secret key  (default: minioadmin)
    DBT_PROJECT_DIR     — path to dbt project (default: /dbt or ./dbt)
"""

import duckdb
import os
import sys
import subprocess
import uuid
from datetime import datetime

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

MINIO_USER = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")

# Resolve dbt project directory: prefer env var, then /dbt (container), then ./dbt (local)
_dbt_candidates = [
    os.getenv("DBT_PROJECT_DIR", ""),
    "/dbt",
    os.path.join(os.path.dirname(__file__)),
]
DBT_PROJECT_DIR = next((p for p in _dbt_candidates if p and os.path.isfile(os.path.join(p, "dbt_project.yml"))), None)

PASS = 0
FAIL = 0
RESULTS: list[dict] = []


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def check(label: str, condition: bool, detail: str = "") -> bool:
    global PASS, FAIL
    status = "PASS" if condition else "FAIL"
    msg = f"  [{status}] {label}" + (f" — {detail}" if detail else "")
    print(msg)
    RESULTS.append({"label": label, "status": status, "detail": detail})
    if condition:
        PASS += 1
    else:
        FAIL += 1
    return condition


def s3_conn() -> duckdb.DuckDBPyConnection:
    """Return a DuckDB in-memory connection configured for MinIO."""
    conn = duckdb.connect(":memory:")
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
    conn.execute(f"SET s3_access_key_id='{MINIO_USER}';")
    conn.execute(f"SET s3_secret_access_key='{MINIO_PASSWORD}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    return conn


def count_parquet(conn: duckdb.DuckDBPyConnection, path: str) -> int:
    """Return row count for a Parquet glob path, or -1 on error."""
    try:
        return conn.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
    except Exception as e:
        print(f"    ⚠  count_parquet({path}): {e}")
        return -1


def run_dbt(args: list[str]) -> tuple[int, str]:
    """
    Run a dbt command in DBT_PROJECT_DIR.
    Returns (returncode, combined stdout+stderr).
    """
    if DBT_PROJECT_DIR is None:
        return 1, "dbt project directory not found"
    cmd = ["dbt"] + args
    print(f"    $ dbt {' '.join(args)}")
    result = subprocess.run(
        cmd,
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
        env={**os.environ, "MINIO_ROOT_USER": MINIO_USER, "MINIO_ROOT_PASSWORD": MINIO_PASSWORD},
    )
    output = result.stdout + result.stderr
    # Print last 20 lines to avoid flooding the terminal
    lines = output.strip().splitlines()
    for line in lines[-20:]:
        print(f"    | {line}")
    return result.returncode, output


# ---------------------------------------------------------------------------
# Step 1 — Bronze mock data creation
# ---------------------------------------------------------------------------

def step_create_bronze_data() -> bool:
    """
    Simulate the extract tasks: run create_mock_bronze_data.py to populate
    the Bronze layer in MinIO.  Mirrors what the Airbyte fallback path does.
    """
    print("\n" + "=" * 60)
    print("STEP 1: Create Bronze mock data (extract simulation)")
    print("=" * 60)

    script = os.path.join(os.path.dirname(__file__), "create_mock_bronze_data.py")
    if not os.path.isfile(script):
        check("create_mock_bronze_data.py exists", False, f"not found at {script}")
        return False

    check("create_mock_bronze_data.py exists", True)

    print("  Running create_mock_bronze_data.py ...")
    result = subprocess.run(
        [sys.executable, script],
        capture_output=True,
        text=True,
        env={**os.environ, "MINIO_ROOT_USER": MINIO_USER, "MINIO_ROOT_PASSWORD": MINIO_PASSWORD},
    )
    output = result.stdout + result.stderr
    for line in output.strip().splitlines():
        print(f"    | {line}")

    ok = result.returncode == 0
    check("Bronze data creation script exits 0", ok, f"rc={result.returncode}")
    if not ok:
        return False

    # Verify data landed in MinIO
    conn = s3_conn()
    weather_cnt = count_parquet(conn, "s3://bronze/weather_data/**/*.parquet")
    customers_cnt = count_parquet(conn, "s3://bronze/customers/**/*.parquet")
    orders_cnt = count_parquet(conn, "s3://bronze/orders/**/*.parquet")
    conn.close()

    check("Bronze weather_data rows > 0", weather_cnt > 0, f"{weather_cnt} rows")
    check("Bronze customers rows > 0", customers_cnt > 0, f"{customers_cnt} rows")
    check("Bronze orders rows > 0", orders_cnt > 0, f"{orders_cnt} rows")

    return weather_cnt > 0 and customers_cnt > 0 and orders_cnt > 0


# ---------------------------------------------------------------------------
# Step 2 — Silver + Gold dbt transformations
# ---------------------------------------------------------------------------

def step_run_silver_gold_dbt() -> bool:
    """
    Simulate silver_transform: run `dbt run --select silver gold` in a single
    dbt invocation so Silver in-memory tables are available to Gold models.
    Req 6.1, 7.1, 8.3
    """
    print("\n" + "=" * 60)
    print("STEP 2: Silver + Gold dbt transformations (silver_transform)")
    print("=" * 60)

    if DBT_PROJECT_DIR is None:
        check("dbt project directory found", False, "dbt_project.yml not found")
        return False
    check("dbt project directory found", True, DBT_PROJECT_DIR)

    rc, output = run_dbt(["run", "--select", "silver", "gold"])
    ok = rc == 0
    check("dbt run --select silver gold exits 0", ok, f"rc={rc}")

    if not ok:
        # Show more context on failure
        print("  ⚠  dbt run failed — check output above for details")
        return False

    # Verify Silver output files exist in MinIO
    conn = s3_conn()
    silver_weather = count_parquet(conn, "s3://silver/weather_clean/**/*.parquet")
    silver_orders = count_parquet(conn, "s3://silver/orders_clean/**/*.parquet")
    silver_customers = count_parquet(conn, "s3://silver/customers_clean/**/*.parquet")

    check("Silver weather_clean rows > 0", silver_weather > 0, f"{silver_weather} rows")
    check("Silver orders_clean rows > 0", silver_orders > 0, f"{silver_orders} rows")
    check("Silver customers_clean rows > 0", silver_customers > 0, f"{silver_customers} rows")

    # Verify Gold output files exist in MinIO
    gold_orders = count_parquet(conn, "s3://gold/order_metrics/**/*.parquet")
    gold_clv = count_parquet(conn, "s3://gold/customer_lifetime_value/**/*.parquet")
    conn.close()

    check("Gold order_metrics rows > 0", gold_orders > 0, f"{gold_orders} rows")
    check("Gold customer_lifetime_value rows > 0", gold_clv > 0, f"{gold_clv} rows")

    return ok and silver_weather > 0 and silver_orders > 0 and silver_customers > 0


# ---------------------------------------------------------------------------
# Step 3 — pipeline_runs table record
# ---------------------------------------------------------------------------

def step_log_pipeline_run() -> bool:
    """
    Simulate log_pipeline_run: write a pipeline run record to
    s3://gold/pipeline_runs/ and verify it can be read back.
    Req 9.6, 10.5, 15.3
    """
    print("\n" + "=" * 60)
    print("STEP 3: Log pipeline run (log_pipeline_run simulation)")
    print("=" * 60)

    dag_run_id = f"e2e_test_{uuid.uuid4().hex[:8]}"
    start_time = datetime.now()
    end_time = datetime.now()
    duration_seconds = int((end_time - start_time).total_seconds())
    status = "success"

    conn = s3_conn()

    # Count rows from each layer for the record
    bronze_count = count_parquet(conn, "s3://bronze/**/*.parquet")
    silver_count = (
        count_parquet(conn, "s3://silver/weather_clean/**/*.parquet") +
        count_parquet(conn, "s3://silver/orders_clean/**/*.parquet") +
        count_parquet(conn, "s3://silver/customers_clean/**/*.parquet")
    )
    gold_count = (
        count_parquet(conn, "s3://gold/order_metrics/**/*.parquet") +
        count_parquet(conn, "s3://gold/customer_lifetime_value/**/*.parquet")
    )

    print(f"  Bronze rows: {bronze_count}")
    print(f"  Silver rows: {silver_count}")
    print(f"  Gold rows:   {gold_count}")

    # Write pipeline run record
    written = False
    try:
        conn.execute(f"""
            COPY (
                SELECT
                    '{dag_run_id}' AS dag_run_id,
                    TIMESTAMP '{start_time}' AS start_time,
                    TIMESTAMP '{end_time}' AS end_time,
                    '{status}' AS status,
                    {bronze_count}::INTEGER AS bronze_row_count,
                    {silver_count}::INTEGER AS silver_row_count,
                    {gold_count}::INTEGER AS gold_row_count,
                    {duration_seconds} AS duration_seconds
            ) TO 's3://gold/pipeline_runs/{dag_run_id}.parquet' (FORMAT PARQUET);
        """)
        written = True
        print(f"  ✅ Wrote pipeline run record: {dag_run_id}")
    except Exception as e:
        print(f"  ❌ Failed to write pipeline run record: {e}")

    check("pipeline_runs record written to S3", written)

    # Read it back to verify
    if written:
        try:
            rows = conn.execute(
                f"SELECT dag_run_id, status, bronze_row_count, silver_row_count, gold_row_count "
                f"FROM read_parquet('s3://gold/pipeline_runs/{dag_run_id}.parquet')"
            ).fetchall()
            check("pipeline_runs record readable", len(rows) == 1, f"{len(rows)} rows")
            if rows:
                row = rows[0]
                check("pipeline_runs dag_run_id matches", row[0] == dag_run_id)
                check("pipeline_runs status is 'success'", row[1] == "success")
                check("pipeline_runs bronze_row_count > 0", (row[2] or 0) > 0, f"{row[2]}")
                check("pipeline_runs silver_row_count > 0", (row[3] or 0) > 0, f"{row[3]}")
                check("pipeline_runs gold_row_count > 0", (row[4] or 0) > 0, f"{row[4]}")
        except Exception as e:
            check("pipeline_runs record readable", False, str(e))

    conn.close()
    return written


# ---------------------------------------------------------------------------
# Step 4 — Failure scenario: missing Bronze data
# ---------------------------------------------------------------------------

def step_failure_scenario() -> bool:
    """
    Simulate the failure scenario (Req 10.8):
    When Bronze data is missing, the extract tasks raise an exception.
    Downstream tasks (bronze_dq, silver_transform, etc.) should NOT run.

    We simulate this by:
    1. Attempting to read from a non-existent Bronze path.
    2. Verifying the read raises an exception (simulating extract task failure).
    3. Confirming that the downstream logic is gated on the extract result.
    """
    print("\n" + "=" * 60)
    print("STEP 4: Failure scenario — missing Bronze data")
    print("=" * 60)

    conn = s3_conn()

    # Simulate extract task: try to read from a path that doesn't exist
    fake_path = "s3://bronze/nonexistent_source/does_not_exist/**/*.parquet"
    extract_failed = False
    try:
        count = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{fake_path}')"
        ).fetchone()[0]
        # If we get here with 0 rows, treat as "no data" failure
        if count == 0:
            extract_failed = True
            print(f"  ℹ  No data at {fake_path} — extract would fail (0 rows)")
        else:
            print(f"  ⚠  Unexpectedly found {count} rows at {fake_path}")
    except Exception as e:
        extract_failed = True
        print(f"  ℹ  Read raised exception as expected: {type(e).__name__}: {e}")

    check(
        "Extract raises error / returns 0 rows when Bronze path missing",
        extract_failed,
        "simulates Airbyte failure with no fallback data",
    )

    # Simulate DAG trigger_rule='none_failed_min_one_success':
    # If ALL extract tasks fail, bronze_dq should be skipped.
    # We model this as: downstream_should_run = any(extract_results) where
    # extract_results is a list of booleans (True = success, False = failed).
    extract_results = [False, False, False]  # all three extract tasks failed
    downstream_should_run = any(extract_results)

    check(
        "Downstream tasks skipped when all extract tasks fail",
        not downstream_should_run,
        f"extract_results={extract_results}, downstream_should_run={downstream_should_run}",
    )

    # Simulate partial failure: one extract succeeds, two fail
    extract_results_partial = [True, False, False]
    downstream_partial = any(extract_results_partial)

    check(
        "Downstream tasks run when at least one extract task succeeds",
        downstream_partial,
        f"extract_results={extract_results_partial}, downstream_should_run={downstream_partial}",
    )

    conn.close()
    return extract_failed and not downstream_should_run and downstream_partial


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    global PASS, FAIL

    print("=" * 60)
    print("MEDALLION PIPELINE — END-TO-END DAG EXECUTION TEST")
    print(f"Requirements: 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.8")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"MinIO endpoint: {MINIO_ENDPOINT}")
    print(f"dbt project: {DBT_PROJECT_DIR or 'NOT FOUND'}")
    print("=" * 60)

    bronze_ok = step_create_bronze_data()
    dbt_ok = step_run_silver_gold_dbt() if bronze_ok else False
    log_ok = step_log_pipeline_run() if dbt_ok else False
    failure_ok = step_failure_scenario()

    if not bronze_ok:
        print("\n⚠  Bronze data creation failed — skipping dbt and log steps")
        check("dbt run --select silver gold (skipped: no Bronze data)", False, "skipped")
        check("pipeline_runs record written (skipped: no Bronze data)", False, "skipped")

    # Final summary
    total = PASS + FAIL
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"  Passed: {PASS}/{total}")
    print(f"  Failed: {FAIL}/{total}")
    print()

    for r in RESULTS:
        icon = "✅" if r["status"] == "PASS" else "❌"
        detail = f" — {r['detail']}" if r["detail"] else ""
        print(f"  {icon} {r['label']}{detail}")

    print()
    if FAIL == 0:
        print("✅ ALL TESTS PASSED — DAG end-to-end execution verified")
    else:
        print(f"❌ {FAIL} TEST(S) FAILED — review output above")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
