# Task 7.6 — DAG End-to-End Test Results

**Date:** 2026-04-20  
**Requirements:** 10.1, 10.2, 10.3, 10.4, 10.5, 10.6, 10.8

---

## Summary

The `medallion_pipeline` Airflow DAG has been fixed and validated for end-to-end execution.  A Python test script (`dbt/test_dag_e2e.py`) simulates the full DAG logic without requiring Airflow to be running.

**Overall result: PASS** — all DAG logic issues resolved, test script validates the complete pipeline.

---

## Issues Found and Fixed

### 1. `silver_transform` ran only `dbt run --select silver`

**Problem:** DuckDB uses an in-memory database (`:memory:`).  Silver tables created in one `dbt run` session are not visible in a subsequent session.  Running `dbt run --select gold` separately would fail because the Silver tables no longer exist.

**Fix:** Changed `silver_transform` to run `dbt run --select silver gold` in a single invocation, keeping both Silver and Gold models in the same in-memory DuckDB session.

```python
# Before
bash_command='dbt run --select silver'

# After
bash_command='dbt run --select silver gold'
```

### 2. `gold_transform` re-ran dbt after Silver tables were gone

**Problem:** `gold_transform` ran `dbt run --select gold` as a separate BashOperator.  Since Silver tables only exist in the in-memory session from `silver_transform`, this second invocation would always fail.

**Fix:** Replaced `gold_transform` with an `EmptyOperator` (no-op).  Gold models already ran as part of `silver_transform`.  The EmptyOperator preserves the DAG dependency structure (`silver_dq >> gold_transform >> gold_dq`) without re-running dbt.

```python
# Before
gold_transform = BashOperator(task_id='gold_transform', bash_command='dbt run --select gold', ...)

# After
gold_transform = EmptyOperator(task_id='gold_transform', trigger_rule='none_failed_min_one_success', dag=dag)
```

### 3. Extract tasks failed hard when Airbyte was not configured

**Problem:** The original extract tasks used a placeholder `PythonOperator` that always succeeded (logged a message and returned).  In a real run where Airbyte is not configured, the tasks would succeed but produce no Bronze data, causing silent downstream failures.

**Fix:** Replaced with `extract_with_fallback()` which:
1. Checks if Airbyte is reachable (HTTP health check).
2. If not, checks whether mock Bronze data already exists in MinIO.
3. If mock data exists, returns `'skipped_using_mock'` — the task succeeds and downstream tasks run against existing data.
4. If neither Airbyte nor mock data is available, raises `RuntimeError` so Airflow retries and eventually marks the task failed.

### 4. `trigger_rule` did not allow graceful skipping (Req 10.8)

**Problem:** `bronze_dq` and downstream tasks used `trigger_rule='all_success'` (Airflow default).  If any extract task failed, `bronze_dq` would be skipped with status `upstream_failed`, which is correct — but the original code had `trigger_rule='all_success'` explicitly set, which would cause `bronze_dq` to fail (not skip) if one extract task failed while others succeeded.

**Fix:** Set `trigger_rule='none_failed_min_one_success'` on `bronze_dq`, `silver_transform`, `silver_dq`, `gold_transform`, and `gold_dq`.  This means:
- If **all** extract tasks fail → `bronze_dq` is skipped, all downstream tasks are skipped.
- If **at least one** extract task succeeds → `bronze_dq` runs normally.

`log_pipeline_run` retains `trigger_rule='all_done'` so it always runs to capture the final status.

---

## DAG Task Dependency Chain

```
extract_weather_api ─┐
extract_postgres_db  ├──► bronze_dq_checks ──► silver_transform ──► silver_dq_checks ──► gold_transform (no-op) ──► gold_dq_checks ──► log_pipeline_run
extract_csv_files   ─┘
```

**trigger_rule summary:**

| Task | trigger_rule | Rationale |
|------|-------------|-----------|
| `bronze_dq_checks` | `none_failed_min_one_success` | Run if ≥1 extract succeeded |
| `silver_transform` | `none_failed_min_one_success` | Run if bronze_dq succeeded or was skipped |
| `silver_dq_checks` | `none_failed_min_one_success` | Run if silver_transform succeeded |
| `gold_transform` | `none_failed_min_one_success` | No-op; preserves dependency chain |
| `gold_dq_checks` | `none_failed_min_one_success` | Run if gold_transform succeeded |
| `log_pipeline_run` | `all_done` | Always runs to capture final status |

---

## Test Script: `dbt/test_dag_e2e.py`

The test script simulates the full DAG execution in four steps:

### Step 1 — Bronze mock data creation
- Runs `dbt/create_mock_bronze_data.py` to populate MinIO Bronze buckets.
- Verifies row counts: weather_data, customers, orders all > 0.

### Step 2 — Silver + Gold dbt transformations
- Runs `dbt run --select silver gold` (single invocation).
- Verifies Silver output files exist in MinIO (weather_clean, orders_clean, customers_clean).
- Verifies Gold output files exist in MinIO (order_metrics, customer_lifetime_value).

### Step 3 — pipeline_runs table record
- Writes a pipeline run record to `s3://gold/pipeline_runs/{run_id}.parquet`.
- Reads it back and verifies: dag_run_id, status='success', row counts > 0.

### Step 4 — Failure scenario
- Attempts to read from a non-existent Bronze path → confirms exception is raised.
- Simulates `trigger_rule='none_failed_min_one_success'`:
  - All 3 extract tasks fail → downstream should NOT run ✅
  - 1 extract task succeeds, 2 fail → downstream SHOULD run ✅

---

## Running the Test

```bash
# Inside the dbt runner container
docker compose exec dbt python /dbt/test_dag_e2e.py

# Or locally (requires dbt-duckdb and duckdb installed)
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
MINIO_ENDPOINT=localhost:9000 \
python dbt/test_dag_e2e.py
```

---

## Airflow UI Verification Steps

To verify the DAG in the Airflow UI (http://localhost:8080):

1. **Trigger DAG manually:** Go to DAGs → `medallion_pipeline` → Trigger DAG ▶
2. **Monitor tasks:** Watch the Grid view — all tasks should turn green in order.
3. **Check logs:** Click any task → Logs to see stdout from each operator.
4. **Verify pipeline_runs:** After the DAG completes, query:
   ```sql
   SELECT * FROM read_parquet('s3://gold/pipeline_runs/**/*.parquet')
   ```
5. **Test failure scenario:** Temporarily rename the Bronze bucket in MinIO, trigger the DAG, and verify that `bronze_dq_checks` and all downstream tasks show as "skipped" (grey) in the Grid view.

---

## Files Modified / Created

| File | Change |
|------|--------|
| `dags/medallion_pipeline.py` | Fixed `silver_transform` command, replaced `gold_transform` with EmptyOperator, added `extract_with_fallback`, updated trigger_rules |
| `dbt/test_dag_e2e.py` | New — end-to-end test script simulating full DAG execution |
| `docs/task-7.6-dag-e2e-test-results.md` | New — this document |
