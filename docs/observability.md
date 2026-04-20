# Observability and Logging Guide

This guide covers how to monitor the pipeline, access logs, query observability tables, and debug failures.

---

## Logging Strategy

The pipeline produces logs at three levels:

| Level | Source | Location |
|-------|--------|----------|
| Task logs | Airflow | Airflow UI + `logs/` volume |
| Transformation logs | dbt | stdout / `dbt/target/run_results.json` |
| Pipeline run records | Airflow PythonOperator | `s3://gold/pipeline_runs/*.parquet` |
| DQ check records | Airflow `on_failure_callback` | `s3://gold/dq_results/*.parquet` |

### Log Format

All pipeline components log errors with:
- **Timestamp** — when the error occurred
- **Component name** — which service or task produced the log
- **Error type** — exception class or check name
- **Human-readable message** — what went wrong and relevant context

---

## Accessing Airflow Task Logs

### Via the Airflow UI

1. Open http://localhost:8080
2. Click on the `medallion_pipeline` DAG
3. Click on a DAG run (circle in the grid)
4. Click on a task instance
5. Click **Log** to view the full task output

### Via the CLI

```bash
# List recent DAG runs
docker exec airflow-webserver airflow dags list-runs -d medallion_pipeline

# View logs for a specific task instance
docker exec airflow-webserver airflow tasks logs medallion_pipeline <task_id> <execution_date>
# Example:
docker exec airflow-webserver airflow tasks logs medallion_pipeline silver_transform 2024-01-15T00:00:00+00:00
```

### Via the filesystem

Airflow task logs are written to the `logs/` directory mounted from the host:

```
logs/
├── dag_processor_manager/
│   └── dag_processor_manager.log    # DAG parsing errors
└── scheduler/
    └── YYYY-MM-DD/
        └── medallion_pipeline.py.log  # Scheduler activity
```

Task-level logs are stored in the Airflow metadata database and accessible via the UI. For persistent log storage, configure `AIRFLOW__LOGGING__REMOTE_LOGGING=True` with an S3 backend.

---

## Observability Tables

Two Parquet-based tables in the Gold layer track pipeline health. They are written by Airflow tasks (not dbt models).

### `pipeline_runs` — DAG Run Summary

**Location**: `s3://gold/pipeline_runs/*.parquet`

**Schema**:
| Column | Type | Description |
|--------|------|-------------|
| `dag_run_id` | STRING | Unique Airflow DAG run identifier |
| `start_time` | TIMESTAMP | When the pipeline run started |
| `end_time` | TIMESTAMP | When the pipeline run completed |
| `status` | STRING | `success`, `failed`, or `running` |
| `bronze_row_count` | INTEGER | Total rows in Bronze layer at run time |
| `silver_row_count` | INTEGER | Total rows across Silver tables |
| `gold_row_count` | INTEGER | Total rows across Gold tables |
| `duration_seconds` | INTEGER | Total pipeline duration in seconds |

**Written by**: `log_pipeline_run` task in `dags/medallion_pipeline.py`

---

### `dq_results` — Data Quality Check Results

**Location**: `s3://gold/dq_results/*.parquet`

**Schema**:
| Column | Type | Description |
|--------|------|-------------|
| `check_id` | STRING | UUID for this check instance |
| `check_name` | STRING | Name of the DQ check (e.g., `silver_dq_checks`) |
| `layer` | STRING | `bronze`, `silver`, or `gold` |
| `table_name` | STRING | Table being checked |
| `check_timestamp` | TIMESTAMP | When the check ran |
| `status` | STRING | `passed` or `failed` |
| `row_count` | INTEGER | Rows affected (if applicable) |
| `error_message` | STRING | Error details if check failed |

**Written by**: `on_failure_callback` in `dags/medallion_pipeline.py` when a DQ task fails

---

## Querying Observability Tables

Use DuckDB to query the observability Parquet files directly from MinIO.

### Setup

```python
import duckdb

conn = duckdb.connect(':memory:')
conn.execute("INSTALL httpfs; LOAD httpfs;")
conn.execute("""
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='admin';
    SET s3_secret_access_key='changeme123';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")
```

### Recent pipeline runs

```sql
SELECT
    dag_run_id,
    start_time,
    status,
    duration_seconds,
    bronze_row_count,
    silver_row_count,
    gold_row_count
FROM read_parquet('s3://gold/pipeline_runs/*.parquet')
ORDER BY start_time DESC
LIMIT 10;
```

### Failed runs in the last 7 days

```sql
SELECT *
FROM read_parquet('s3://gold/pipeline_runs/*.parquet')
WHERE status = 'failed'
  AND start_time >= CURRENT_TIMESTAMP - INTERVAL '7 days'
ORDER BY start_time DESC;
```

### DQ check failures by layer

```sql
SELECT
    layer,
    check_name,
    COUNT(*) AS failure_count,
    MAX(check_timestamp) AS last_failure
FROM read_parquet('s3://gold/dq_results/*.parquet')
WHERE status = 'failed'
GROUP BY layer, check_name
ORDER BY failure_count DESC;
```

### Average pipeline duration over time

```sql
SELECT
    DATE_TRUNC('day', start_time) AS run_date,
    COUNT(*) AS run_count,
    AVG(duration_seconds) AS avg_duration_seconds,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failures
FROM read_parquet('s3://gold/pipeline_runs/*.parquet')
GROUP BY 1
ORDER BY 1 DESC;
```

---

## dbt Run Results

After every `dbt run` or `dbt test`, dbt writes a results file:

```
dbt/target/run_results.json
```

This file contains per-model execution times, row counts, and status. Parse it to understand which models are slow:

```bash
# View run results summary
docker exec dbt cat /dbt/target/run_results.json | python -c "
import json, sys
results = json.load(sys.stdin)
for r in results['results']:
    print(f\"{r['unique_id']}: {r['status']} ({r['execution_time']:.2f}s)\")
"
```

---

## Debugging Pipeline Failures

### Step 1: Identify the failing task

Open the Airflow UI at http://localhost:8080. Failed tasks show in red. Click the task to view its log.

### Step 2: Check the task log

Common failure patterns:

**dbt model failure**
```
Compilation Error in model weather_clean
  ...syntax error...
```
→ Fix the SQL in `dbt/models/silver/weather_clean.sql` and re-trigger.

**S3 connection failure**
```
ConnectionError: Could not connect to minio:9000
```
→ Check MinIO is healthy: `docker compose ps minio`
→ Verify credentials in `.env` match what dbt uses.

**Bronze data missing**
```
No files found matching s3://bronze/weather_data/**/*.parquet
```
→ Run the Bronze data generator: `docker exec dbt python /dbt/create_mock_bronze_data.py`
→ Or trigger an Airbyte sync manually.

**dbt test failure**
```
Failure in test unique_weather_clean_location_date
  Got 3 results, configured to fail if != 0
```
→ Duplicates exist in the Silver table. Check the Bronze source for duplicate records.
→ The `ROW_NUMBER()` deduplication in the Silver model should handle this — verify the `unique_key` config.

### Step 3: Re-run a specific task

```bash
# Re-run a single task without re-running the whole DAG
docker exec airflow-webserver airflow tasks run medallion_pipeline silver_transform <execution_date> --local
```

### Step 4: Check observability tables

```bash
# Query recent DQ failures
docker exec dbt python -c "
import duckdb
conn = duckdb.connect(':memory:')
conn.execute('INSTALL httpfs; LOAD httpfs;')
conn.execute(\"SET s3_endpoint='minio:9000'; SET s3_access_key_id='admin'; SET s3_secret_access_key='changeme123'; SET s3_use_ssl=false; SET s3_url_style='path';\")
try:
    rows = conn.execute(\"SELECT * FROM read_parquet('s3://gold/dq_results/*.parquet') ORDER BY check_timestamp DESC LIMIT 5\").fetchall()
    for r in rows: print(r)
except Exception as e:
    print('No DQ results yet:', e)
"
```

### Step 5: Validate data at each layer

```bash
# Check Bronze data exists
docker exec minio mc ls local/bronze/ --recursive | head -20

# Check Silver data
docker exec minio mc ls local/silver/ --recursive | head -20

# Check Gold data
docker exec minio mc ls local/gold/ --recursive | head -20
```

---

## Log Retention

Airflow retains task logs for the number of DAG runs configured in `AIRFLOW__LOG_RETENTION_DAYS` (default: 30 days). Older logs are automatically cleaned up by the Airflow scheduler.

To adjust retention:
```yaml
# In docker-compose.yml, under airflow-webserver environment:
AIRFLOW__LOG_RETENTION_DAYS: 30
```

Observability Parquet files in MinIO are retained indefinitely. Implement a lifecycle policy in MinIO if storage becomes a concern:
```bash
# Set 90-day retention on pipeline_runs
docker exec minio mc ilm add local/gold --prefix pipeline_runs/ --expiry-days 90
```
