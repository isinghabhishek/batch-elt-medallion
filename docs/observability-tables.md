# Observability Tables Documentation

## Overview

The observability tables provide comprehensive monitoring and auditing capabilities for the Medallion ELT pipeline. These tables are part of the Gold layer and track pipeline execution metadata and data quality check results.

## Tables

### 1. pipeline_runs

**Purpose**: Track DAG run metadata for pipeline monitoring and performance analysis.

**Location**: `dbt/models/gold/pipeline_runs.sql`

**Schema**:

| Column | Type | Description |
|--------|------|-------------|
| `dag_run_id` | VARCHAR | Unique identifier for the DAG run (from Airflow) |
| `start_time` | TIMESTAMP | When the pipeline run started |
| `end_time` | TIMESTAMP | When the pipeline run completed (NULL if still running) |
| `status` | VARCHAR | Run status: 'success', 'failed', or 'running' |
| `bronze_row_count` | INTEGER | Total rows ingested into Bronze layer |
| `silver_row_count` | INTEGER | Total rows processed into Silver layer |
| `gold_row_count` | INTEGER | Total rows aggregated into Gold layer |
| `duration_seconds` | INTEGER | Total pipeline execution time in seconds |

**Partitioning**: Partitioned by `days(start_time)` for efficient time-range queries

**Population**: Populated by Airflow tasks at the end of each DAG run

**Usage Examples**:

```sql
-- Get last 10 pipeline runs
SELECT * FROM pipeline_runs 
ORDER BY start_time DESC 
LIMIT 10;

-- Calculate average pipeline duration
SELECT AVG(duration_seconds) as avg_duration_sec
FROM pipeline_runs
WHERE status = 'success'
  AND start_time >= CURRENT_DATE - INTERVAL '30 days';

-- Monitor pipeline success rate
SELECT 
    status,
    COUNT(*) as run_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage
FROM pipeline_runs
WHERE start_time >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY status;
```

### 2. dq_results

**Purpose**: Track data quality check results across all layers for auditing and debugging.

**Location**: `dbt/models/gold/dq_results.sql`

**Schema**:

| Column | Type | Description |
|--------|------|-------------|
| `check_id` | VARCHAR | Unique identifier for the check execution |
| `check_name` | VARCHAR | Name of the data quality check |
| `layer` | VARCHAR | Layer where check was executed: 'bronze', 'silver', or 'gold' |
| `table_name` | VARCHAR | Name of the table being checked |
| `check_timestamp` | TIMESTAMP | When the check was executed |
| `status` | VARCHAR | Check result: 'passed' or 'failed' |
| `row_count` | INTEGER | Number of rows checked (if applicable) |
| `error_message` | VARCHAR | Error details if check failed (NULL if passed) |

**Partitioning**: Partitioned by `days(check_timestamp)` for efficient time-range queries

**Population**: Populated by:
- Airflow tasks (Bronze DQ checks)
- dbt test results (Silver and Gold DQ checks)
- Custom data quality operators

**Usage Examples**:

```sql
-- Get all failed checks in the last 24 hours
SELECT * FROM dq_results
WHERE status = 'failed'
  AND check_timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
ORDER BY check_timestamp DESC;

-- Calculate DQ pass rate by layer
SELECT 
    layer,
    COUNT(*) as total_checks,
    SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) as passed_checks,
    ROUND(SUM(CASE WHEN status = 'passed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as pass_rate
FROM dq_results
WHERE check_timestamp >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY layer;

-- Find tables with recurring DQ failures
SELECT 
    table_name,
    check_name,
    COUNT(*) as failure_count,
    MAX(check_timestamp) as last_failure
FROM dq_results
WHERE status = 'failed'
  AND check_timestamp >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY table_name, check_name
HAVING COUNT(*) > 1
ORDER BY failure_count DESC;
```

## Implementation Details

### Table Creation

Both tables are created as empty table definitions using dbt models with the following characteristics:

- **Materialization**: `table` (not incremental, as they're populated externally)
- **Schema Evolution**: `on_schema_change='fail'` (strict schema enforcement)
- **Initial State**: Empty (WHERE 1 = 0 clause ensures no initial rows)

### Integration with Airflow

The Airflow DAG (`dags/medallion_pipeline.py`) should include tasks to populate these tables:

```python
# Example: Log pipeline run at end of DAG
def log_pipeline_run(**context):
    from duckdb import connect
    import os
    
    conn = connect(':memory:')
    # ... configure S3 and Iceberg ...
    
    dag_run_id = context['dag_run'].run_id
    start_time = context['dag_run'].start_date
    end_time = datetime.now()
    status = 'success'
    duration = (end_time - start_time).total_seconds()
    
    conn.execute(f"""
        INSERT INTO pipeline_runs
        VALUES ('{dag_run_id}', '{start_time}', '{end_time}', '{status}', 
                NULL, NULL, NULL, {duration})
    """)
    conn.close()

log_run = PythonOperator(
    task_id='log_pipeline_run',
    python_callable=log_pipeline_run,
    provide_context=True,
    dag=dag,
)
```

### Integration with dbt Tests

dbt test results can be captured and inserted into `dq_results`:

```python
# Example: Parse dbt test results and log to dq_results
def log_dbt_test_results(**context):
    import json
    
    # Read dbt test results
    with open('/dbt/target/run_results.json', 'r') as f:
        results = json.load(f)
    
    conn = connect(':memory:')
    # ... configure S3 and Iceberg ...
    
    for result in results['results']:
        if result['resource_type'] == 'test':
            check_id = result['unique_id']
            check_name = result['name']
            status = 'passed' if result['status'] == 'pass' else 'failed'
            error_msg = result.get('message', None)
            
            conn.execute(f"""
                INSERT INTO dq_results
                VALUES ('{check_id}', '{check_name}', 'silver', 'unknown',
                        CURRENT_TIMESTAMP, '{status}', NULL, '{error_msg}')
            """)
    
    conn.close()
```

## Superset Dashboard Integration

These tables are designed to power the "Pipeline Metrics" dashboard in Superset:

### Recommended Charts

1. **DAG Run History** (Table)
   - Dataset: `pipeline_runs`
   - Columns: `dag_run_id`, `start_time`, `status`, `duration_seconds`
   - Filter: Last 30 days

2. **Pipeline Success Rate** (Pie Chart)
   - Dataset: `pipeline_runs`
   - Metric: Count
   - Group By: `status`
   - Filter: Last 7 days

3. **Row Counts by Layer** (Bar Chart)
   - Dataset: `pipeline_runs`
   - Metrics: `bronze_row_count`, `silver_row_count`, `gold_row_count`
   - X-axis: `start_time` (daily)

4. **DQ Check Pass/Fail History** (Line Chart)
   - Dataset: `dq_results`
   - Metric: Count
   - Group By: `status`, `layer`
   - X-axis: `check_timestamp` (daily)

5. **Pipeline Duration Trend** (Line Chart)
   - Dataset: `pipeline_runs`
   - Metric: `AVG(duration_seconds)`
   - X-axis: `start_time` (daily)
   - Filter: `status = 'success'`

## Maintenance

### Data Retention

Consider implementing data retention policies to prevent unbounded growth:

```sql
-- Delete pipeline runs older than 90 days
DELETE FROM pipeline_runs
WHERE start_time < CURRENT_DATE - INTERVAL '90 days';

-- Delete DQ results older than 90 days
DELETE FROM dq_results
WHERE check_timestamp < CURRENT_DATE - INTERVAL '90 days';
```

### Monitoring Queries

```sql
-- Check for recent pipeline failures
SELECT * FROM pipeline_runs
WHERE status = 'failed'
  AND start_time >= CURRENT_DATE - INTERVAL '7 days';

-- Check for tables with no recent DQ checks
SELECT DISTINCT table_name
FROM dq_results
WHERE check_timestamp < CURRENT_DATE - INTERVAL '7 days';
```

## Requirements Mapping

These observability tables satisfy the following requirements:

- **Requirement 9.6**: Log DQ check results to `dq_results` table for auditability
- **Requirement 15.3**: Write pipeline run summary records to `pipeline_runs` table
- **Requirement 15.5**: Enable Superset charts displaying pipeline metrics

## Next Steps

1. **Phase 7 (Task 7.3)**: Implement Airflow task to populate `pipeline_runs`
2. **Phase 7 (Task 7.2)**: Integrate dbt test results into `dq_results`
3. **Phase 9 (Task 9.3)**: Build Superset dashboard using these tables
4. **Phase 11 (Task 11.6)**: Document observability patterns and debugging workflows
