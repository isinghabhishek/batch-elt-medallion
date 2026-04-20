# Demo Screenshot Guide

This guide documents exactly what to capture for each UI in the Medallion Pipeline stack, including step-by-step instructions and file naming conventions.

## Prerequisites

Ensure all services are running before capturing screenshots:

```bash
docker compose up -d
docker compose ps   # all core services should show "healthy"
```

---

## 1. MinIO Console — http://localhost:9001

Login: `admin` / `<MINIO_ROOT_PASSWORD>` (from your `.env` file)

### 1.1 Bucket Overview

**File:** `docs/screenshots/minio-buckets.png`

Steps:
1. Open http://localhost:9001 and log in
2. Click "Buckets" in the left sidebar
3. Confirm all four buckets are visible: `bronze`, `silver`, `gold`, `iceberg-warehouse`
4. Capture the full browser window showing the bucket list

### 1.2 Bronze Bucket Contents

**File:** `docs/screenshots/minio-bronze-contents.png`

Steps:
1. Click the `bronze` bucket
2. Navigate into any source folder (e.g., `weather_data/`)
3. Expand the directory tree to show Parquet files
4. Capture the file browser showing the nested path structure

### 1.3 Iceberg Warehouse Metadata

**File:** `docs/screenshots/minio-iceberg-warehouse.png`

Steps:
1. Click the `iceberg-warehouse` bucket
2. Navigate into any namespace folder (e.g., `silver/weather_clean/`)
3. Show the `metadata/` directory containing snapshot JSON files
4. Capture the metadata file listing

### 1.4 Object Preview

**File:** `docs/screenshots/minio-object-preview.png`

Steps:
1. Navigate to any Parquet file in the `bronze` bucket
2. Click the file to open the object detail panel
3. Show the object metadata (size, last modified, content type)
4. Capture the detail panel

---

## 2. Airbyte UI — http://localhost:8000

### 2.1 Connections Overview

**File:** `docs/screenshots/airbyte-connections.png`

Steps:
1. Open http://localhost:8000
2. Click "Connections" in the left sidebar
3. Show all three configured connections:
   - Weather API → MinIO (bronze)
   - PostgreSQL seed_db → MinIO (bronze)
   - File source → MinIO (bronze)
4. Capture the connections list with status indicators

### 2.2 Connection Detail — Weather API

**File:** `docs/screenshots/airbyte-weather-connection.png`

Steps:
1. Click the Weather API connection
2. Show the "Status" tab with sync history
3. Expand the most recent successful sync job
4. Capture the job detail showing records synced and duration

### 2.3 Source Configuration

**File:** `docs/screenshots/airbyte-source-config.png`

Steps:
1. Click "Sources" in the left sidebar
2. Click the Open-Meteo Weather API source
3. Show the source configuration form (URL, stream settings)
4. Capture the configuration panel

### 2.4 Destination Configuration

**File:** `docs/screenshots/airbyte-destination-config.png`

Steps:
1. Click "Destinations" in the left sidebar
2. Click the S3/MinIO destination
3. Show the destination settings (endpoint, bucket, format: Parquet)
4. Capture the configuration panel

### 2.5 Sync Job Logs

**File:** `docs/screenshots/airbyte-sync-logs.png`

Steps:
1. Navigate to any connection's sync history
2. Click a completed sync job
3. Expand the log output showing extraction and load steps
4. Capture the log panel showing successful completion

---

## 3. Airflow UI — http://localhost:8080

Login: `admin` / `admin`

### 3.1 DAG List

**File:** `docs/screenshots/airflow-dag-list.png`

Steps:
1. Open http://localhost:8080
2. The DAGs list is the home page
3. Locate `medallion_pipeline` in the list
4. Ensure it shows as active (toggle is on, no import errors)
5. Capture the full DAG list view

### 3.2 DAG Graph View

**File:** `docs/screenshots/airflow-dag-graph.png`

Steps:
1. Click `medallion_pipeline` to open the DAG
2. Click the "Graph" tab
3. The graph should show the full task dependency chain:
   `extract_weather_api`, `extract_postgres_db`, `extract_csv_files` → `bronze_dq_checks` → `silver_transform` → `silver_dq_checks` → `gold_transform` → `gold_dq_checks` → `log_pipeline_run`
4. Capture the full graph with all nodes and edges visible

### 3.3 Successful DAG Run

**File:** `docs/screenshots/airflow-successful-run.png`

Steps:
1. Trigger a manual DAG run: click the play button on `medallion_pipeline`
2. Wait for the run to complete
3. Click the run in the "Runs" tab
4. Show all tasks in green (success) state
5. Capture the grid view showing the completed run

### 3.4 Task Logs

**File:** `docs/screenshots/airflow-task-logs.png`

Steps:
1. From a completed DAG run, click any task (e.g., `silver_transform`)
2. Click "Log" to open the task log
3. Scroll to show the dbt run output with model execution times
4. Capture the log output showing successful dbt run

### 3.5 Pipeline Run History

**File:** `docs/screenshots/airflow-run-history.png`

Steps:
1. Click the "Calendar" or "Runs" tab on the `medallion_pipeline` DAG
2. Show multiple run entries with their statuses and durations
3. Capture the run history view

---

## 4. Superset UI — http://localhost:8088

Login: `admin` / `admin`

### 4.1 Dashboard List

**File:** `docs/screenshots/superset-dashboard-list.png`

Steps:
1. Open http://localhost:8088
2. Click "Dashboards" in the top navigation
3. Show both dashboards: "Pipeline Metrics" and "Business KPIs"
4. Capture the dashboard list

### 4.2 Pipeline Metrics Dashboard

**File:** `docs/screenshots/superset-pipeline-metrics.png`

Steps:
1. Click "Pipeline Metrics" dashboard
2. Wait for all charts to load
3. The dashboard should show:
   - DAG run history table
   - Row counts by layer (bar chart)
   - DQ check pass/fail history
   - Pipeline run duration over time
4. Capture the full dashboard with all charts visible

### 4.3 Business KPIs Dashboard

**File:** `docs/screenshots/superset-business-kpis.png`

Steps:
1. Click "Business KPIs" dashboard
2. Wait for all charts to load
3. The dashboard should show:
   - Daily weather trends (line chart)
   - Order metrics over time
   - Top customers by lifetime value (bar chart)
   - Revenue trends
4. Capture the full dashboard with all charts visible

### 4.4 Dataset Configuration

**File:** `docs/screenshots/superset-datasets.png`

Steps:
1. Click "Data" → "Datasets" in the top navigation
2. Show all Gold layer datasets:
   - `pipeline_runs`
   - `order_metrics`
   - `customer_lifetime_value`
   - `dq_results`
3. Capture the dataset list

### 4.5 SQL Lab Query

**File:** `docs/screenshots/superset-sql-lab.png`

Steps:
1. Click "SQL" → "SQL Lab" in the top navigation
2. Select the DuckDB database connection
3. Run a sample query against a Gold table:
   ```sql
   SELECT order_date, total_orders, total_revenue
   FROM order_metrics
   ORDER BY order_date DESC
   LIMIT 10
   ```
4. Capture the query editor with results visible

---

## Screenshot Checklist

| Screenshot | File | Status |
|------------|------|--------|
| MinIO bucket overview | `docs/screenshots/minio-buckets.png` | ☐ |
| MinIO bronze contents | `docs/screenshots/minio-bronze-contents.png` | ☐ |
| MinIO iceberg warehouse | `docs/screenshots/minio-iceberg-warehouse.png` | ☐ |
| MinIO object preview | `docs/screenshots/minio-object-preview.png` | ☐ |
| Airbyte connections list | `docs/screenshots/airbyte-connections.png` | ☐ |
| Airbyte weather connection | `docs/screenshots/airbyte-weather-connection.png` | ☐ |
| Airbyte source config | `docs/screenshots/airbyte-source-config.png` | ☐ |
| Airbyte destination config | `docs/screenshots/airbyte-destination-config.png` | ☐ |
| Airbyte sync logs | `docs/screenshots/airbyte-sync-logs.png` | ☐ |
| Airflow DAG list | `docs/screenshots/airflow-dag-list.png` | ☐ |
| Airflow DAG graph | `docs/screenshots/airflow-dag-graph.png` | ☐ |
| Airflow successful run | `docs/screenshots/airflow-successful-run.png` | ☐ |
| Airflow task logs | `docs/screenshots/airflow-task-logs.png` | ☐ |
| Airflow run history | `docs/screenshots/airflow-run-history.png` | ☐ |
| Superset dashboard list | `docs/screenshots/superset-dashboard-list.png` | ☐ |
| Superset pipeline metrics | `docs/screenshots/superset-pipeline-metrics.png` | ☐ |
| Superset business KPIs | `docs/screenshots/superset-business-kpis.png` | ☐ |
| Superset datasets | `docs/screenshots/superset-datasets.png` | ☐ |
| Superset SQL Lab | `docs/screenshots/superset-sql-lab.png` | ☐ |

---

## Tips for Good Screenshots

- Use a browser zoom level of 100% for consistent sizing
- Maximize the browser window before capturing
- Use a tool like macOS Screenshot (`Cmd+Shift+4`), Flameshot (Linux), or Snipping Tool (Windows)
- Recommended resolution: 1920×1080 or higher
- Save as PNG for lossless quality
- Redact any sensitive credentials visible in the UI before publishing
