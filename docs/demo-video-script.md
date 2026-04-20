# Demo Video Script

**Title:** Modern Batch ELT Pipeline with Medallion Architecture — End-to-End Demo

**Target length:** 8–12 minutes

**Recommended tools:** OBS Studio, Loom, or QuickTime (macOS)

---

## Pre-Recording Checklist

- [ ] All Docker services are running: `docker compose ps` shows all healthy
- [ ] Mock Bronze data has been generated: `docker exec dbt python /dbt/create_mock_bronze_data.py`
- [ ] Browser tabs open and logged in: MinIO (9001), Airbyte (8000), Airflow (8080), Superset (8088)
- [ ] Terminal window open at the project root
- [ ] Screen resolution set to 1920×1080
- [ ] Notifications silenced

---

## Scene 1 — Introduction (0:00–0:45)

**What to show:** Terminal at project root, `docker compose ps` output

**Script:**
> "This is a fully local data engineering pipeline implementing the Medallion Architecture — Bronze, Silver, and Gold layers — using Apache Iceberg, MinIO, Airbyte, dbt, Airflow, and Superset. Everything runs in Docker with a single `docker compose up`. Let me walk you through the full end-to-end flow."

**Actions:**
1. Show the terminal with `docker compose ps` — all services healthy
2. Briefly pan over the repository structure with `ls -la`

---

## Scene 2 — Architecture Overview (0:45–1:30)

**What to show:** README.md in browser or VS Code, Mermaid architecture diagram

**Script:**
> "The architecture follows an ELT pattern. Airbyte extracts data from three sources — a REST API, a PostgreSQL database, and flat files — and loads raw records into the Bronze layer on MinIO. dbt then transforms Bronze into Silver, cleaning and standardizing the data, and aggregates Silver into Gold business metrics. Airflow orchestrates the whole pipeline on a daily schedule, and Superset visualizes the Gold layer."

**Actions:**
1. Open README.md and scroll to the architecture diagram
2. Point out each component in the Mermaid graph

---

## Scene 3 — MinIO Object Storage (1:30–2:30)

**What to show:** MinIO Console at http://localhost:9001

**Script:**
> "MinIO is our S3-compatible object storage — the data lake backend. All three layers live here as Parquet files. Let me show you the bucket structure."

**Actions:**
1. Open MinIO Console, log in
2. Show the four buckets: `bronze`, `silver`, `gold`, `iceberg-warehouse`
3. Navigate into `bronze/` and show the source-partitioned directory structure
4. Navigate into `iceberg-warehouse/` and show the Iceberg metadata JSON files
5. Say: "The `iceberg-warehouse` bucket holds all the table metadata — schemas, snapshots, partition specs. This is what enables time travel and schema evolution."

---

## Scene 4 — Airbyte Data Ingestion (2:30–3:45)

**What to show:** Airbyte UI at http://localhost:8000

**Script:**
> "Airbyte handles the Extract and Load steps. We have three source connectors configured."

**Actions:**
1. Open Airbyte, navigate to Connections
2. Show the three connections (Weather API, PostgreSQL, File source)
3. Click the Weather API connection, show the sync history
4. Click into a completed sync job, show the log output and records synced count
5. Navigate to Destinations, show the S3/MinIO destination configured with Parquet output
6. Say: "All connection configs are stored as YAML in the `connections/` directory — infrastructure as code, no manual UI steps needed to recreate them."

---

## Scene 5 — Triggering the Pipeline in Airflow (3:45–5:15)

**What to show:** Airflow UI at http://localhost:8080

**Script:**
> "Airflow orchestrates the full pipeline. Let me show you the DAG and trigger a run."

**Actions:**
1. Open Airflow, show the DAG list with `medallion_pipeline`
2. Click the DAG, show the Graph view with the full task dependency chain
3. Point out the three parallel extract tasks feeding into `bronze_dq_checks`
4. Trigger a manual run by clicking the play button
5. Switch to the Grid view and watch tasks turn green in real time
6. Once complete, click `silver_transform` task → Logs
7. Show the dbt run output: model names, execution times, row counts
8. Say: "Each task retries up to twice on failure, and the final `log_pipeline_run` task always runs — even if something upstream fails — so we always have a record of what happened."

---

## Scene 6 — dbt Transformations (5:15–6:30)

**What to show:** Terminal running dbt commands

**Script:**
> "Let me show the dbt transformations directly. dbt defines all our SQL logic as version-controlled models with built-in tests."

**Actions:**
1. Open a terminal
2. Run: `docker exec dbt dbt run --select silver gold`
3. Show the output — model names, execution order, row counts
4. Run: `docker exec dbt dbt test --select silver`
5. Show the test output — unique and not_null checks passing
6. Open `dbt/models/silver/orders_clean.sql` in an editor
7. Point out the incremental materialization config and the cleaning logic (type casting, deduplication, null handling)
8. Say: "The Silver models use incremental materialization — on re-runs, only new records are merged in using the primary key. This is what makes the pipeline idempotent."

---

## Scene 7 — Gold Layer and Superset Dashboards (6:30–8:30)

**What to show:** Superset UI at http://localhost:8088

**Script:**
> "The Gold layer contains pre-aggregated business metrics. Superset connects directly to these Gold tables."

**Actions:**
1. Open Superset, navigate to Dashboards
2. Open "Business KPIs" dashboard
3. Walk through each chart:
   - Daily order metrics: "Total orders and revenue aggregated by day"
   - Customer lifetime value: "Top customers ranked by total spend"
   - Weather trends: "Temperature and humidity from the weather API source"
4. Open "Pipeline Metrics" dashboard
5. Walk through:
   - DAG run history table: "Every pipeline run logged with status and duration"
   - DQ check results: "Data quality checks at each layer boundary"
6. Open SQL Lab, run a quick query against `order_metrics`
7. Say: "Every chart here is backed by a Gold Iceberg table. The data flows from raw API responses all the way to this dashboard without any manual steps."

---

## Scene 8 — Advanced Features: Time Travel (8:30–9:30)

**What to show:** Terminal running time travel queries

**Script:**
> "One of the most powerful Iceberg features is time travel — querying data as it existed at any prior snapshot."

**Actions:**
1. Open a terminal
2. Run: `docker exec dbt python /dbt/test_time_travel.py`
3. Show the output listing snapshots with timestamps
4. Show the time travel query result comparing two snapshots
5. Say: "Every write to an Iceberg table creates an immutable snapshot. You can query any of them by snapshot ID or timestamp — no data is ever deleted, just superseded."

---

## Scene 9 — Schema Evolution (9:30–10:15)

**What to show:** Terminal running schema evolution demo

**Script:**
> "Schema evolution lets you add columns to a table without rewriting any existing Parquet files."

**Actions:**
1. Run: `docker exec dbt python /dbt/test_schema_evolution.py`
2. Show the output: new column added, existing records show NULL for the new column
3. Say: "The existing Parquet files on MinIO are untouched. Iceberg just updates the metadata to include the new column definition. Old files are still readable — they just return NULL for the new column."

---

## Scene 10 — Wrap-Up (10:15–11:00)

**What to show:** README.md, repository structure

**Script:**
> "That's the full end-to-end pipeline. To summarize what we've seen:
> - MinIO as a local S3-compatible data lake
> - Airbyte ingesting from three source types into the Bronze layer
> - dbt transforming Bronze → Silver → Gold with built-in tests
> - Airflow orchestrating the full pipeline with retry logic and observability
> - Superset dashboards on the Gold layer
> - Apache Iceberg providing ACID transactions, time travel, and schema evolution
>
> The entire stack runs locally with `docker compose up` — no cloud account needed. Check the README for quickstart instructions and the `docs/` directory for detailed runbooks on each component."

**Actions:**
1. Show the README quickstart section
2. Show the `docs/` directory listing
3. End on the Airflow DAG graph view — a clean visual of the full pipeline

---

## Post-Recording Checklist

- [ ] Trim dead air at start and end
- [ ] Add chapter markers at each scene boundary
- [ ] Add captions/subtitles for accessibility
- [ ] Export at 1080p, H.264, ~10 Mbps bitrate
- [ ] Upload to YouTube (unlisted) or Loom
- [ ] Add video link to README.md Demo Materials section
