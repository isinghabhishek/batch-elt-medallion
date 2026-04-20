# Learning Milestones

This project is structured as a progressive learning path. Each milestone builds on the previous one and maps to a phase of the implementation. Complete them in order for the best learning experience.

---

## Milestone 1 — Local Data Lakehouse Infrastructure

**Phase**: Phase 1 (Infrastructure Setup)

**Learning Objectives**
- Understand how Docker Compose orchestrates multi-service data stacks
- Learn how MinIO provides S3-compatible object storage locally
- Understand the role of the Apache Iceberg REST Catalog in managing table metadata
- Practice reading Docker health checks and diagnosing startup failures

**What You Build**
A fully running local data lakehouse stack: MinIO for storage, Iceberg REST Catalog for metadata, PostgreSQL for operational databases, and supporting services for Airflow and Superset.

**Completion Criteria**
- [ ] `docker compose up -d` starts without errors
- [ ] All core services show `healthy` in `docker compose ps`
- [ ] MinIO Console is accessible at http://localhost:9001
- [ ] Four buckets exist: `bronze`, `silver`, `gold`, `iceberg-warehouse`
- [ ] Iceberg Catalog responds at http://localhost:8181/v1/config
- [ ] Airflow UI is accessible at http://localhost:8080
- [ ] Superset UI is accessible at http://localhost:8088

**Validation Steps**
```bash
# Check all services
docker compose ps

# Verify MinIO buckets
docker exec minio mc ls local/

# Verify Iceberg catalog
curl http://localhost:8181/v1/config

# Verify PostgreSQL databases
docker exec postgres psql -U postgres -c "\l"
```

**Key Takeaways**
- Docker Compose `depends_on` with `condition: service_healthy` ensures correct startup order
- MinIO's `minio_init` sidecar pattern is a common way to run one-time setup tasks
- Iceberg REST Catalog is stateless — it reads/writes metadata to S3, not a local database
- Health checks are critical for production-grade container orchestration

---

## Milestone 2 — Multi-Source Data Ingestion (Bronze Layer)

**Phase**: Phase 2 (Data Ingestion Setup)

**Learning Objectives**
- Understand the EL (Extract-Load) pattern and how Airbyte implements it
- Learn how raw data lands in a Bronze layer without transformation
- Understand Parquet as a columnar storage format for data lakes
- Practice configuring source connectors for different data types (API, database, files)

**What You Build**
Three Airbyte source connectors (REST API, PostgreSQL, CSV files) writing raw Parquet data to the Bronze layer in MinIO, with metadata columns added to every record.

**Completion Criteria**
- [ ] `connections/weather_api.yaml`, `connections/postgres_source.yaml`, `connections/file_source.yaml` exist
- [ ] Bronze Parquet files appear in MinIO under `s3://bronze/`
- [ ] Each Bronze record contains `_source_name`, `_ingested_at`, `_file_path`, `_has_null_key`
- [ ] Mock Bronze data can be generated via `python dbt/create_mock_bronze_data.py`
- [ ] Bronze data is readable with DuckDB: `SELECT COUNT(*) FROM read_parquet('s3://bronze/**/*.parquet')`

**Validation Steps**
```bash
# Generate mock Bronze data
docker exec dbt python /dbt/create_mock_bronze_data.py

# Verify Bronze data in MinIO
docker exec minio mc ls local/bronze/ --recursive

# Query Bronze data with DuckDB
docker exec dbt python -c "
import duckdb
conn = duckdb.connect(':memory:')
conn.execute(\"INSTALL httpfs; LOAD httpfs;\")
conn.execute(\"SET s3_endpoint='minio:9000'; SET s3_access_key_id='admin'; SET s3_secret_access_key='changeme123'; SET s3_use_ssl=false; SET s3_url_style='path';\")
print(conn.execute(\"SELECT _source_name, COUNT(*) FROM read_parquet('s3://bronze/**/*.parquet') GROUP BY 1\").fetchall())
"
```

**Key Takeaways**
- Bronze layer is append-only — raw data is never modified after landing
- Metadata columns (`_ingested_at`, `_source_name`) enable lineage tracking and incremental processing
- Parquet's columnar format makes it efficient for analytical queries even without a query engine
- Airbyte connection configs as YAML enable reproducible infrastructure-as-code

---

## Milestone 3 — Silver Layer: Cleaning and Standardization

**Phase**: Phase 3 (Silver Layer Transformations)

**Learning Objectives**
- Understand dbt's model-test-document workflow
- Learn incremental materialization and why it matters for large datasets
- Practice writing SQL transformations for data cleaning (type casting, deduplication, null handling)
- Understand how dbt schema tests enforce data quality contracts

**What You Build**
Three dbt Silver models (`weather_clean`, `orders_clean`, `customers_clean`) that clean, type-cast, deduplicate, and standardize Bronze data, with dbt schema tests enforcing primary key constraints.

**Completion Criteria**
- [ ] `dbt run --select silver` completes without errors
- [ ] Silver models produce records with correct types (DATE, DOUBLE, not raw strings)
- [ ] No duplicate primary keys in any Silver table
- [ ] No NULL primary keys in any Silver table
- [ ] Empty strings are converted to NULL
- [ ] `_silver_processed_at` column is populated on all records
- [ ] `dbt test --select silver` passes all schema tests

**Validation Steps**
```bash
# Run Silver transformations
docker exec dbt dbt run --select silver

# Run Silver tests
docker exec dbt dbt test --select silver

# Validate with Python script
docker exec dbt python /dbt/verify_silver_data.py

# Check for duplicates manually
docker exec dbt python -c "
import duckdb
conn = duckdb.connect(':memory:')
# ... configure S3 ...
result = conn.execute(\"SELECT order_id, COUNT(*) FROM read_parquet('s3://silver/orders_clean/**/*.parquet') GROUP BY 1 HAVING COUNT(*) > 1\").fetchall()
print('Duplicates:', result)
"
```

**Key Takeaways**
- dbt's `incremental` materialization uses `unique_key` to merge rather than append on re-runs
- `on_schema_change: append_new_columns` enables schema evolution without breaking existing pipelines
- dbt tests are SQL queries — a test "fails" when the query returns rows (violations)
- The Silver layer is the most important for data quality — bad data caught here never reaches Gold

---

## Milestone 4 — Gold Layer: Business Aggregations

**Phase**: Phase 4 (Gold Layer Aggregations)

**Learning Objectives**
- Understand how to design business-oriented aggregate models
- Learn dbt's `ref()` function for model dependency management
- Practice incremental aggregation patterns (only process new Silver records)
- Understand how observability tables enable pipeline monitoring

**What You Build**
Three Gold business models (`order_metrics`, `customer_lifetime_value`, and a daily weather summary) plus two observability tables (`pipeline_runs`, `dq_results`), all with dbt tests asserting metric correctness.

**Completion Criteria**
- [ ] `dbt run --select gold` completes without errors
- [ ] `order_metrics` contains daily aggregates with no NULL metric columns
- [ ] `customer_lifetime_value` correctly joins orders and customers
- [ ] All metric columns pass `non_negative_value` tests
- [ ] `pipeline_runs` and `dq_results` tables are created (empty, populated by Airflow)
- [ ] `dbt test --select gold` passes all schema tests

**Validation Steps**
```bash
# Run Gold transformations
docker exec dbt dbt run --select gold

# Run Gold tests
docker exec dbt dbt test --select gold

# Verify aggregations
docker exec dbt python /dbt/verify_customer_ltv.py

# Check order metrics
docker exec dbt python -c "
import duckdb
conn = duckdb.connect(':memory:')
# ... configure S3 ...
print(conn.execute(\"SELECT * FROM read_parquet('s3://gold/order_metrics/**/*.parquet') ORDER BY order_date\").fetchall())
"
```

**Key Takeaways**
- Gold models use `ref('silver_model')` — dbt automatically resolves execution order
- Incremental Gold models only process Silver records newer than the last Gold run timestamp
- Observability tables (pipeline_runs, dq_results) are a production pattern for pipeline health monitoring
- Custom dbt tests (`non_negative_value`, `row_count_greater_than_zero`) extend dbt's built-in test library

---

## Milestone 5 — End-to-End Orchestration with Airflow

**Phase**: Phase 5 (Orchestration Integration)

**Learning Objectives**
- Understand Airflow DAG structure, operators, and task dependencies
- Learn how to integrate Airbyte syncs into an Airflow workflow
- Practice configuring retry logic and failure handling
- Understand how `trigger_rule` controls task execution on upstream failure

**What You Build**
A complete `medallion_pipeline` Airflow DAG that orchestrates the full Bronze → Silver → Gold pipeline with proper dependency management, retry logic, and pipeline run logging.

**Completion Criteria**
- [ ] `medallion_pipeline` DAG is visible in Airflow UI
- [ ] DAG has no import errors (green status in Airflow)
- [ ] All tasks are connected in the correct dependency order
- [ ] `log_pipeline_run` task uses `trigger_rule='all_done'`
- [ ] Tasks are configured with 2 retries and 5-minute delay
- [ ] Triggering the DAG manually runs without Python errors

**Validation Steps**
```bash
# Check DAG loads without errors
docker exec airflow-scheduler airflow dags list

# Validate DAG structure
docker exec airflow-scheduler airflow dags show medallion_pipeline

# Trigger a manual run
docker exec airflow-webserver airflow dags trigger medallion_pipeline

# Check task states
docker exec airflow-webserver airflow tasks states-for-dag-run medallion_pipeline <run_id>
```

**Key Takeaways**
- Airflow `BashOperator` is the simplest way to run dbt commands from a DAG
- `trigger_rule='all_done'` ensures cleanup/logging tasks always run, even after failures
- Airflow Variables store runtime configuration (connection IDs, credentials) separately from DAG code
- The `on_failure_callback` pattern enables structured error logging to observability tables

---

## Milestone 6 — Dashboards and Visualization

**Phase**: Phase 6 (Dashboards and Visualization)

**Learning Objectives**
- Understand how Superset connects to data sources via SQLAlchemy
- Learn how to create datasets, charts, and dashboards in Superset
- Practice exporting and importing dashboards as code (JSON)
- Understand cache TTL and its impact on dashboard freshness

**What You Build**
Two Superset dashboards: "Pipeline Metrics" (DAG run history, DQ results) and "Business KPIs" (order trends, customer lifetime value, weather data).

**Completion Criteria**
- [ ] Superset is accessible at http://localhost:8088
- [ ] DuckDB database connection is configured in Superset
- [ ] Datasets exist for Gold tables (order_metrics, customer_lifetime_value)
- [ ] "Pipeline Metrics" dashboard loads without errors
- [ ] "Business KPIs" dashboard loads without errors
- [ ] Charts display data from Gold layer

**Validation Steps**
```bash
# Access Superset
open http://localhost:8088

# Import dashboards via script
docker exec superset python /app/superset/import_dashboards.py

# Verify datasets via Superset API
curl -X GET http://localhost:8088/api/v1/dataset/ \
  -H "Authorization: Bearer <token>"
```

**Key Takeaways**
- Superset's "Database" → "Dataset" → "Chart" → "Dashboard" hierarchy mirrors most BI tools
- Dashboard JSON exports enable version-controlled, reproducible dashboard definitions
- DuckDB's in-process nature means Superset queries run directly against Parquet files on MinIO
- Cache TTL settings balance dashboard freshness against query performance

---

## Milestone 7 — Advanced Iceberg Features

**Phase**: Phase 7 (Advanced Features)

**Learning Objectives**
- Understand Apache Iceberg's schema evolution capabilities
- Learn how time travel queries work with Iceberg snapshots
- Practice verifying idempotency in incremental pipelines
- Understand ACID transaction semantics in a data lakehouse context

**What You Build**
Demonstrations of schema evolution (adding a column without rewriting data), time travel queries (querying historical snapshots), and idempotency verification (running the pipeline twice produces no duplicates).

**Completion Criteria**
- [ ] Schema evolution: add a new column to a Silver table, verify existing Parquet files still readable
- [ ] Time travel: list snapshots for a Gold table, query data at a prior snapshot
- [ ] Idempotency: run `dbt run --select silver gold` twice, verify row counts are identical
- [ ] `docs/schema-evolution.md` runbook is complete and accurate
- [ ] `docs/time-travel.md` runbook is complete and accurate

**Validation Steps**
```bash
# Schema evolution
docker exec dbt python /dbt/test_schema_evolution.py

# Time travel
docker exec dbt python /dbt/test_time_travel.py

# Idempotency
docker exec dbt python /dbt/test_idempotency.py

# Verify no duplicates after two runs
docker exec dbt dbt run --select silver gold
docker exec dbt dbt run --select silver gold
docker exec dbt dbt test --select silver gold
```

**Key Takeaways**
- Iceberg schema evolution works because metadata (schema) is separate from data (Parquet files)
- Every write to an Iceberg table creates a new snapshot — time travel queries any prior snapshot
- dbt incremental models with `unique_key` are idempotent by design — re-runs merge, not append
- ACID transactions in Iceberg use optimistic concurrency — the first writer wins, others retry

---

## Summary

| Milestone | Phase | Core Skill |
|-----------|-------|-----------|
| 1 — Infrastructure | Phase 1 | Docker Compose, MinIO, Iceberg |
| 2 — Bronze Ingestion | Phase 2 | Airbyte, EL patterns, Parquet |
| 3 — Silver Layer | Phase 3 | dbt, SQL cleaning, schema tests |
| 4 — Gold Layer | Phase 4 | Aggregations, incremental models |
| 5 — Orchestration | Phase 5 | Airflow DAGs, retry logic |
| 6 — Dashboards | Phase 6 | Superset, BI tooling |
| 7 — Advanced Features | Phase 7 | Schema evolution, time travel |
