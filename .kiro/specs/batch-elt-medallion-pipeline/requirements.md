# Requirements Document

## Introduction

A fully local, Docker Compose-based Modern Batch ELT Pipeline implementing the Medallion Architecture (Bronze → Silver → Gold). The system ingests data from multiple sources (REST APIs, relational databases, flat files), lands raw data into a Bronze layer, cleans and standardizes it into a Silver layer, and produces business-ready aggregates in a Gold layer. Apache Iceberg provides the open table format with ACID transactions, time travel, and schema evolution. Airbyte handles extraction, MinIO provides S3-compatible object storage, dbt drives transformations, Apache Airflow orchestrates the pipeline, and Apache Superset delivers dashboards — all running locally at zero cost. The project is designed as a learning-oriented portfolio piece with clear milestones and modern system design patterns.

## Glossary

- **Pipeline**: The end-to-end ELT workflow from source ingestion to Gold layer output.
- **Medallion_Architecture**: A layered data lakehouse pattern with Bronze, Silver, and Gold zones.
- **Bronze_Layer**: The raw landing zone; data is stored as-is with no transformation, preserving source fidelity.
- **Silver_Layer**: The cleaned and standardized zone; data is deduplicated, typed, and conformed.
- **Gold_Layer**: The business-ready aggregation zone; data is modeled for analytics and reporting.
- **Iceberg_Table**: An Apache Iceberg table stored on MinIO providing ACID transactions, schema evolution, and time travel.
- **Airbyte**: The open-source EL (Extract-Load) tool responsible for pulling data from sources and writing to the Bronze layer.
- **MinIO**: A self-hosted, S3-compatible object storage service used as the data lake storage backend.
- **dbt**: The data build tool used to define and execute SQL-based transformations from Bronze → Silver → Gold.
- **Airflow**: Apache Airflow, the workflow orchestrator that schedules and monitors pipeline DAGs.
- **Superset**: Apache Superset, the BI and dashboarding tool connected to the Gold layer.
- **Catalog**: The Apache Iceberg REST catalog (or Hive Metastore) that tracks table metadata and schema versions.
- **DQ_Check**: A data quality check — a validation rule applied at a layer boundary to assert correctness.
- **DAG**: A Directed Acyclic Graph representing an Airflow workflow.
- **Source_Connector**: An Airbyte connector configured to read from a specific data source.
- **Namespace**: A logical grouping of Iceberg tables within the Catalog (equivalent to a database schema).
- **Schema_Evolution**: The ability to add, rename, or drop columns in an Iceberg table without rewriting existing data.
- **Time_Travel**: The ability to query an Iceberg table as it existed at a prior snapshot or timestamp.
- **Snapshot**: An immutable point-in-time version of an Iceberg table recorded after each write operation.

---

## Requirements

### Requirement 1: Local Infrastructure Provisioning

**User Story:** As a developer, I want the entire stack to start with a single command, so that I can reproduce the environment on any machine without cloud costs.

#### Acceptance Criteria

1. THE Pipeline SHALL be fully defined in a `docker-compose.yml` file at the repository root that starts all services with `docker compose up`.
2. WHEN `docker compose up` is executed on a clean machine with Docker and Docker Compose installed, THE Pipeline SHALL reach a healthy state for all services within 10 minutes on a standard broadband connection.
3. THE Pipeline SHALL include the following services: MinIO, Airbyte (server + worker + webapp), Apache Iceberg REST Catalog, dbt runner, Apache Airflow (webserver + scheduler + worker), and Apache Superset.
4. WHEN a service fails its health check, THE Pipeline SHALL surface the failure in `docker compose ps` output with a non-healthy status.
5. THE Pipeline SHALL use named Docker volumes for all stateful services so that data persists across `docker compose down` and `docker compose up` cycles.
6. IF a required environment variable is missing at startup, THEN THE Pipeline SHALL log a descriptive error message identifying the missing variable and exit with a non-zero code.
7. THE Pipeline SHALL expose the following local ports: MinIO Console on 9001, Airbyte UI on 8000, Airflow UI on 8080, Superset UI on 8088, and Iceberg REST Catalog on 8181.

---

### Requirement 2: MinIO Object Storage Configuration

**User Story:** As a developer, I want MinIO to act as a local S3-compatible data lake, so that all pipeline components interact with storage using standard S3 APIs.

#### Acceptance Criteria

1. WHEN MinIO starts, THE Pipeline SHALL automatically create the following buckets: `bronze`, `silver`, `gold`, and `iceberg-warehouse`.
2. THE MinIO service SHALL expose an S3-compatible API on port 9000 accessible to all other containers via the Docker internal network hostname `minio`.
3. WHEN a component writes an object using the S3 API with endpoint `http://minio:9000`, THE MinIO service SHALL store the object and return a 200 OK response.
4. THE Pipeline SHALL configure a dedicated MinIO service account with access key and secret key stored in a `.env` file excluded from version control.
5. IF the `bronze`, `silver`, `gold`, or `iceberg-warehouse` bucket already exists at startup, THEN THE Pipeline SHALL skip bucket creation without error.

---

### Requirement 3: Iceberg Table Format and Catalog

**User Story:** As a developer, I want all data layers to use Apache Iceberg tables, so that I get ACID transactions, schema evolution, and time travel out of the box.

#### Acceptance Criteria

1. THE Catalog SHALL register all Iceberg tables under namespaces `bronze`, `silver`, and `gold` corresponding to their Medallion layer.
2. WHEN a dbt model writes to an Iceberg table, THE Catalog SHALL record a new Snapshot with a unique snapshot ID and a timestamp.
3. THE Pipeline SHALL support Schema_Evolution: WHEN a new column is added to a dbt model, THE Catalog SHALL update the table schema without rewriting existing Parquet data files.
4. WHEN a Time_Travel query is issued against an Iceberg table using a snapshot ID or timestamp, THE Catalog SHALL return the data as it existed at that point in time.
5. THE Iceberg_Table SHALL enforce ACID semantics: WHEN two concurrent writes target the same table, THE Catalog SHALL serialize them and reject the conflicting write with an optimistic concurrency error.
6. THE Pipeline SHALL store all Iceberg data files and metadata files in the `iceberg-warehouse` MinIO bucket using the path pattern `iceberg-warehouse/{namespace}/{table_name}/`.

---

### Requirement 4: Data Source Connectivity via Airbyte

**User Story:** As a developer, I want Airbyte to extract data from multiple source types, so that I can demonstrate multi-source ingestion in my portfolio.

#### Acceptance Criteria

1. THE Pipeline SHALL include at least three Source_Connectors: one REST API source (e.g., a public open data API), one relational database source (e.g., PostgreSQL seed database), and one file-based source (CSV or JSON files in a local directory).
2. WHEN an Airbyte sync job runs, THE Airbyte service SHALL write raw records to the `bronze` MinIO bucket in Parquet or JSON format under the path `bronze/{source_name}/{stream_name}/`.
3. WHEN a sync job completes successfully, THE Airbyte service SHALL record the job status as `succeeded` in its internal job history.
4. IF a source is unreachable during a sync, THEN THE Airbyte service SHALL mark the job as `failed`, log the error, and SHALL NOT write partial data to the Bronze layer.
5. THE Airbyte service SHALL support incremental sync mode for sources that provide a cursor field, writing only new or updated records on subsequent runs.
6. WHEN an incremental sync runs after a full refresh, THE Airbyte service SHALL append only records with a cursor value greater than the last synced cursor value.
7. THE Pipeline SHALL provide Airbyte connection configurations as code (YAML or JSON) in a `connections/` directory so that connections can be recreated via the Airbyte API without manual UI steps.

---

### Requirement 5: Bronze Layer — Raw Data Landing

**User Story:** As a developer, I want the Bronze layer to preserve raw source data exactly as received, so that I can always reprocess from the original records.

#### Acceptance Criteria

1. THE Bronze_Layer SHALL store data as-is from the source with no field renaming, type casting, or filtering applied.
2. THE Bronze_Layer SHALL add the following metadata columns to every record: `_source_name` (string), `_ingested_at` (timestamp), and `_file_path` (string pointing to the source object in MinIO).
3. WHEN a Bronze Iceberg table is created, THE Catalog SHALL partition it by `_ingested_at` date to enable efficient time-range queries.
4. THE Bronze_Layer SHALL retain all historical loads; WHEN a new sync runs, THE Pipeline SHALL append new records rather than overwrite existing ones.
5. IF a source record contains a null primary key, THEN THE Bronze_Layer SHALL still ingest the record and flag it with `_has_null_key = true`.

---

### Requirement 6: Silver Layer — Cleaning and Standardization

**User Story:** As a developer, I want the Silver layer to contain clean, typed, and deduplicated data, so that downstream analytics are built on reliable foundations.

#### Acceptance Criteria

1. WHEN a dbt Silver model runs, THE dbt runner SHALL read from the corresponding Bronze Iceberg table and write to the Silver Iceberg table in the `silver` namespace.
2. THE Silver_Layer SHALL cast all date and timestamp fields to ISO 8601 format (`YYYY-MM-DDTHH:MM:SSZ`).
3. THE Silver_Layer SHALL deduplicate records using the source-defined primary key, retaining the record with the latest `_ingested_at` value when duplicates exist.
4. THE Silver_Layer SHALL replace empty strings with SQL NULL values for all string columns.
5. WHEN a Silver dbt model runs, THE dbt runner SHALL execute schema tests asserting `not_null` and `unique` constraints on primary key columns.
6. IF a Silver dbt model test fails, THEN THE dbt runner SHALL exit with a non-zero code and THE Airflow DAG SHALL mark the task as failed.
7. THE Silver_Layer SHALL add a `_silver_processed_at` timestamp column recording when the record was written to the Silver layer.
8. THE Silver_Layer SHALL standardize all string columns to trimmed, UTF-8 encoded values.

---

### Requirement 7: Gold Layer — Business Aggregations

**User Story:** As a developer, I want the Gold layer to contain pre-aggregated, business-ready datasets, so that Superset dashboards query fast and reflect meaningful KPIs.

#### Acceptance Criteria

1. WHEN a dbt Gold model runs, THE dbt runner SHALL read from Silver Iceberg tables and write aggregated results to Gold Iceberg tables in the `gold` namespace.
2. THE Gold_Layer SHALL include at least three business-oriented aggregate models demonstrating different aggregation patterns (e.g., daily counts, rolling averages, top-N rankings).
3. THE Gold_Layer SHALL add a `_gold_updated_at` timestamp column recording when the aggregate was last refreshed.
4. WHEN a Gold dbt model runs, THE dbt runner SHALL execute dbt tests asserting that aggregate metric columns contain no NULL values and that row counts are greater than zero.
5. THE Gold_Layer SHALL use dbt incremental materialization so that only new or changed Silver records are processed on each run, rather than full table scans.
6. WHERE a Gold model depends on multiple Silver tables, THE dbt runner SHALL resolve the dependency order automatically using dbt's DAG and execute models in topological order.

---

### Requirement 8: dbt Transformation Framework

**User Story:** As a developer, I want all transformations defined as dbt models with tests and documentation, so that the transformation logic is version-controlled, testable, and self-documenting.

#### Acceptance Criteria

1. THE dbt runner SHALL connect to a query engine (e.g., Trino or DuckDB) that reads and writes Iceberg tables on MinIO.
2. THE Pipeline SHALL organize dbt models in the directory structure `models/bronze/`, `models/silver/`, `models/gold/` corresponding to Medallion layers.
3. WHEN `dbt run` is executed, THE dbt runner SHALL compile and execute all models in dependency order without manual sequencing.
4. WHEN `dbt test` is executed, THE dbt runner SHALL run all schema tests and custom data tests defined in `schema.yml` files.
5. WHEN `dbt docs generate` is executed, THE dbt runner SHALL produce a static documentation site describing all models, columns, and lineage.
6. THE dbt runner SHALL use dbt sources to reference Bronze Iceberg tables, ensuring lineage is tracked from raw ingestion through Gold.
7. IF a dbt model SQL contains a syntax error, THEN THE dbt runner SHALL report the model name, file path, and error message and exit with a non-zero code.

---

### Requirement 9: Data Quality Checks

**User Story:** As a developer, I want automated data quality checks at each layer boundary, so that bad data is caught early and never silently propagates downstream.

#### Acceptance Criteria

1. THE Pipeline SHALL execute DQ_Checks at three points: after Bronze ingestion, after Silver transformation, and after Gold aggregation.
2. WHEN a Bronze DQ_Check runs, THE Pipeline SHALL assert that the row count of the newly ingested batch is greater than zero.
3. WHEN a Silver DQ_Check runs, THE Pipeline SHALL assert that the primary key column contains no NULL values and no duplicate values.
4. WHEN a Gold DQ_Check runs, THE Pipeline SHALL assert that all metric columns contain no NULL values and that aggregate totals are non-negative.
5. IF any DQ_Check assertion fails, THEN THE Airflow DAG SHALL mark the corresponding task as failed and halt downstream tasks in the same DAG run.
6. THE Pipeline SHALL log DQ_Check results (check name, layer, row count, pass/fail status, timestamp) to a `dq_results` Iceberg table in the `gold` namespace for auditability.
7. THE Pipeline SHALL use dbt's built-in test framework for Silver and Gold DQ_Checks, and a custom Airflow operator or dbt source freshness check for Bronze DQ_Checks.

---

### Requirement 10: Airflow Orchestration

**User Story:** As a developer, I want Airflow to schedule and monitor the full pipeline, so that I can demonstrate end-to-end workflow orchestration with dependency management.

#### Acceptance Criteria

1. THE Airflow service SHALL define a DAG named `medallion_pipeline` that executes the following tasks in order: `extract_load` (Airbyte sync) → `bronze_dq` → `silver_transform` → `silver_dq` → `gold_transform` → `gold_dq`.
2. WHEN the `medallion_pipeline` DAG is triggered, THE Airflow service SHALL execute each task only after all upstream tasks have succeeded.
3. THE Airflow service SHALL schedule the `medallion_pipeline` DAG to run on a configurable cron schedule (default: daily at midnight UTC).
4. WHEN a task fails, THE Airflow service SHALL retry the task up to 2 times with a 5-minute delay before marking it as permanently failed.
5. WHEN a DAG run completes (success or failure), THE Airflow service SHALL record the run duration, task statuses, and log URLs in its metadata database.
6. THE Airflow service SHALL expose task logs via the Airflow UI accessible at `http://localhost:8080`.
7. THE Pipeline SHALL define all DAGs as Python files in a `dags/` directory mounted into the Airflow container, enabling DAG changes without container rebuilds.
8. IF the Airbyte sync task fails, THEN THE Airflow DAG SHALL skip all downstream transformation and DQ tasks for that run.

---

### Requirement 11: Apache Superset Dashboards

**User Story:** As a developer, I want Superset dashboards connected to the Gold layer, so that I can visualize pipeline outputs and demonstrate end-to-end data flow in my portfolio.

#### Acceptance Criteria

1. THE Superset service SHALL connect to the Gold layer query engine using a SQLAlchemy-compatible connection string configured at startup.
2. THE Pipeline SHALL provision at least two Superset dashboards via Superset's import API or configuration-as-code: one showing pipeline run metrics (row counts per layer, DQ pass/fail history) and one showing business KPIs from Gold models.
3. WHEN a Gold Iceberg table is updated, THE Superset service SHALL reflect the new data within one query execution (no stale cache beyond the configured cache TTL).
4. THE Superset service SHALL be accessible at `http://localhost:8088` with a default admin account configured via environment variables.
5. WHERE Superset dashboard definitions are stored as JSON export files in a `superset/dashboards/` directory, THE Pipeline SHALL import them automatically on first startup.

---

### Requirement 12: Schema Evolution and Time Travel Demonstration

**User Story:** As a developer, I want to demonstrate Iceberg's schema evolution and time travel capabilities, so that I can showcase advanced data lakehouse features in my portfolio.

#### Acceptance Criteria

1. THE Pipeline SHALL include a documented runbook (`docs/schema-evolution.md`) demonstrating how to add a new column to a Silver Iceberg table without rewriting existing data.
2. WHEN a new column is added to an Iceberg table via `ALTER TABLE ... ADD COLUMN`, THE Catalog SHALL update the table schema and THE existing Parquet files SHALL remain readable without modification.
3. THE Pipeline SHALL include a documented runbook (`docs/time-travel.md`) demonstrating how to query a Gold table at a prior snapshot using `AS OF` syntax.
4. WHEN a Time_Travel query is issued with a valid snapshot ID, THE query engine SHALL return the data as it existed at that snapshot without error.
5. WHEN a Time_Travel query is issued with an invalid or expired snapshot ID, THE query engine SHALL return a descriptive error message identifying the invalid snapshot.

---

### Requirement 13: Incremental Processing and Idempotency

**User Story:** As a developer, I want the pipeline to process only new data on each run and be safe to re-run, so that I avoid reprocessing costs and demonstrate production-grade patterns.

#### Acceptance Criteria

1. WHEN the `medallion_pipeline` DAG runs for the second time with no new source data, THE Pipeline SHALL produce no duplicate records in the Silver or Gold layers.
2. WHEN the `medallion_pipeline` DAG is re-run for a past date (backfill), THE Pipeline SHALL overwrite the existing partition for that date rather than appending duplicates.
3. THE Silver_Layer dbt models SHALL use dbt incremental materialization with a `unique_key` defined so that re-runs merge rather than append.
4. THE Gold_Layer dbt models SHALL use dbt incremental materialization so that only Silver records newer than the last Gold run timestamp are aggregated.
5. FOR ALL pipeline runs, running the pipeline twice with the same source data SHALL produce the same Gold layer output as running it once (idempotence property).

---

### Requirement 14: Repository Structure and Developer Experience

**User Story:** As a developer, I want a well-organized repository with clear documentation, so that the project is easy to navigate and impressive to reviewers.

#### Acceptance Criteria

1. THE Pipeline SHALL organize the repository with the following top-level directories: `docker/` (Dockerfiles and compose), `dags/` (Airflow DAGs), `dbt/` (dbt project), `connections/` (Airbyte configs), `superset/` (dashboard exports), `docs/` (runbooks and architecture), and `scripts/` (setup and seed scripts).
2. THE Pipeline SHALL include a `README.md` at the repository root with: project overview, architecture diagram (Mermaid), prerequisites, quickstart instructions, and milestone descriptions.
3. THE Pipeline SHALL include a `docs/architecture.md` file containing a Mermaid diagram showing all components, data flow directions, and layer boundaries.
4. THE Pipeline SHALL include a `docs/milestones.md` file defining at least five learning milestones with clear completion criteria.
5. THE Pipeline SHALL include a `.env.example` file listing all required environment variables with placeholder values and descriptions.
6. THE Pipeline SHALL include a `scripts/seed.sh` script that populates the PostgreSQL seed database and places sample CSV/JSON files in the file source directory.
7. WHEN `scripts/seed.sh` is executed, THE Pipeline SHALL be ready for a first full pipeline run without any additional manual steps.

---

### Requirement 15: Observability and Logging

**User Story:** As a developer, I want structured logs and pipeline run metrics, so that I can debug failures quickly and demonstrate operational awareness.

#### Acceptance Criteria

1. WHEN any pipeline component encounters an error, THE component SHALL log the error with: timestamp, component name, error type, and a human-readable message.
2. THE Airflow service SHALL retain task logs for at least 30 DAG runs before rotation.
3. THE Pipeline SHALL write pipeline run summary records (DAG run ID, start time, end time, status, rows ingested per layer) to a `pipeline_runs` Iceberg table in the `gold` namespace.
4. WHEN `dbt run` completes, THE dbt runner SHALL output a run results JSON file (`target/run_results.json`) recording model execution times and row counts.
5. THE Pipeline SHALL include a Superset chart on the pipeline metrics dashboard displaying the last 10 DAG run statuses and durations sourced from the `pipeline_runs` table.
