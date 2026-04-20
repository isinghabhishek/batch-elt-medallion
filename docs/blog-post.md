# Building a Local Data Lakehouse: Design Decisions and Learnings

*A deep dive into building a production-grade Medallion Architecture pipeline that runs entirely on your laptop.*

---

## Why I Built This

Data engineering job descriptions increasingly list Apache Iceberg, dbt, and Airflow as requirements. But most tutorials either use managed cloud services (which cost money and hide the plumbing) or are too simplified to reflect real-world patterns.

I wanted a project that:
1. Runs entirely locally — zero cloud costs, works offline
2. Uses the same tools and patterns as production data platforms
3. Is structured as a learning path, not just a demo

The result is a fully local Medallion Architecture pipeline: MinIO for object storage, Apache Iceberg for the table format, Airbyte for ingestion, dbt for transformations, Airflow for orchestration, and Superset for dashboards — all wired together with Docker Compose.

---

## The Architecture Decision: Why Medallion?

The Medallion Architecture (Bronze → Silver → Gold) is a layered approach to organizing data in a lakehouse. Each layer has a clear contract:

- **Bronze** — raw data, exactly as received from the source, append-only
- **Silver** — cleaned, typed, deduplicated data with enforced quality constraints
- **Gold** — pre-aggregated business metrics optimized for dashboard queries

The key insight is that each layer boundary is a quality gate. Bad data caught at the Bronze → Silver boundary never reaches Gold. This makes debugging straightforward: if a dashboard shows wrong numbers, you check Gold first, then Silver, then Bronze. The problem is always in exactly one layer.

The alternative — a single "transform everything at once" approach — makes it much harder to isolate where data quality issues originate.

---

## Why Apache Iceberg?

I evaluated three open table formats: Delta Lake, Apache Hudi, and Apache Iceberg. I chose Iceberg for this project for a few reasons:

**Vendor neutrality.** Iceberg is governed by the Apache Software Foundation and has first-class support from Spark, Flink, Trino, DuckDB, and Snowflake. Delta Lake is primarily a Databricks project; Hudi is primarily a Uber/AWS project. For a portfolio project demonstrating transferable skills, Iceberg felt like the right choice.

**REST Catalog.** Iceberg's REST Catalog specification means any catalog-compatible service can manage table metadata. I ran the open-source `tabulario/iceberg-rest` image locally — no Hive Metastore, no Glue, no external dependencies.

**DuckDB compatibility.** DuckDB 0.10.0 has native Iceberg read support via the `iceberg` extension. This made it straightforward to use dbt-duckdb as the transformation engine without needing Spark.

**One honest limitation I hit:** DuckDB 0.10.0 doesn't support writing to Iceberg tables via the REST Catalog secret API. The `dbt-duckdb` adapter writes Parquet files directly to S3 paths rather than registering them through the catalog. This means the Iceberg catalog tracks metadata for tables created via Python scripts, but dbt models write directly to S3. In a production setup, you'd use a catalog-aware engine like Trino or Spark for writes.

---

## The dbt + DuckDB Choice

dbt is the standard for SQL-based transformations in modern data stacks. The question was which query engine to use as the dbt adapter.

Options I considered:
- **dbt-spark** — requires a running Spark cluster, heavyweight for local dev
- **dbt-trino** — requires a running Trino cluster, still heavyweight
- **dbt-duckdb** — DuckDB runs in-process, zero infrastructure overhead

DuckDB was the obvious choice for a local-first project. It reads Parquet files directly from S3 (MinIO) via the `httpfs` extension, runs SQL transformations in memory, and writes results back to S3. The entire transformation layer runs inside a single Docker container with no separate query engine service.

**The in-memory database tradeoff.** I configured dbt to use DuckDB's `:memory:` mode, which means tables don't persist between separate dbt invocations. This is fine for the pipeline (Airflow runs `dbt run --select silver gold` in a single command), but it means `dbt test` can't run independently after `dbt run` — the tables are gone. In production, you'd use a persistent DuckDB file path (`path: '/data/warehouse.db'`) or a catalog-aware engine.

**The dbt static parser quirk.** I hit an unexpected issue: dbt's static parser rejects `-- key: value` style comments inside `{{ config(...) }}` blocks. The colon in the comment triggers a YAML parsing error. The fix was simple — remove inline comments from config blocks — but it took a while to diagnose because the error message pointed to the wrong line.

---

## Airflow Orchestration Design

The DAG structure follows a straightforward dependency chain:

```
extract_weather_api ─┐
extract_postgres_db  ├─► bronze_dq_checks ─► silver_transform ─► silver_dq_checks
extract_csv_files   ─┘                                                    │
                                                                          ▼
                                                              gold_transform ─► gold_dq_checks ─► log_pipeline_run
```

A few design decisions worth explaining:

**Parallel extraction.** The three extract tasks run in parallel — there's no reason to serialize them. Airflow's `LocalExecutor` handles this without any additional configuration.

**`trigger_rule='all_done'` on `log_pipeline_run`.** The default trigger rule in Airflow is `all_success` — a task only runs if all upstream tasks succeeded. But I want `log_pipeline_run` to always execute, even if the pipeline fails, so I have a record of the failure. `all_done` means "run when all upstream tasks are in a terminal state (success, failed, or skipped)."

**BashOperator for dbt.** I used `BashOperator` to run dbt commands rather than a dedicated dbt Airflow provider. This keeps the DAG simple and avoids a dependency on `astronomer-cosmos` or similar. The tradeoff is less granular task-level visibility into individual dbt model runs — each dbt invocation is a single Airflow task.

---

## Data Quality Strategy

Data quality checks happen at three points:

1. **Bronze DQ** — assert that the newly ingested batch has at least one row. This catches source connectivity failures and empty syncs.

2. **Silver DQ** — dbt schema tests asserting `unique` and `not_null` on primary key columns. These catch deduplication failures and null key bugs in the cleaning logic.

3. **Gold DQ** — custom dbt tests asserting non-negative metric values and non-zero row counts. These catch aggregation logic errors.

The key principle: **fail fast, fail loudly.** If Bronze DQ fails, Silver and Gold tasks are skipped for that run. There's no point running transformations on bad input data. Airflow's task dependency model makes this easy to implement — a failed task automatically skips all downstream tasks.

All DQ results are written to a `dq_results` Iceberg table in the Gold namespace. This gives you a queryable audit trail of every check that has ever run, which is invaluable for debugging intermittent failures.

---

## Incremental Processing and Idempotency

Making the pipeline idempotent — safe to re-run with the same input data — required careful design of the dbt models.

**Silver models** use `incremental` materialization with a `unique_key` set to the source primary key. On re-runs, dbt generates a `MERGE` statement that updates existing records and inserts new ones. Running the Silver models twice with the same Bronze data produces exactly the same Silver output.

**Gold models** use `incremental` materialization with a filter on `_silver_processed_at > (SELECT MAX(_gold_updated_at) FROM this)`. Only Silver records newer than the last Gold run are aggregated. This avoids full table scans on large Silver tables.

**The idempotency test:** I ran `dbt run --select silver gold` twice in sequence and verified that row counts and aggregate totals were identical. The `verify_gold_data.py` script automates this check — 43/43 assertions pass.

---

## What I'd Do Differently

**Use a persistent DuckDB database.** The `:memory:` configuration works for the pipeline but breaks `dbt test` as a standalone command. A persistent file path would fix this with a one-line change in `profiles.yml`.

**Complete the Airbyte integration.** The Airbyte worker and webapp services need additional configuration to fully function in the Docker Compose setup. For this project, I used mock Bronze data generated by a Python script. In a real deployment, you'd complete the Airbyte configuration and use real source connectors.

**Add a proper secrets manager.** Credentials are stored in a `.env` file, which is fine for local development but not for production. HashiCorp Vault or AWS Secrets Manager would be the next step.

**Add column-level lineage.** dbt generates model-level lineage automatically, but column-level lineage (which source column maps to which Gold metric) requires additional tooling like OpenLineage or dbt's column-level lineage feature (available in dbt Cloud).

---

## Key Takeaways

**Docker Compose is underrated for data engineering.** Running a full data lakehouse stack locally — with health checks, service dependencies, and named volumes — is entirely feasible with Docker Compose. You don't need Kubernetes for local development.

**Iceberg's metadata-data separation is the key insight.** Schema evolution and time travel work because Iceberg stores metadata (schema, snapshots, partition specs) separately from data (Parquet files). Adding a column doesn't touch any existing files — it just updates the metadata JSON. This is fundamentally different from how traditional databases work.

**dbt's incremental materialization is idempotency by design.** The `unique_key` parameter turns a potentially dangerous "append on re-run" into a safe "merge on re-run." This is one of the most important dbt patterns to understand for production pipelines.

**Observability is not optional.** The `pipeline_runs` and `dq_results` tables were the most valuable debugging tools during development. When something went wrong, I could query these tables to see exactly which check failed, at what time, and with what error message. Build observability in from the start.

---

## Resources

- [Apache Iceberg documentation](https://iceberg.apache.org/docs/latest/)
- [dbt-duckdb adapter](https://github.com/duckdb/dbt-duckdb)
- [Medallion Architecture — Databricks](https://www.databricks.com/glossary/medallion-architecture)
- [Airflow best practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [MinIO documentation](https://min.io/docs/minio/container/index.html)

---

*The full project is available on GitHub with a one-command quickstart. See the [README](../README.md) for setup instructions.*
