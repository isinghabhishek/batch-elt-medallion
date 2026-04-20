# Design Document: Batch ELT Medallion Pipeline

## Overview

This design document specifies a fully local, Docker Compose-based Modern Batch ELT Pipeline implementing the Medallion Architecture (Bronze → Silver → Gold). The system demonstrates production-grade data engineering patterns including ACID transactions, schema evolution, time travel, incremental processing, and comprehensive observability—all running at zero cost on a developer's machine.

### System Goals

- **Zero-Cost Learning Environment**: Complete stack runs locally with Docker Compose
- **Production Patterns**: ACID transactions, idempotency, incremental processing, data quality checks
- **Modern Stack**: Apache Iceberg, MinIO, Airbyte, dbt, Airflow, Superset
- **Portfolio-Ready**: Clear architecture, documentation, and demonstrable features

### Key Design Decisions

1. **Query Engine**: DuckDB chosen over Trino for simplicity, lower resource footprint, and native Iceberg support
2. **Catalog**: Iceberg REST Catalog for standard metadata management
3. **Storage**: MinIO provides S3-compatible object storage
4. **Orchestration**: Airflow manages end-to-end workflow dependencies
5. **Transformations**: dbt with dbt-duckdb adapter for SQL-based transformations

## Architecture

### High-Level System Architecture

```mermaid
graph TB
    subgraph Sources["Data Sources"]
        API[REST API<br/>Public Open Data]
        PG[PostgreSQL<br/>Seed Database]
        FILES[CSV/JSON Files<br/>Local Directory]
    end
    
    subgraph Ingestion["Extraction & Loading (Airbyte)"]
        AB_SERVER[Airbyte Server]
        AB_WORKER[Airbyte Worker]
        AB_UI[Airbyte UI<br/>:8000]
    end
    
    subgraph Storage["Object Storage (MinIO)"]
        MINIO[MinIO Server<br/>:9000 API<br/>:9001 Console]
        BRONZE_BUCKET[bronze/]
        SILVER_BUCKET[silver/]
        GOLD_BUCKET[gold/]
        WAREHOUSE[iceberg-warehouse/]
    end
    
    subgraph Catalog["Metadata Management"]
        REST_CAT[Iceberg REST Catalog<br/>:8181]
    end
    
    subgraph Transform["Transformation Layer (dbt + DuckDB)"]
        DBT[dbt Runner<br/>dbt-duckdb adapter]
        DUCKDB[DuckDB Engine<br/>In-Process]
    end
    
    subgraph Orchestration["Workflow Orchestration (Airflow)"]
        AF_WEB[Airflow Webserver<br/>:8080]
        AF_SCHED[Airflow Scheduler]
        AF_WORKER[Airflow Worker]
        AF_DB[(Airflow Metadata DB<br/>PostgreSQL)]
    end
    
    subgraph Analytics["Business Intelligence (Superset)"]
        SS[Superset<br/>:8088]
        SS_DB[(Superset Metadata DB<br/>SQLite)]
    end
    
    subgraph Layers["Medallion Layers (Iceberg Tables)"]
        BRONZE[Bronze Layer<br/>Raw Data<br/>Partitioned by _ingested_at]
        SILVER[Silver Layer<br/>Cleaned & Typed<br/>Deduplicated]
        GOLD[Gold Layer<br/>Business Aggregates<br/>Incremental]
    end
    
    API --> AB_WORKER
    PG --> AB_WORKER
    FILES --> AB_WORKER
    
    AB_WORKER --> BRONZE_BUCKET
    AB_SERVER -.manages.-> AB_WORKER
    AB_UI -.configures.-> AB_SERVER
    
    BRONZE_BUCKET --> BRONZE
    BRONZE --> REST_CAT
    
    DBT --> DUCKDB
    DUCKDB -.reads/writes.-> WAREHOUSE
    DUCKDB -.queries metadata.-> REST_CAT
    
    BRONZE -.dbt source.-> DBT
    DBT --> SILVER
    SILVER --> REST_CAT
    DBT --> GOLD
    GOLD --> REST_CAT
    
    AF_SCHED -.triggers.-> AF_WORKER
    AF_WORKER -.executes.-> AB_SERVER
    AF_WORKER -.executes.-> DBT
    AF_WEB -.monitors.-> AF_DB
    AF_SCHED -.updates.-> AF_DB
    
    GOLD -.queries.-> SS
    SS -.stores dashboards.-> SS_DB
    
    MINIO -.hosts.-> BRONZE_BUCKET
    MINIO -.hosts.-> SILVER_BUCKET
    MINIO -.hosts.-> GOLD_BUCKET
    MINIO -.hosts.-> WAREHOUSE
    
    classDef source fill:#e1f5ff,stroke:#01579b
    classDef storage fill:#fff3e0,stroke:#e65100
    classDef compute fill:#f3e5f5,stroke:#4a148c
    classDef orchestration fill:#e8f5e9,stroke:#1b5e20
    classDef analytics fill:#fce4ec,stroke:#880e4f
    classDef layer fill:#fff9c4,stroke:#f57f17
    
    class API,PG,FILES source
    class MINIO,BRONZE_BUCKET,SILVER_BUCKET,GOLD_BUCKET,WAREHOUSE storage
    class AB_SERVER,AB_WORKER,DBT,DUCKDB compute
    class AF_WEB,AF_SCHED,AF_WORKER,AF_DB orchestration
    class SS,SS_DB analytics
    class BRONZE,SILVER,GOLD layer
```

### Data Flow Sequence


```mermaid
sequenceDiagram
    participant Airflow
    participant Airbyte
    participant MinIO
    participant Bronze
    participant dbt
    participant DuckDB
    participant Catalog
    participant Silver
    participant Gold
    participant Superset
    
    Airflow->>Airbyte: Trigger sync job (extract_load task)
    Airbyte->>API/PG/Files: Extract data
    Airbyte->>MinIO: Write raw Parquet to bronze/
    Airbyte-->>Airflow: Sync complete
    
    Airflow->>dbt: Run bronze_dq checks
    dbt->>DuckDB: Query Bronze tables
    DuckDB->>Catalog: Get table metadata
    DuckDB->>MinIO: Read bronze/ data
    DuckDB-->>dbt: Row counts, validation results
    dbt-->>Airflow: DQ checks passed
    
    Airflow->>dbt: Run silver_transform
    dbt->>DuckDB: Execute Silver models
    DuckDB->>Catalog: Get Bronze schema
    DuckDB->>MinIO: Read bronze/, write silver/
    DuckDB->>Catalog: Register Silver snapshot
    dbt-->>Airflow: Silver transform complete
    
    Airflow->>dbt: Run silver_dq checks
    dbt->>DuckDB: Execute dbt tests
    DuckDB-->>dbt: Test results
    dbt-->>Airflow: DQ checks passed
    
    Airflow->>dbt: Run gold_transform
    dbt->>DuckDB: Execute Gold models (incremental)
    DuckDB->>Catalog: Get Silver schema
    DuckDB->>MinIO: Read silver/, write gold/
    DuckDB->>Catalog: Register Gold snapshot
    dbt-->>Airflow: Gold transform complete
    
    Airflow->>dbt: Run gold_dq checks
    dbt->>DuckDB: Execute dbt tests
    DuckDB-->>dbt: Test results
    dbt-->>Airflow: DQ checks passed
    
    Airflow->>Airflow: Log pipeline_runs record
    
    User->>Superset: View dashboard
    Superset->>DuckDB: Query Gold tables
    DuckDB->>Catalog: Get Gold schema
    DuckDB->>MinIO: Read gold/ data
    DuckDB-->>Superset: Query results
    Superset-->>User: Render charts
```

## Components and Interfaces

### 1. MinIO Object Storage

**Purpose**: S3-compatible object storage backend for all data layers

**Docker Image**: `minio/minio:latest`

**Configuration**:
- API Port: 9000 (internal), Console Port: 9001 (external)
- Buckets: `bronze`, `silver`, `gold`, `iceberg-warehouse`
- Credentials: `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` from `.env`
- Health Check: `curl -f http://localhost:9000/minio/health/live`

**Initialization**:
```bash
# Entrypoint script creates buckets on first start
mc alias set local http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD
mc mb local/bronze local/silver local/gold local/iceberg-warehouse --ignore-existing
```

**Interfaces**:
- S3 API: All components use boto3/AWS SDK with endpoint `http://minio:9000`
- Console UI: Web interface at `http://localhost:9001`

### 2. Apache Iceberg REST Catalog

**Purpose**: Centralized metadata management for Iceberg tables

**Docker Image**: `tabulario/iceberg-rest:latest`

**Configuration**:
- Port: 8181
- Warehouse Path: `s3://iceberg-warehouse/`
- Backend: In-memory SQLite (for local dev) or PostgreSQL (for persistence)
- S3 Endpoint: `http://minio:9000`

**Environment Variables**:
```yaml
CATALOG_WAREHOUSE: s3://iceberg-warehouse/
CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
CATALOG_S3_ENDPOINT: http://minio:9000
CATALOG_S3_ACCESS__KEY__ID: ${MINIO_ROOT_USER}
CATALOG_S3_SECRET__ACCESS__KEY: ${MINIO_ROOT_PASSWORD}
CATALOG_S3_PATH__STYLE__ACCESS: true
```

**API Endpoints**:
- `GET /v1/config`: Catalog configuration
- `POST /v1/namespaces`: Create namespace (bronze, silver, gold)
- `POST /v1/namespaces/{ns}/tables`: Register table
- `GET /v1/namespaces/{ns}/tables/{table}`: Get table metadata
- `POST /v1/namespaces/{ns}/tables/{table}`: Commit table changes

### 3. Airbyte

**Purpose**: Extract data from sources and load to Bronze layer

**Docker Images**:
- `airbyte/server:latest`
- `airbyte/worker:latest`
- `airbyte/webapp:latest`
- `airbyte/db:latest` (PostgreSQL for Airbyte metadata)

**Configuration**:
- UI Port: 8000
- API Port: 8001
- Database: PostgreSQL container for job history and connection configs

**Source Connectors** (3 required):
1. **REST API Source**: `source-http` connector
   - Example: [Open-Meteo Weather API](https://open-meteo.com/en/docs)
   - Config: Base URL, pagination, authentication
   
2. **PostgreSQL Source**: `source-postgres` connector
   - Connects to seed database container
   - Incremental sync using `updated_at` cursor
   
3. **File Source**: `source-file` connector
   - Reads CSV/JSON from mounted volume
   - Example: Sample e-commerce orders, customer data

**Destination Configuration**:
- Destination: S3 (MinIO)
- Endpoint: `http://minio:9000`
- Bucket: `bronze`
- Format: Parquet
- Path Pattern: `bronze/{source_name}/{stream_name}/{timestamp}/`

**Connection Configuration as Code**:
```yaml
# connections/weather_api.yaml
source:
  name: weather-api
  connector: source-http
  config:
    base_url: https://api.open-meteo.com/v1/forecast
    streams:
      - name: weather_forecast
        http_method: GET
        path: /forecast
        
destination:
  name: minio-bronze
  connector: destination-s3
  config:
    endpoint: http://minio:9000
    bucket: bronze
    format: parquet
    s3_path_format: ${source_name}/${stream_name}/${timestamp}/
```

**Airbyte API Integration**:
```python
# Airflow operator calls Airbyte API
import requests

def trigger_airbyte_sync(connection_id):
    response = requests.post(
        f"http://airbyte-server:8001/api/v1/connections/{connection_id}/sync",
        headers={"Authorization": f"Bearer {AIRBYTE_API_KEY}"}
    )
    return response.json()["job"]["id"]
```

### 4. DuckDB Query Engine

**Purpose**: Execute SQL queries against Iceberg tables

**Integration**: Embedded in dbt runner container (no separate service)

**DuckDB Extensions**:
- `iceberg`: Read/write Iceberg tables
- `httpfs`: Access S3-compatible storage

**Configuration**:
```sql
-- DuckDB initialization
INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;

SET s3_endpoint='minio:9000';
SET s3_access_key_id='${MINIO_ROOT_USER}';
SET s3_secret_access_key='${MINIO_ROOT_PASSWORD}';
SET s3_use_ssl=false;
SET s3_url_style='path';

-- Connect to Iceberg catalog
CREATE SECRET iceberg_rest (
    TYPE ICEBERG_REST,
    CATALOG_URL 'http://iceberg-rest:8181',
    S3_ENDPOINT 'minio:9000',
    S3_ACCESS_KEY_ID '${MINIO_ROOT_USER}',
    S3_SECRET_ACCESS_KEY '${MINIO_ROOT_PASSWORD}',
    S3_USE_SSL false
);
```

**Query Patterns**:
```sql
-- Read from Iceberg table
SELECT * FROM iceberg_scan('iceberg-rest.bronze.weather_data');

-- Write to Iceberg table
CREATE TABLE iceberg_rest.silver.weather_clean AS
SELECT * FROM iceberg_rest.bronze.weather_data WHERE temp IS NOT NULL;

-- Time travel query
SELECT * FROM iceberg_rest.gold.daily_metrics
FOR SYSTEM_TIME AS OF '2024-01-15 10:00:00';
```

### 5. dbt Transformation Framework

**Purpose**: Define and execute SQL transformations

**Docker Image**: Custom image based on `python:3.11-slim`

**Dockerfile**:
```dockerfile
FROM python:3.11-slim

RUN pip install dbt-duckdb==1.7.0 duckdb==0.10.0

WORKDIR /dbt
COPY dbt/ /dbt/

CMD ["dbt", "run"]
```

**dbt Project Structure**:
```
dbt/
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── bronze/
│   │   └── sources.yml
│   ├── silver/
│   │   ├── schema.yml
│   │   ├── weather_clean.sql
│   │   ├── orders_clean.sql
│   │   └── customers_clean.sql
│   └── gold/
│       ├── schema.yml
│       ├── daily_weather_summary.sql
│       ├── order_metrics.sql
│       └── customer_lifetime_value.sql
├── tests/
│   └── assert_positive_metrics.sql
└── macros/
    └── generate_schema_name.sql
```

**profiles.yml**:
```yaml
medallion_pipeline:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ':memory:'
      extensions:
        - iceberg
        - httpfs
      settings:
        s3_endpoint: minio:9000
        s3_access_key_id: "{{ env_var('MINIO_ROOT_USER') }}"
        s3_secret_access_key: "{{ env_var('MINIO_ROOT_PASSWORD') }}"
        s3_use_ssl: false
        s3_url_style: path
```

**dbt_project.yml**:
```yaml
name: medallion_pipeline
version: 1.0.0
config-version: 2

profile: medallion_pipeline

model-paths: ["models"]
test-paths: ["tests"]
macro-paths: ["macros"]

models:
  medallion_pipeline:
    bronze:
      +materialized: external
      +location: "s3://bronze/"
    silver:
      +materialized: incremental
      +unique_key: id
      +on_schema_change: append_new_columns
      +location: "s3://silver/"
    gold:
      +materialized: incremental
      +unique_key: date
      +location: "s3://gold/"
```

**Example Silver Model** (`models/silver/weather_clean.sql`):
```sql
{{
  config(
    materialized='incremental',
    unique_key='location_date',
    on_schema_change='append_new_columns'
  )
}}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'weather_data') }}
    {% if is_incremental() %}
    WHERE _ingested_at > (SELECT MAX(_silver_processed_at) FROM {{ this }})
    {% endif %}
),

cleaned AS (
    SELECT
        TRIM(location) AS location,
        CAST(date AS DATE) AS date,
        CAST(temperature AS DOUBLE) AS temperature_celsius,
        CAST(humidity AS DOUBLE) AS humidity_percent,
        CASE 
            WHEN TRIM(condition) = '' THEN NULL 
            ELSE TRIM(condition) 
        END AS condition,
        _ingested_at,
        CURRENT_TIMESTAMP AS _silver_processed_at,
        location || '_' || date AS location_date
    FROM source
    WHERE location IS NOT NULL
      AND date IS NOT NULL
)

SELECT * FROM cleaned
```

**Example Gold Model** (`models/gold/daily_weather_summary.sql`):
```sql
{{
  config(
    materialized='incremental',
    unique_key='date',
    on_schema_change='fail'
  )
}}

WITH silver_data AS (
    SELECT * FROM {{ ref('weather_clean') }}
    {% if is_incremental() %}
    WHERE date > (SELECT MAX(date) FROM {{ this }})
    {% endif %}
),

daily_agg AS (
    SELECT
        date,
        COUNT(DISTINCT location) AS location_count,
        AVG(temperature_celsius) AS avg_temperature,
        MIN(temperature_celsius) AS min_temperature,
        MAX(temperature_celsius) AS max_temperature,
        AVG(humidity_percent) AS avg_humidity,
        CURRENT_TIMESTAMP AS _gold_updated_at
    FROM silver_data
    GROUP BY date
)

SELECT * FROM daily_agg
```

**dbt Tests** (`models/silver/schema.yml`):
```yaml
version: 2

models:
  - name: weather_clean
    description: Cleaned and standardized weather data
    columns:
      - name: location_date
        description: Composite primary key
        tests:
          - unique
          - not_null
      - name: location
        tests:
          - not_null
      - name: date
        tests:
          - not_null
      - name: temperature_celsius
        tests:
          - not_null
      - name: _silver_processed_at
        tests:
          - not_null
```

### 6. Apache Airflow

**Purpose**: Orchestrate end-to-end pipeline workflow

**Docker Images**:
- `apache/airflow:2.8.0-python3.11`
- Services: webserver, scheduler, worker
- Database: PostgreSQL for Airflow metadata

**Configuration**:
- Webserver Port: 8080
- Executor: LocalExecutor (single-machine)
- DAG Directory: `./dags` mounted as volume

**Environment Variables**:
```yaml
AIRFLOW__CORE__EXECUTOR: LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
AIRFLOW__CORE__LOAD_EXAMPLES: false
AIRFLOW__WEBSERVER__EXPOSE_CONFIG: true
```

**DAG Definition** (`dags/medallion_pipeline.py`):
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'medallion_pipeline',
    default_args=default_args,
    description='End-to-end Medallion ELT pipeline',
    schedule_interval='0 0 * * *',  # Daily at midnight UTC
    catchup=False,
    tags=['medallion', 'elt', 'iceberg'],
)

# Task 1: Extract and Load (Airbyte syncs)
extract_weather = AirbyteTriggerSyncOperator(
    task_id='extract_weather_api',
    airbyte_conn_id='airbyte_default',
    connection_id='{{ var.value.weather_connection_id }}',
    asynchronous=False,
    timeout=3600,
    wait_seconds=10,
    dag=dag,
)

extract_postgres = AirbyteTriggerSyncOperator(
    task_id='extract_postgres_db',
    airbyte_conn_id='airbyte_default',
    connection_id='{{ var.value.postgres_connection_id }}',
    asynchronous=False,
    timeout=3600,
    wait_seconds=10,
    dag=dag,
)

extract_files = AirbyteTriggerSyncOperator(
    task_id='extract_csv_files',
    airbyte_conn_id='airbyte_default',
    connection_id='{{ var.value.file_connection_id }}',
    asynchronous=False,
    timeout=3600,
    wait_seconds=10,
    dag=dag,
)

# Task 2: Bronze DQ Checks
bronze_dq = BashOperator(
    task_id='bronze_dq_checks',
    bash_command='dbt test --select source:bronze',
    cwd='/dbt',
    dag=dag,
)

# Task 3: Silver Transformation
silver_transform = BashOperator(
    task_id='silver_transform',
    bash_command='dbt run --select silver',
    cwd='/dbt',
    dag=dag,
)

# Task 4: Silver DQ Checks
silver_dq = BashOperator(
    task_id='silver_dq_checks',
    bash_command='dbt test --select silver',
    cwd='/dbt',
    dag=dag,
)

# Task 5: Gold Transformation
gold_transform = BashOperator(
    task_id='gold_transform',
    bash_command='dbt run --select gold',
    cwd='/dbt',
    dag=dag,
)

# Task 6: Gold DQ Checks
gold_dq = BashOperator(
    task_id='gold_dq_checks',
    bash_command='dbt test --select gold',
    cwd='/dbt',
    dag=dag,
)

# Task 7: Log Pipeline Run
def log_pipeline_run(**context):
    from duckdb import connect
    import os
    
    conn = connect(':memory:')
    conn.execute("INSTALL iceberg; LOAD iceberg;")
    conn.execute(f"SET s3_endpoint='minio:9000';")
    # ... S3 config ...
    
    dag_run_id = context['dag_run'].run_id
    start_time = context['dag_run'].start_date
    end_time = datetime.now()
    status = 'success'
    
    conn.execute(f"""
        INSERT INTO iceberg_rest.gold.pipeline_runs
        VALUES ('{dag_run_id}', '{start_time}', '{end_time}', '{status}')
    """)
    conn.close()

log_run = PythonOperator(
    task_id='log_pipeline_run',
    python_callable=log_pipeline_run,
    provide_context=True,
    dag=dag,
)

# Define dependencies
[extract_weather, extract_postgres, extract_files] >> bronze_dq
bronze_dq >> silver_transform >> silver_dq
silver_dq >> gold_transform >> gold_dq
gold_dq >> log_run
```

### 7. Apache Superset

**Purpose**: Business intelligence and dashboarding

**Docker Image**: `apache/superset:latest`

**Configuration**:
- Port: 8088
- Database: SQLite for Superset metadata
- Admin User: `admin` / `admin` (configured via environment)

**Database Connection**:
```python
# Superset connects to DuckDB via SQLAlchemy
SQLALCHEMY_DATABASE_URI = 'duckdb:////data/warehouse.db'

# DuckDB connection with Iceberg extension
import duckdb
conn = duckdb.connect('/data/warehouse.db')
conn.execute("INSTALL iceberg; LOAD iceberg;")
conn.execute("SET s3_endpoint='minio:9000';")
# ... S3 config ...
```

**Dashboard Definitions** (`superset/dashboards/pipeline_metrics.json`):
```json
{
  "dashboard_title": "Pipeline Metrics",
  "slices": [
    {
      "slice_name": "DAG Run History",
      "viz_type": "table",
      "params": {
        "datasource": "gold.pipeline_runs",
        "metrics": ["count"],
        "groupby": ["status", "date"],
        "time_range": "Last 30 days"
      }
    },
    {
      "slice_name": "Row Counts by Layer",
      "viz_type": "bar",
      "params": {
        "datasource": "gold.layer_metrics",
        "metrics": ["row_count"],
        "groupby": ["layer", "table_name"]
      }
    }
  ]
}
```

## Data Models

### Bronze Layer Schema

**Purpose**: Preserve raw source data with minimal metadata

**Common Metadata Columns** (added to all Bronze tables):
```sql
_source_name STRING,          -- Source connector name
_ingested_at TIMESTAMP,       -- Airbyte sync timestamp
_file_path STRING,            -- S3 path to source file
_has_null_key BOOLEAN         -- Flag for records with null PK
```

**Partitioning Strategy**:
- Partition by `DATE(_ingested_at)` for efficient time-range queries
- Enables incremental processing and data retention policies

**Example Bronze Table**:
```sql
CREATE TABLE iceberg_rest.bronze.weather_data (
    location STRING,
    date STRING,              -- Raw string from source
    temperature STRING,       -- Raw string from source
    humidity STRING,
    condition STRING,
    _source_name STRING,
    _ingested_at TIMESTAMP,
    _file_path STRING,
    _has_null_key BOOLEAN
)
USING iceberg
PARTITIONED BY (days(_ingested_at))
LOCATION 's3://bronze/weather_data/';
```

### Silver Layer Schema

**Purpose**: Cleaned, typed, and deduplicated data

**Transformations Applied**:
1. Type casting (strings → proper types)
2. Deduplication (keep latest by `_ingested_at`)
3. Null handling (empty strings → NULL)
4. String trimming and UTF-8 encoding
5. ISO 8601 timestamp standardization

**Additional Metadata**:
```sql
_silver_processed_at TIMESTAMP  -- Silver transformation timestamp
```

**Example Silver Table**:
```sql
CREATE TABLE iceberg_rest.silver.weather_clean (
    location STRING NOT NULL,
    date DATE NOT NULL,
    temperature_celsius DOUBLE,
    humidity_percent DOUBLE,
    condition STRING,
    _ingested_at TIMESTAMP,
    _silver_processed_at TIMESTAMP NOT NULL,
    location_date STRING NOT NULL  -- Composite key
)
USING iceberg
PARTITIONED BY (months(date))
LOCATION 's3://silver/weather_clean/';
```

**Constraints**:
- Primary Key: `location_date` (enforced via dbt tests)
- Not Null: `location`, `date`, `_silver_processed_at`
- Unique: `location_date`

### Gold Layer Schema

**Purpose**: Business-ready aggregates and metrics

**Aggregation Patterns**:
1. **Daily Summaries**: Time-series aggregates by day
2. **Rolling Windows**: 7-day, 30-day moving averages
3. **Top-N Rankings**: Best/worst performers
4. **Cumulative Metrics**: Running totals, lifetime values

**Additional Metadata**:
```sql
_gold_updated_at TIMESTAMP  -- Last refresh timestamp
```

**Example Gold Table 1: Daily Weather Summary**:
```sql
CREATE TABLE iceberg_rest.gold.daily_weather_summary (
    date DATE NOT NULL,
    location_count INTEGER NOT NULL,
    avg_temperature DOUBLE NOT NULL,
    min_temperature DOUBLE NOT NULL,
    max_temperature DOUBLE NOT NULL,
    avg_humidity DOUBLE NOT NULL,
    _gold_updated_at TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (months(date))
LOCATION 's3://gold/daily_weather_summary/';
```

**Example Gold Table 2: Order Metrics**:
```sql
CREATE TABLE iceberg_rest.gold.order_metrics (
    date DATE NOT NULL,
    total_orders INTEGER NOT NULL,
    total_revenue DOUBLE NOT NULL,
    avg_order_value DOUBLE NOT NULL,
    unique_customers INTEGER NOT NULL,
    _gold_updated_at TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (months(date))
LOCATION 's3://gold/order_metrics/';
```

**Example Gold Table 3: Customer Lifetime Value**:
```sql
CREATE TABLE iceberg_rest.gold.customer_lifetime_value (
    customer_id STRING NOT NULL,
    first_order_date DATE,
    last_order_date DATE,
    total_orders INTEGER NOT NULL,
    total_spent DOUBLE NOT NULL,
    avg_order_value DOUBLE NOT NULL,
    days_since_last_order INTEGER,
    _gold_updated_at TIMESTAMP NOT NULL
)
USING iceberg
LOCATION 's3://gold/customer_lifetime_value/';
```

### Observability Tables

**Pipeline Runs** (Gold layer):
```sql
CREATE TABLE iceberg_rest.gold.pipeline_runs (
    dag_run_id STRING NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status STRING NOT NULL,  -- success, failed, running
    bronze_row_count INTEGER,
    silver_row_count INTEGER,
    gold_row_count INTEGER,
    duration_seconds INTEGER
)
USING iceberg
PARTITIONED BY (days(start_time))
LOCATION 's3://gold/pipeline_runs/';
```

**DQ Results** (Gold layer):
```sql
CREATE TABLE iceberg_rest.gold.dq_results (
    check_id STRING NOT NULL,
    check_name STRING NOT NULL,
    layer STRING NOT NULL,  -- bronze, silver, gold
    table_name STRING NOT NULL,
    check_timestamp TIMESTAMP NOT NULL,
    status STRING NOT NULL,  -- passed, failed
    row_count INTEGER,
    error_message STRING
)
USING iceberg
PARTITIONED BY (days(check_timestamp))
LOCATION 's3://gold/dq_results/';
```

## Docker Compose Service Definitions

### Complete docker-compose.yml


```yaml
version: '3.8'

services:
  # ============================================
  # MinIO Object Storage
  # ============================================
  minio:
    image: minio/minio:latest
    container_name: minio
    ports:
      - "9000:9000"  # S3 API
      - "9001:9001"  # Console UI
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      timeout: 10s
      retries: 3
    networks:
      - medallion_net

  # MinIO bucket initialization
  minio_init:
    image: minio/mc:latest
    container_name: minio_init
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 ${MINIO_ROOT_USER} ${MINIO_ROOT_PASSWORD};
      mc mb local/bronze --ignore-existing;
      mc mb local/silver --ignore-existing;
      mc mb local/gold --ignore-existing;
      mc mb local/iceberg-warehouse --ignore-existing;
      echo 'Buckets created successfully';
      "
    networks:
      - medallion_net

  # ============================================
  # Iceberg REST Catalog
  # ============================================
  iceberg-rest:
    image: tabulario/iceberg-rest:latest
    container_name: iceberg-rest
    ports:
      - "8181:8181"
    environment:
      CATALOG_WAREHOUSE: s3://iceberg-warehouse/
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
      CATALOG_S3_ENDPOINT: http://minio:9000
      CATALOG_S3_ACCESS__KEY__ID: ${MINIO_ROOT_USER}
      CATALOG_S3_SECRET__ACCESS__KEY: ${MINIO_ROOT_PASSWORD}
      CATALOG_S3_PATH__STYLE__ACCESS: "true"
    depends_on:
      minio:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8181/v1/config"]
      interval: 30s
      timeout: 10s
      retries: 3
    networks:
      - medallion_net

  # ============================================
  # PostgreSQL (Airbyte + Airflow + Seed DB)
  # ============================================
  postgres:
    image: postgres:15-alpine
    container_name: postgres
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_MULTIPLE_DATABASES: airbyte,airflow,seed_db
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./scripts/init-multiple-dbs.sh:/docker-entrypoint-initdb.d/init-multiple-dbs.sh
      - ./scripts/seed-data.sql:/docker-entrypoint-initdb.d/seed-data.sql
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - medallion_net

  # ============================================
  # Airbyte
  # ============================================
  airbyte-db:
    image: airbyte/db:latest
    container_name: airbyte-db
    environment:
      POSTGRES_USER: airbyte
      POSTGRES_PASSWORD: airbyte
      POSTGRES_DB: airbyte
    volumes:
      - airbyte_db_data:/var/lib/postgresql/data
    networks:
      - medallion_net

  airbyte-server:
    image: airbyte/server:latest
    container_name: airbyte-server
    ports:
      - "8001:8001"
    environment:
      DATABASE_USER: airbyte
      DATABASE_PASSWORD: airbyte
      DATABASE_HOST: airbyte-db
      DATABASE_PORT: 5432
      DATABASE_DB: airbyte
      WORKSPACE_ROOT: /workspace
      CONFIG_ROOT: /configs
    volumes:
      - airbyte_workspace:/workspace
      - airbyte_config:/configs
      - ./connections:/connections
    depends_on:
      airbyte-db:
        condition: service_started
    networks:
      - medallion_net

  airbyte-worker:
    image: airbyte/worker:latest
    container_name: airbyte-worker
    environment:
      DATABASE_USER: airbyte
      DATABASE_PASSWORD: airbyte
      DATABASE_HOST: airbyte-db
      DATABASE_PORT: 5432
      DATABASE_DB: airbyte
      WORKSPACE_ROOT: /workspace
      LOCAL_ROOT: /tmp/airbyte_local
    volumes:
      - airbyte_workspace:/workspace
      - /var/run/docker.sock:/var/run/docker.sock
    depends_on:
      - airbyte-server
    networks:
      - medallion_net

  airbyte-webapp:
    image: airbyte/webapp:latest
    container_name: airbyte-webapp
    ports:
      - "8000:80"
    environment:
      AIRBYTE_SERVER_HOST: airbyte-server
      AIRBYTE_SERVER_PORT: 8001
    depends_on:
      - airbyte-server
    networks:
      - medallion_net

  # ============================================
  # dbt Runner
  # ============================================
  dbt:
    build:
      context: ./docker/dbt
      dockerfile: Dockerfile
    container_name: dbt
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    volumes:
      - ./dbt:/dbt
    depends_on:
      iceberg-rest:
        condition: service_healthy
      minio:
        condition: service_healthy
    networks:
      - medallion_net
    command: ["tail", "-f", "/dev/null"]  # Keep container running

  # ============================================
  # Apache Airflow
  # ============================================
  airflow-init:
    image: apache/airflow:2.8.0-python3.11
    container_name: airflow-init
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
    depends_on:
      postgres:
        condition: service_healthy
    entrypoint: >
      /bin/bash -c "
      airflow db init &&
      airflow users create \
        --username admin \
        --password admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com
      "
    networks:
      - medallion_net

  airflow-webserver:
    image: apache/airflow:2.8.0-python3.11
    container_name: airflow-webserver
    ports:
      - "8080:8080"
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      AIRFLOW__WEBSERVER__EXPOSE_CONFIG: "true"
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ./dbt:/dbt
    depends_on:
      - airflow-init
    command: webserver
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3
    networks:
      - medallion_net

  airflow-scheduler:
    image: apache/airflow:2.8.0-python3.11
    container_name: airflow-scheduler
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ./dbt:/dbt
    depends_on:
      - airflow-init
    command: scheduler
    networks:
      - medallion_net

  # ============================================
  # Apache Superset
  # ============================================
  superset:
    image: apache/superset:latest
    container_name: superset
    ports:
      - "8088:8088"
    environment:
      SUPERSET_SECRET_KEY: ${SUPERSET_SECRET_KEY}
      SUPERSET_LOAD_EXAMPLES: "no"
    volumes:
      - superset_data:/app/superset_home
      - ./superset/dashboards:/app/dashboards
    depends_on:
      iceberg-rest:
        condition: service_healthy
    entrypoint: >
      /bin/bash -c "
      superset db upgrade &&
      superset fab create-admin \
        --username admin \
        --firstname Admin \
        --lastname User \
        --email admin@example.com \
        --password admin &&
      superset init &&
      gunicorn \
        --bind 0.0.0.0:8088 \
        --workers 4 \
        --timeout 120 \
        --limit-request-line 0 \
        --limit-request-field_size 0 \
        'superset.app:create_app()'
      "
    networks:
      - medallion_net

networks:
  medallion_net:
    driver: bridge

volumes:
  minio_data:
  postgres_data:
  airbyte_db_data:
  airbyte_workspace:
  airbyte_config:
  superset_data:
```

### Network Architecture

All services communicate via the `medallion_net` Docker bridge network:

- **Internal DNS**: Services reference each other by container name (e.g., `minio:9000`)
- **Port Exposure**: Only UI/API ports exposed to host (9001, 8000, 8080, 8088, 8181)
- **Security**: Data plane traffic stays within Docker network

### Volume Management

**Named Volumes** (persist across restarts):
- `minio_data`: Object storage data
- `postgres_data`: Database state
- `airbyte_db_data`, `airbyte_workspace`, `airbyte_config`: Airbyte state
- `superset_data`: Superset metadata

**Bind Mounts** (development workflow):
- `./dags`: Airflow DAG definitions
- `./dbt`: dbt project files
- `./connections`: Airbyte connection configs
- `./superset/dashboards`: Dashboard exports

## Error Handling

### Airbyte Sync Failures

**Failure Modes**:
1. Source unreachable (network timeout, API down)
2. Authentication failure (invalid credentials)
3. Schema mismatch (source schema changed)
4. Destination write failure (MinIO unavailable)

**Handling Strategy**:
```python
# Airflow DAG error handling
extract_weather = AirbyteTriggerSyncOperator(
    task_id='extract_weather_api',
    airbyte_conn_id='airbyte_default',
    connection_id='{{ var.value.weather_connection_id }}',
    asynchronous=False,
    timeout=3600,
    wait_seconds=10,
    retries=2,
    retry_delay=timedelta(minutes=5),
    on_failure_callback=notify_failure,
    dag=dag,
)

def notify_failure(context):
    task_instance = context['task_instance']
    log.error(f"Task {task_instance.task_id} failed: {context['exception']}")
    # Log to dq_results table
    # Send alert (Slack, email, etc.)
```

**Partial Data Prevention**:
- Airbyte writes to staging location first
- Only commits to Bronze on successful completion
- Failed syncs leave no partial data in Bronze

### dbt Transformation Failures

**Failure Modes**:
1. SQL syntax error in model
2. Schema test failure (not_null, unique violations)
3. DuckDB out of memory
4. Iceberg catalog unavailable

**Handling Strategy**:
```yaml
# dbt_project.yml
on-run-start:
  - "{{ log('Starting dbt run at ' ~ run_started_at, info=True) }}"

on-run-end:
  - "{{ log_dbt_results() }}"  # Custom macro to log to gold.dq_results

models:
  medallion_pipeline:
    +on_schema_change: fail  # Explicit failure on unexpected schema changes
```

**Rollback Strategy**:
- Iceberg ACID transactions ensure atomic commits
- Failed transformations don't corrupt existing tables
- Time travel enables rollback to previous snapshot

### DuckDB Resource Exhaustion

**Memory Management**:
```sql
-- DuckDB configuration
SET memory_limit='4GB';
SET threads=4;
SET temp_directory='/tmp/duckdb';
```

**Incremental Processing**:
- Process data in date partitions
- Use dbt incremental materialization
- Avoid full table scans in Gold layer

### Airflow Task Failures

**Retry Configuration**:
```python
default_args = {
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}
```

**Dependency Handling**:
- Upstream task failure skips downstream tasks
- `trigger_rule='all_success'` (default) ensures clean state
- Failed DAG runs don't corrupt data (ACID guarantees)

### Catalog Unavailability

**Failure Impact**:
- dbt cannot read/write Iceberg tables
- Queries fail with "catalog unreachable" error

**Mitigation**:
```python
# Health check before dbt run
def check_catalog_health():
    import requests
    response = requests.get('http://iceberg-rest:8181/v1/config', timeout=10)
    if response.status_code != 200:
        raise Exception("Iceberg catalog unhealthy")

check_catalog = PythonOperator(
    task_id='check_catalog_health',
    python_callable=check_catalog_health,
    dag=dag,
)

check_catalog >> silver_transform
```

## Security Considerations

### Credentials Management

**Environment Variables** (`.env` file):
```bash
# MinIO
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=<strong-password>

# Superset
SUPERSET_SECRET_KEY=<random-secret-key>

# Airbyte API
AIRBYTE_API_KEY=<api-key>
```

**Best Practices**:
- `.env` file excluded from version control (`.gitignore`)
- `.env.example` provided with placeholder values
- Rotate credentials regularly
- Use strong passwords (16+ characters, mixed case, symbols)

### Network Isolation

**Docker Network Security**:
- All services on isolated `medallion_net` bridge network
- No direct host network access
- Only necessary ports exposed to host

**Service-to-Service Communication**:
- Internal DNS resolution (no hardcoded IPs)
- No TLS required (local development only)
- Production deployment would add TLS/mTLS

### Data Access Control

**MinIO Bucket Policies**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {"AWS": ["*"]},
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": ["arn:aws:s3:::bronze/*", "arn:aws:s3:::silver/*", "arn:aws:s3:::gold/*"]
    }
  ]
}
```

**Iceberg Catalog Authorization**:
- REST catalog has no built-in auth (local dev)
- Production would integrate with OAuth2/OIDC

### Secrets in Airflow

**Airflow Connections**:
```python
# Store credentials in Airflow Connections (encrypted in metadata DB)
from airflow.models import Connection

conn = Connection(
    conn_id='minio_s3',
    conn_type='aws',
    extra={
        'aws_access_key_id': os.getenv('MINIO_ROOT_USER'),
        'aws_secret_access_key': os.getenv('MINIO_ROOT_PASSWORD'),
        'endpoint_url': 'http://minio:9000'
    }
)
```

**Airflow Variables**:
```python
# Store non-sensitive config in Airflow Variables
from airflow.models import Variable

Variable.set('weather_connection_id', 'abc-123-def')
Variable.set('postgres_connection_id', 'xyz-789-ghi')
```

## Performance Optimization

### Partitioning Strategy

**Bronze Layer**:
- Partition by `DATE(_ingested_at)`
- Enables efficient incremental reads
- Supports data retention policies (drop old partitions)

**Silver Layer**:
- Partition by `MONTH(date)` for time-series data
- Partition by `category` for high-cardinality dimensions
- Balance partition size (target: 100MB-1GB per partition)

**Gold Layer**:
- Partition by `MONTH(date)` for time-series aggregates
- No partitioning for small dimension tables (<10K rows)

### Incremental Processing

**dbt Incremental Models**:
```sql
{{
  config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge'
  )
}}

SELECT * FROM {{ source('bronze', 'orders') }}
{% if is_incremental() %}
WHERE _ingested_at > (SELECT MAX(_silver_processed_at) FROM {{ this }})
{% endif %}
```

**Benefits**:
- Process only new data (10x-100x faster)
- Reduce DuckDB memory usage
- Enable frequent pipeline runs (hourly instead of daily)

### Query Optimization

**DuckDB Best Practices**:
```sql
-- Use column pruning
SELECT id, name, date FROM table  -- Good
SELECT * FROM table               -- Bad (reads all columns)

-- Push down filters
SELECT * FROM table WHERE date >= '2024-01-01'  -- Good (partition pruning)
SELECT * FROM table                              -- Bad (full scan)

-- Use appropriate joins
SELECT * FROM a INNER JOIN b ON a.id = b.id  -- Good
SELECT * FROM a, b WHERE a.id = b.id         -- Bad (cross join + filter)
```

**Iceberg Metadata Pruning**:
- Iceberg tracks min/max values per data file
- Filters on partitioned columns skip entire files
- Example: `WHERE date = '2024-01-15'` reads only 1 partition

### Resource Limits

**Docker Compose Resource Constraints**:
```yaml
services:
  dbt:
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 4G
        reservations:
          cpus: '1.0'
          memory: 2G
```

**DuckDB Configuration**:
```sql
SET memory_limit='4GB';
SET threads=4;
SET max_memory='4GB';
SET temp_directory='/tmp/duckdb';
```

## Testing Strategy

### Unit Tests

**dbt Schema Tests**:
```yaml
# models/silver/schema.yml
models:
  - name: weather_clean
    tests:
      - dbt_utils.recency:
          datepart: day
          field: _silver_processed_at
          interval: 1
    columns:
      - name: location_date
        tests:
          - unique
          - not_null
      - name: temperature_celsius
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: -100
              max_value: 100
```

**Custom Data Tests**:
```sql
-- tests/assert_positive_metrics.sql
SELECT *
FROM {{ ref('order_metrics') }}
WHERE total_orders < 0
   OR total_revenue < 0
   OR avg_order_value < 0
```

### Integration Tests

**End-to-End Pipeline Test**:
```bash
#!/bin/bash
# tests/integration/test_pipeline.sh

# 1. Seed test data
docker exec postgres psql -U postgres -d seed_db -f /test_data.sql

# 2. Trigger Airbyte sync
curl -X POST http://localhost:8001/api/v1/connections/${CONN_ID}/sync

# 3. Run dbt transformations
docker exec dbt dbt run

# 4. Validate Gold layer
docker exec dbt dbt test --select gold

# 5. Query Superset API
curl http://localhost:8088/api/v1/chart/1/data | jq '.result.data | length'
```

### Data Quality Checks

**Bronze Layer DQ**:
```sql
-- Assert non-zero row count
SELECT COUNT(*) AS row_count
FROM iceberg_rest.bronze.weather_data
WHERE DATE(_ingested_at) = CURRENT_DATE
HAVING COUNT(*) = 0;  -- Fails if zero rows
```

**Silver Layer DQ**:
```sql
-- Assert no null primary keys
SELECT COUNT(*) AS null_pk_count
FROM iceberg_rest.silver.weather_clean
WHERE location_date IS NULL
HAVING COUNT(*) > 0;  -- Fails if any nulls
```

**Gold Layer DQ**:
```sql
-- Assert metrics are non-negative
SELECT COUNT(*) AS negative_metric_count
FROM iceberg_rest.gold.order_metrics
WHERE total_orders < 0 OR total_revenue < 0
HAVING COUNT(*) > 0;  -- Fails if any negatives
```

## Implementation Plan

### Phase 1: Infrastructure Setup (Week 1)

**Milestone**: All services start successfully with `docker compose up`

**Tasks**:
1. Create `docker-compose.yml` with all service definitions
2. Create `.env.example` with required environment variables
3. Write `scripts/init-multiple-dbs.sh` for PostgreSQL initialization
4. Write `scripts/seed-data.sql` with sample data
5. Test service health checks and inter-service connectivity
6. Document port mappings and access URLs in README

**Validation**:
```bash
docker compose up -d
docker compose ps  # All services "healthy"
curl http://localhost:9001  # MinIO console loads
curl http://localhost:8000  # Airbyte UI loads
curl http://localhost:8080  # Airflow UI loads
curl http://localhost:8088  # Superset UI loads
curl http://localhost:8181/v1/config  # Iceberg catalog responds
```

### Phase 2: Data Ingestion (Week 2)

**Milestone**: Airbyte successfully loads data to Bronze layer

**Tasks**:
1. Configure 3 Airbyte source connectors (REST API, PostgreSQL, CSV)
2. Configure Airbyte S3 destination pointing to MinIO
3. Create `connections/*.yaml` files for connection-as-code
4. Write Airflow DAG tasks to trigger Airbyte syncs
5. Verify data lands in `bronze/` MinIO bucket
6. Create Bronze Iceberg tables via DuckDB

**Validation**:
```bash
# Trigger manual sync via Airbyte UI
# Check MinIO console for bronze/ objects
mc ls local/bronze/weather_api/weather_forecast/

# Query Bronze table
docker exec dbt duckdb -c "
  SELECT COUNT(*) FROM iceberg_rest.bronze.weather_data;
"
```

### Phase 3: Silver Transformations (Week 3)

**Milestone**: dbt successfully transforms Bronze → Silver

**Tasks**:
1. Create dbt project structure (`dbt_project.yml`, `profiles.yml`)
2. Define Bronze sources in `models/bronze/sources.yml`
3. Write Silver models with cleaning logic
4. Add dbt schema tests for Silver tables
5. Configure DuckDB connection with Iceberg extension
6. Test dbt run locally, then integrate with Airflow

**Validation**:
```bash
docker exec dbt dbt run --select silver
docker exec dbt dbt test --select silver

# Verify Silver tables exist
docker exec dbt duckdb -c "
  SELECT COUNT(*) FROM iceberg_rest.silver.weather_clean;
"
```

### Phase 4: Gold Aggregations (Week 4)

**Milestone**: dbt successfully transforms Silver → Gold

**Tasks**:
1. Write Gold models with business aggregations
2. Implement incremental materialization for Gold models
3. Add dbt tests for Gold tables
4. Create observability tables (`pipeline_runs`, `dq_results`)
5. Integrate Gold transformations into Airflow DAG

**Validation**:
```bash
docker exec dbt dbt run --select gold
docker exec dbt dbt test --select gold

# Verify Gold tables exist
docker exec dbt duckdb -c "
  SELECT * FROM iceberg_rest.gold.daily_weather_summary LIMIT 10;
"
```

### Phase 5: Orchestration (Week 5)

**Milestone**: Airflow DAG runs end-to-end successfully

**Tasks**:
1. Complete `dags/medallion_pipeline.py` with all tasks
2. Configure Airflow connections (Airbyte, MinIO)
3. Set Airflow variables (connection IDs)
4. Implement error handling and retries
5. Add logging to `pipeline_runs` table
6. Test manual DAG trigger, then enable schedule

**Validation**:
```bash
# Trigger DAG via Airflow UI
# Monitor task execution in Airflow logs
# Verify all tasks succeed

# Check pipeline_runs table
docker exec dbt duckdb -c "
  SELECT * FROM iceberg_rest.gold.pipeline_runs ORDER BY start_time DESC LIMIT 5;
"
```

### Phase 6: Dashboards (Week 6)

**Milestone**: Superset dashboards display Gold layer data

**Tasks**:
1. Configure Superset database connection to DuckDB
2. Create Superset datasets from Gold tables
3. Build 2 dashboards (pipeline metrics, business KPIs)
4. Export dashboards to `superset/dashboards/*.json`
5. Automate dashboard import on Superset startup

**Validation**:
```bash
# Access Superset UI at http://localhost:8088
# Verify dashboards load and display data
# Test dashboard refresh after pipeline run
```

### Phase 7: Advanced Features (Week 7)

**Milestone**: Schema evolution and time travel demonstrated

**Tasks**:
1. Write `docs/schema-evolution.md` runbook
2. Demonstrate adding column to Silver table
3. Write `docs/time-travel.md` runbook
4. Demonstrate querying Gold table at prior snapshot
5. Test incremental processing (run pipeline twice)
6. Verify idempotency (same input → same output)

**Validation**:
```sql
-- Schema evolution
ALTER TABLE iceberg_rest.silver.weather_clean ADD COLUMN wind_speed DOUBLE;
SELECT * FROM iceberg_rest.silver.weather_clean LIMIT 1;  -- New column present

-- Time travel
SELECT snapshot_id, committed_at FROM iceberg_rest.silver.weather_clean.snapshots;
SELECT * FROM iceberg_rest.silver.weather_clean FOR SYSTEM_TIME AS OF '2024-01-15 10:00:00';
```

### Phase 8: Documentation & Polish (Week 8)

**Milestone**: Repository is portfolio-ready

**Tasks**:
1. Write comprehensive README with architecture diagram
2. Create `docs/architecture.md` with detailed Mermaid diagrams
3. Write `docs/milestones.md` with learning objectives
4. Add inline code comments and docstrings
5. Create demo video or screenshots
6. Write blog post explaining design decisions

**Deliverables**:
- README.md with quickstart guide
- docs/architecture.md with system design
- docs/milestones.md with learning path
- docs/schema-evolution.md runbook
- docs/time-travel.md runbook
- Demo video or screenshots

## Conclusion

This design provides a complete blueprint for building a production-grade Batch ELT Medallion Pipeline running entirely on Docker Compose. The architecture demonstrates modern data engineering patterns including ACID transactions, schema evolution, incremental processing, and comprehensive observability—all at zero cost.

The phased implementation plan breaks the project into manageable milestones, each with clear validation criteria. By following this design, you'll build a portfolio piece that showcases expertise in data lakehouse architecture, workflow orchestration, and modern data stack technologies.



## Property-Based Testing Applicability Assessment

### Is PBT Appropriate for This Feature?

**Answer: NO** - Property-based testing is NOT appropriate for this batch ELT pipeline project.

**Reasoning**:

This project falls into multiple categories where PBT is explicitly not recommended:

1. **Infrastructure as Code (IaC)**: The Docker Compose configuration, service definitions, and infrastructure setup are declarative configurations, not functions with testable input/output behavior.

2. **Integration-Heavy System**: The core value is in the integration between multiple external systems (Airbyte, MinIO, Iceberg, dbt, Airflow, Superset), not in pure algorithmic logic.

3. **Side-Effect Operations**: Most operations are side-effect-driven:
   - Airbyte writes data to MinIO (I/O operation)
   - dbt transforms data in Iceberg tables (database operations)
   - Airflow orchestrates workflows (scheduling and state management)
   - Superset renders dashboards (UI rendering)

4. **Configuration Validation**: Data quality checks and schema validations are better suited to example-based tests and schema validation tools.

### Alternative Testing Strategies

Instead of property-based testing, this project should use:

1. **Integration Tests**: End-to-end pipeline tests that verify data flows correctly through all layers
2. **Example-Based Unit Tests**: dbt schema tests with specific assertions (not_null, unique, accepted_range)
3. **Snapshot Tests**: Verify Docker Compose configuration generates expected service definitions
4. **Schema Validation**: Use dbt tests and Iceberg schema enforcement
5. **Smoke Tests**: Health checks to verify all services start correctly
6. **Data Quality Checks**: SQL-based assertions on row counts, null values, and metric ranges

### Testing Approach Summary

The Testing Strategy section already outlines the appropriate testing approach:
- **Unit Tests**: dbt schema tests and custom data tests
- **Integration Tests**: End-to-end pipeline validation scripts
- **Data Quality Checks**: SQL assertions at Bronze, Silver, and Gold boundaries
- **Health Checks**: Docker Compose health checks for service availability

These testing strategies are more appropriate than property-based testing for validating infrastructure, data pipelines, and system integrations.

