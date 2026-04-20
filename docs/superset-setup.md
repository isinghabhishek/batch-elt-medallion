# Superset Setup Guide

This guide walks through configuring Apache Superset to connect to the Gold layer and create dashboards for the Medallion Pipeline.

## Overview

Superset provides business intelligence and data visualization capabilities for the pipeline. It connects to the Gold layer to display:
- Pipeline metrics (DAG runs, row counts, DQ checks)
- Business KPIs (weather trends, order metrics, customer lifetime value)

## Prerequisites

- All services running via `docker compose up -d`
- Gold layer tables populated with data
- Superset accessible at http://localhost:8088

## Step 1: Access Superset

1. Navigate to http://localhost:8088
2. Login with default credentials:
   - Username: `admin`
   - Password: `admin`

## Step 2: Configure Database Connection

Superset needs to connect to DuckDB to query Gold layer Parquet files stored in MinIO.

### Option A: DuckDB with Direct S3 Access (Recommended)

This approach uses DuckDB to read Parquet files directly from MinIO S3.

#### 2.1 Install DuckDB Driver

First, install the DuckDB SQLAlchemy driver in the Superset container:

```bash
docker exec -it superset pip install duckdb-engine
```

#### 2.2 Create Database Connection

1. In Superset UI, navigate to **Settings > Database Connections**
2. Click **+ Database**
3. Select **Other** as the database type
4. Fill in the connection details:

**Display Name**: `Gold Layer (DuckDB)`

**SQLAlchemy URI**:
```
duckdb:////tmp/gold_layer.db
```

**Advanced Settings** → **SQL Lab** → Enable:
- ✅ Expose database in SQL Lab
- ✅ Allow CREATE TABLE AS
- ✅ Allow CREATE VIEW AS
- ✅ Allow DML

**Advanced Settings** → **Other** → **Engine Parameters**:
```json
{
  "connect_args": {
    "read_only": false,
    "config": {
      "s3_endpoint": "minio:9000",
      "s3_access_key_id": "minioadmin",
      "s3_secret_access_key": "minioadmin",
      "s3_use_ssl": "false",
      "s3_url_style": "path"
    }
  }
}
```

4. Click **Test Connection**
5. If successful, click **Connect**

### Option B: PostgreSQL with Foreign Data Wrapper (Alternative)

If DuckDB connection has issues, you can use PostgreSQL with a foreign data wrapper to query Parquet files.

#### 2.1 Create PostgreSQL Connection

1. Navigate to **Settings > Database Connections**
2. Click **+ Database**
3. Select **PostgreSQL**
4. Fill in:
   - **Host**: `postgres`
   - **Port**: `5432`
   - **Database**: `seed_db`
   - **Username**: `postgres`
   - **Password**: `postgres`

## Step 3: Create SQL Lab Queries

Before creating datasets, test queries in SQL Lab to ensure connectivity.

### 3.1 Initialize DuckDB Extensions

In SQL Lab, run this initialization query:

```sql
INSTALL httpfs;
LOAD httpfs;

SET s3_endpoint='minio:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;
SET s3_url_style='path';
```

### 3.2 Test Gold Layer Queries

#### Query Pipeline Runs

```sql
SELECT 
    dag_run_id,
    start_time,
    end_time,
    status,
    bronze_row_count,
    silver_row_count,
    gold_row_count,
    duration_seconds
FROM read_parquet('s3://gold/pipeline_runs/*.parquet')
ORDER BY start_time DESC
LIMIT 10;
```

#### Query Order Metrics

```sql
SELECT 
    date,
    total_orders,
    total_revenue,
    avg_order_value,
    unique_customers
FROM read_parquet('s3://gold/order_metrics/*.parquet')
ORDER BY date DESC
LIMIT 30;
```

#### Query Customer Lifetime Value

```sql
SELECT 
    customer_id,
    total_orders,
    total_spent,
    avg_order_value,
    days_since_last_order
FROM read_parquet('s3://gold/customer_lifetime_value/*.parquet')
ORDER BY total_spent DESC
LIMIT 10;
```

## Step 4: Create Datasets

Datasets in Superset represent tables or queries that can be used in charts.

### 4.1 Create Pipeline Runs Dataset

1. Navigate to **Data > Datasets**
2. Click **+ Dataset**
3. Select database: **Gold Layer (DuckDB)**
4. Enter SQL:
```sql
SELECT * FROM read_parquet('s3://gold/pipeline_runs/*.parquet')
```
5. Name: `pipeline_runs`
6. Click **Create Dataset and Create Chart**

### 4.2 Create Order Metrics Dataset

1. Click **+ Dataset**
2. Select database: **Gold Layer (DuckDB)**
3. Enter SQL:
```sql
SELECT * FROM read_parquet('s3://gold/order_metrics/*.parquet')
```
4. Name: `order_metrics`
5. Click **Create Dataset and Create Chart**

### 4.3 Create Customer Lifetime Value Dataset

1. Click **+ Dataset**
2. Select database: **Gold Layer (DuckDB)**
3. Enter SQL:
```sql
SELECT * FROM read_parquet('s3://gold/customer_lifetime_value/*.parquet')
```
4. Name: `customer_lifetime_value`
5. Click **Create Dataset and Create Chart**

### 4.4 Create DQ Results Dataset

1. Click **+ Dataset**
2. Select database: **Gold Layer (DuckDB)**
3. Enter SQL:
```sql
SELECT * FROM read_parquet('s3://gold/dq_results/*.parquet')
```
4. Name: `dq_results`
5. Click **Create Dataset and Create Chart**

## Step 5: Configure Dataset Settings

For each dataset:

1. Navigate to **Data > Datasets**
2. Click on the dataset name
3. Go to **Settings** tab
4. Configure:
   - **Cache Timeout**: 300 seconds (5 minutes)
   - **Offset**: 0
   - **Default Endpoint**: Leave blank

5. Go to **Columns** tab
6. For timestamp columns (start_time, end_time, check_timestamp, date):
   - Set **Type**: `DATETIME` or `DATE`
   - Enable **Is temporal**
   - Enable **Is filterable**

7. Click **Save**

## Troubleshooting

### Issue: "Cannot connect to database"

**Solution**: Verify DuckDB driver is installed:
```bash
docker exec -it superset pip list | grep duckdb
```

If not installed:
```bash
docker exec -it superset pip install duckdb-engine
docker restart superset
```

### Issue: "S3 access denied"

**Solution**: Verify MinIO credentials in Engine Parameters match `.env` file:
```bash
# Check .env file
cat .env | grep MINIO

# Test MinIO access
docker exec -it superset curl http://minio:9000/minio/health/live
```

### Issue: "No data returned"

**Solution**: Verify Gold layer has data:
```bash
# Check if Parquet files exist
docker exec -it dbt duckdb -c "
  SET s3_endpoint='minio:9000';
  SET s3_access_key_id='minioadmin';
  SET s3_secret_access_key='minioadmin';
  SET s3_use_ssl=false;
  SELECT COUNT(*) FROM read_parquet('s3://gold/pipeline_runs/*.parquet');
"
```

### Issue: "DuckDB extensions not loaded"

**Solution**: Run initialization query in SQL Lab before creating datasets:
```sql
INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint='minio:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;
SET s3_url_style='path';
```

## Alternative: Using Views

If direct Parquet queries are slow, create DuckDB views:

```sql
-- Create persistent database
CREATE DATABASE IF NOT EXISTS gold_views;

-- Create views
CREATE OR REPLACE VIEW gold_views.pipeline_runs AS
SELECT * FROM read_parquet('s3://gold/pipeline_runs/*.parquet');

CREATE OR REPLACE VIEW gold_views.order_metrics AS
SELECT * FROM read_parquet('s3://gold/order_metrics/*.parquet');

CREATE OR REPLACE VIEW gold_views.customer_lifetime_value AS
SELECT * FROM read_parquet('s3://gold/customer_lifetime_value/*.parquet');

CREATE OR REPLACE VIEW gold_views.dq_results AS
SELECT * FROM read_parquet('s3://gold/dq_results/*.parquet');
```

Then reference views in Superset datasets:
```sql
SELECT * FROM gold_views.pipeline_runs
```

## Security Best Practices

1. **Change Default Password**: Update Superset admin password after first login
2. **Use Secrets**: Store MinIO credentials in environment variables, not in Engine Parameters
3. **Enable HTTPS**: Configure SSL/TLS for production deployments
4. **Restrict Access**: Use Superset RBAC to limit dataset and dashboard access
5. **Rotate Credentials**: Regularly rotate MinIO access keys

## Next Steps

After completing database setup:
1. ✅ Database connection configured
2. ✅ Datasets created for all Gold tables
3. ✅ Dataset settings configured
4. → Proceed to creating dashboards (Task 9.3 and 9.4)

## Reference

- [Superset Documentation](https://superset.apache.org/docs/intro)
- [DuckDB SQLAlchemy Driver](https://github.com/Mause/duckdb_engine)
- [DuckDB S3 Configuration](https://duckdb.org/docs/extensions/httpfs.html)
