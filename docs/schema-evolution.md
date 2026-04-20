# Schema Evolution with Apache Iceberg

## Overview

This runbook demonstrates Apache Iceberg's schema evolution capabilities, which allow you to modify table schemas without rewriting existing data files. This is a critical feature for production data pipelines where schema changes are inevitable but data reprocessing is expensive.

## What is Schema Evolution?

Schema evolution enables you to:
- Add new columns to existing tables
- Rename columns
- Drop columns
- Change column types (with compatible conversions)
- Reorder columns

All of these operations update only the table metadata in the Iceberg catalog—existing Parquet data files remain untouched and readable.

## Prerequisites

- Docker Compose stack running (`docker compose up -d`)
- Bronze and Silver layers populated with data
- DuckDB CLI or dbt runner container access

## Demonstration: Adding a Column to Silver Layer

### Step 1: Verify Current Schema

First, let's examine the current schema of the `weather_clean` Silver table:

```bash
# Access the dbt container
docker exec -it dbt bash

# Start DuckDB with Iceberg support
duckdb :memory:
```

```sql
-- Load required extensions
INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;

-- Configure S3 connection to MinIO
SET s3_endpoint='minio:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
SET s3_use_ssl=false;
SET s3_url_style='path';

-- Connect to Iceberg catalog
CREATE SECRET iceberg_rest (
    TYPE ICEBERG_REST,
    CATALOG_URL 'http://iceberg-rest:8181',
    S3_ENDPOINT 'minio:9000',
    S3_ACCESS_KEY_ID 'minioadmin',
    S3_SECRET_ACCESS_KEY 'minioadmin',
    S3_USE_SSL false
);

-- View current schema
DESCRIBE iceberg_scan('iceberg-rest.silver.weather_clean');
```

**Expected Output:**
```
column_name              | column_type | null | key | default | extra
-------------------------|-------------|------|-----|---------|------
location                 | VARCHAR     | NO   |     |         |
date                     | DATE        | NO   |     |         |
temperature_celsius      | DOUBLE      | YES  |     |         |
humidity_percent         | DOUBLE      | YES  |     |         |
condition                | VARCHAR     | YES  |     |         |
_ingested_at             | TIMESTAMP   | YES  |     |         |
_silver_processed_at     | TIMESTAMP   | NO   |     |         |
location_date            | VARCHAR     | NO   |     |         |
```

### Step 2: Query Existing Data

Before adding the column, let's query the table to see current row count and sample data:

```sql
-- Count existing records
SELECT COUNT(*) as total_rows 
FROM iceberg_scan('iceberg-rest.silver.weather_clean');

-- Sample existing data
SELECT location, date, temperature_celsius, humidity_percent
FROM iceberg_scan('iceberg-rest.silver.weather_clean')
LIMIT 5;
```

### Step 3: Add New Column

Now we'll add a `wind_speed` column to demonstrate schema evolution:

```sql
-- Add new column to the Iceberg table
ALTER TABLE iceberg_rest.silver.weather_clean 
ADD COLUMN wind_speed DOUBLE;
```

**What Happens:**
1. Iceberg catalog updates the table metadata with the new column definition
2. A new snapshot is created recording this schema change
3. Existing Parquet files are NOT rewritten
4. The new column will have NULL values for all existing records

### Step 4: Verify Schema Update

Check that the schema now includes the new column:

```sql
-- View updated schema
DESCRIBE iceberg_scan('iceberg-rest.silver.weather_clean');
```

**Expected Output:**
```
column_name              | column_type | null | key | default | extra
-------------------------|-------------|------|-----|---------|------
location                 | VARCHAR     | NO   |     |         |
date                     | DATE        | NO   |     |         |
temperature_celsius      | DOUBLE      | YES  |     |         |
humidity_percent         | DOUBLE      | YES  |     |         |
condition                | VARCHAR     | YES  |     |         |
_ingested_at             | TIMESTAMP   | YES  |     |         |
_silver_processed_at     | TIMESTAMP   | NO   |     |         |
location_date            | VARCHAR     | NO   |     |         |
wind_speed               | DOUBLE      | YES  |     |         |  <-- NEW COLUMN
```

### Step 5: Query with New Column

Query the table including the new column:

```sql
-- Query with new column (will show NULL for existing records)
SELECT 
    location, 
    date, 
    temperature_celsius, 
    wind_speed,
    _silver_processed_at
FROM iceberg_scan('iceberg-rest.silver.weather_clean')
ORDER BY date DESC
LIMIT 10;
```

**Expected Result:**
- All existing records will have `wind_speed = NULL`
- The query executes successfully without errors
- No data files were rewritten

### Step 6: Verify Data Files Unchanged

Check the Iceberg table metadata to confirm no data files were rewritten:

```sql
-- List snapshots to see the schema evolution event
SELECT 
    snapshot_id,
    parent_id,
    operation,
    summary
FROM iceberg_rest.silver.weather_clean.snapshots
ORDER BY committed_at DESC;
```

**Expected Output:**
- A new snapshot with `operation = 'schema_update'` or similar
- The `summary` field shows no data files were added or deleted
- Previous snapshots remain unchanged

### Step 7: Insert New Data with Wind Speed

Now let's insert a new record that includes the wind_speed value:

```sql
-- Insert new record with wind_speed
INSERT INTO iceberg_rest.silver.weather_clean
VALUES (
    'San Francisco',
    DATE '2026-04-21',
    18.5,
    65.0,
    'Partly Cloudy',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    'San Francisco_2026-04-21',
    12.3  -- wind_speed value
);

-- Query to see old records (NULL wind_speed) and new record (populated wind_speed)
SELECT 
    location, 
    date, 
    temperature_celsius, 
    wind_speed,
    CASE 
        WHEN wind_speed IS NULL THEN 'Old Record'
        ELSE 'New Record'
    END as record_type
FROM iceberg_scan('iceberg-rest.silver.weather_clean')
ORDER BY date DESC
LIMIT 10;
```

## Alternative: Schema Evolution via dbt

You can also evolve schemas through dbt models by adding columns to your SQL:

### Update dbt Model

Edit `dbt/models/silver/weather_clean.sql`:

```sql
{{
  config(
    materialized='incremental',
    unique_key='location_date',
    on_schema_change='append_new_columns'  -- Enable automatic schema evolution
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
        CAST(wind_speed AS DOUBLE) AS wind_speed,  -- NEW COLUMN
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

### Run dbt to Apply Schema Change

```bash
# Run the updated model
dbt run --select weather_clean

# dbt will automatically add the new column to the Iceberg table
```

## Key Concepts

### Why Existing Files Remain Readable

Iceberg uses a schema-on-read approach:
1. **Metadata Layer**: The Iceberg catalog stores the current schema version
2. **Data Layer**: Parquet files store data with their own embedded schema
3. **Schema Reconciliation**: When reading, Iceberg reconciles the table schema with file schemas
4. **Missing Columns**: If a file doesn't contain a column from the table schema, Iceberg returns NULL values

### Schema Evolution Operations

| Operation | Metadata Update | Data Rewrite Required |
|-----------|----------------|----------------------|
| Add Column | Yes | No |
| Drop Column | Yes | No |
| Rename Column | Yes | No |
| Reorder Columns | Yes | No |
| Change Type (compatible) | Yes | No |
| Change Type (incompatible) | Yes | Yes (manual) |

### Compatible Type Changes

These type changes are safe and don't require data rewrites:
- `INT` → `BIGINT`
- `FLOAT` → `DOUBLE`
- `DECIMAL(10,2)` → `DECIMAL(20,2)` (increasing precision)

### Incompatible Type Changes

These require manual data migration:
- `STRING` → `INT`
- `BIGINT` → `INT` (narrowing)
- `TIMESTAMP` → `DATE`

## Validation Queries

### Check Schema Version History

```sql
-- View all schema changes over time
SELECT 
    snapshot_id,
    committed_at,
    operation,
    summary
FROM iceberg_rest.silver.weather_clean.snapshots
WHERE operation LIKE '%schema%'
ORDER BY committed_at;
```

### Verify Column Nullability

```sql
-- Count NULL values in new column
SELECT 
    COUNT(*) as total_rows,
    COUNT(wind_speed) as rows_with_wind_speed,
    COUNT(*) - COUNT(wind_speed) as rows_without_wind_speed
FROM iceberg_scan('iceberg-rest.silver.weather_clean');
```

### Compare File Counts Before/After

```sql
-- List data files (should be unchanged after schema evolution)
SELECT 
    file_path,
    file_size_in_bytes,
    record_count
FROM iceberg_rest.silver.weather_clean.files
ORDER BY file_path;
```

## Troubleshooting

### Error: "Column already exists"

If you try to add a column that already exists:
```sql
ALTER TABLE iceberg_rest.silver.weather_clean ADD COLUMN wind_speed DOUBLE;
-- Error: Column 'wind_speed' already exists
```

**Solution**: Check current schema first with `DESCRIBE`.

### Error: "Incompatible type change"

If you try an incompatible type change:
```sql
ALTER TABLE iceberg_rest.silver.weather_clean 
ALTER COLUMN temperature_celsius TYPE VARCHAR;
-- Error: Cannot change type from DOUBLE to VARCHAR
```

**Solution**: Create a new column, migrate data, drop old column.

### dbt Schema Evolution Not Working

If `on_schema_change='append_new_columns'` doesn't work:
- Verify dbt-duckdb version supports Iceberg schema evolution
- Check that the table exists before running incremental model
- Use `dbt run --full-refresh` to force a complete rebuild

## Best Practices

1. **Document Schema Changes**: Always document why and when schema changes were made
2. **Test in Development**: Test schema evolution on a copy of production data first
3. **Backward Compatibility**: Ensure downstream consumers can handle NULL values in new columns
4. **Avoid Dropping Columns**: Instead, deprecate columns and stop writing to them
5. **Use dbt for Automation**: Let dbt handle schema evolution automatically with `on_schema_change`

## Cleanup (Optional)

To revert the schema change:

```sql
-- Drop the added column
ALTER TABLE iceberg_rest.silver.weather_clean 
DROP COLUMN wind_speed;
```

## Summary

Schema evolution with Iceberg provides:
- **Zero-downtime schema changes**: No data rewrite required
- **Backward compatibility**: Old data files remain readable
- **Audit trail**: All schema changes recorded in snapshots
- **Flexibility**: Add, drop, rename, and reorder columns easily

This capability is essential for production data pipelines where schema changes are frequent but data reprocessing is expensive.
