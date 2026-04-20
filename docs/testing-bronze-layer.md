# Testing Bronze Layer - Task 3.8

This document provides instructions for testing Airbyte sync to the Bronze layer.

## Prerequisites

Before running these tests, ensure:

1. ✅ All Docker services are running (`docker compose ps` shows all healthy)
2. ✅ Airbyte connections are configured (Weather API, PostgreSQL, CSV Files)
3. ✅ Bronze Iceberg tables are created (Task 3.7 complete)
4. ✅ Python dependencies installed: `pip install boto3 duckdb requests`

## Test Objectives (Task 3.8)

This test validates:

- **Requirement 4.2**: Airbyte writes raw records to bronze/ MinIO bucket
- **Requirement 4.3**: Sync jobs complete successfully
- **Requirement 4.4**: Data is written in Parquet format
- **Requirement 5.1**: Bronze layer stores data as-is from source
- **Requirement 5.2**: Metadata columns are added (_source_name, _ingested_at, _file_path)
- **Requirement 5.3**: Bronze tables are partitioned by _ingested_at date

## Method 1: Automated Test Script

### Setup

1. **Get Airbyte Connection IDs**

   First, you need to create connections in Airbyte UI and get their IDs:

   ```bash
   # Access Airbyte UI
   open http://localhost:8000
   
   # After creating connections, get their IDs from the URL or API
   curl http://localhost:8001/api/v1/connections | jq '.connections[] | {name: .name, id: .connectionId}'
   ```

2. **Set Environment Variables**

   ```bash
   export WEATHER_CONNECTION_ID="your-weather-connection-id"
   export POSTGRES_CONNECTION_ID="your-postgres-connection-id"
   export FILE_CONNECTION_ID="your-file-connection-id"
   ```

3. **Run the Test Script**

   ```bash
   python scripts/test_bronze_sync.py
   ```

### Expected Output

```
============================================================
Task 3.8: Test Airbyte Sync to Bronze Layer
============================================================
Started at: 2024-01-15T10:30:00

############################################################
# Testing: Weather API
############################################################

============================================================
Testing Weather API sync...
============================================================
🚀 Triggering sync for connection: abc-123-def
✅ Sync job started: job-456-xyz
⏳ Waiting for sync to complete...
   Status: running
   Status: running
   Status: succeeded
✅ Sync completed successfully!

📦 Verifying MinIO bucket: bronze/weather_api/
✅ Found 3 files, total size: 45,678 bytes

   Sample files:
   - weather_api/weather_data/2024-01-15/data.parquet (15,226 bytes)
   - weather_api/weather_data/2024-01-15/data_001.parquet (15,226 bytes)
   - weather_api/weather_data/2024-01-15/data_002.parquet (15,226 bytes)

🔍 Querying Bronze table: weather_data
✅ Table exists with 1,234 rows

   Validating metadata columns...
   Table columns: location, date, temperature, humidity, condition, _source_name, _ingested_at, _file_path, _has_null_key
✅ All required metadata columns present

   Sample records with metadata:

   Record 1:
      _source_name: weather_api
      _ingested_at: 2024-01-15 10:32:15
      _file_path: s3://bronze/weather_api/weather_data/2024-01-15/data.parquet

   Metadata validation:
      Total rows: 1,234
      _source_name populated: 1,234 (100.0%)
      _ingested_at populated: 1,234 (100.0%)
      _file_path populated: 1,234 (100.0%)
      Ingestion time range: 2024-01-15 10:32:15 to 2024-01-15 10:32:15
✅ All metadata columns are 100% populated

============================================================
TEST SUMMARY
============================================================
Source               Sync            MinIO           Table          
------------------------------------------------------------
Weather API          ✅ Success      ✅ Verified     ✅ Validated   
PostgreSQL           ✅ Success      ✅ Verified     ✅ Validated   
CSV Files            ✅ Success      ✅ Verified     ✅ Validated   

============================================================
Completed at: 2024-01-15T10:35:00
============================================================
```

## Method 2: Manual Testing via Airbyte UI

### Step 1: Trigger Manual Sync

1. Open Airbyte UI: http://localhost:8000
2. Navigate to **Connections**
3. Select a connection (e.g., "Weather API → MinIO Bronze")
4. Click **"Sync Now"** button
5. Monitor the sync progress in the UI
6. Wait for status to show **"Succeeded"**

### Step 2: Verify Data in MinIO

1. Open MinIO Console: http://localhost:9001
2. Login with credentials from `.env` file
3. Navigate to **bronze** bucket
4. Browse to the source folder (e.g., `weather_api/weather_data/`)
5. Verify Parquet files exist with recent timestamps
6. Check file sizes are non-zero

### Step 3: Query Bronze Iceberg Table

Use DuckDB to query the Bronze table:

```python
import duckdb
import os

# Initialize connection
conn = duckdb.connect(':memory:')

# Install extensions
conn.execute("INSTALL iceberg;")
conn.execute("INSTALL httpfs;")
conn.execute("LOAD iceberg;")
conn.execute("LOAD httpfs;")

# Configure S3
conn.execute("SET s3_endpoint='http://localhost:9000';")
conn.execute("SET s3_access_key_id='admin';")
conn.execute("SET s3_secret_access_key='changeme123';")
conn.execute("SET s3_use_ssl=false;")
conn.execute("SET s3_url_style='path';")

# Create Iceberg catalog secret
conn.execute("""
    CREATE SECRET iceberg_rest (
        TYPE ICEBERG_REST,
        CATALOG_URL 'http://localhost:8181',
        S3_ENDPOINT 'http://localhost:9000',
        S3_ACCESS_KEY_ID 'admin',
        S3_SECRET_ACCESS_KEY 'changeme123',
        S3_USE_SSL false
    );
""")

# Query Bronze table
result = conn.execute("""
    SELECT 
        COUNT(*) as total_rows,
        COUNT(DISTINCT _source_name) as source_count,
        MIN(_ingested_at) as earliest_ingestion,
        MAX(_ingested_at) as latest_ingestion
    FROM iceberg_rest.bronze.weather_data
""").fetchall()

print(f"Total rows: {result[0][0]}")
print(f"Sources: {result[0][1]}")
print(f"Ingestion range: {result[0][2]} to {result[0][3]}")

# Sample records
sample = conn.execute("""
    SELECT * FROM iceberg_rest.bronze.weather_data LIMIT 5
""").fetchall()

for row in sample:
    print(row)

conn.close()
```

### Step 4: Validate Metadata Columns

Run this query to validate all metadata columns are populated:

```sql
SELECT 
    COUNT(*) as total_rows,
    COUNT(_source_name) as source_name_count,
    COUNT(_ingested_at) as ingested_at_count,
    COUNT(_file_path) as file_path_count,
    SUM(CASE WHEN _has_null_key THEN 1 ELSE 0 END) as null_key_count
FROM iceberg_rest.bronze.weather_data;
```

**Expected Results:**
- `source_name_count` = `total_rows` (100% populated)
- `ingested_at_count` = `total_rows` (100% populated)
- `file_path_count` = `total_rows` (100% populated)
- `null_key_count` = 0 (or small number if source has null keys)

## Method 3: Using Airbyte API

### Trigger Sync via API

```bash
# Get connection ID
CONNECTION_ID="your-connection-id"

# Trigger sync
curl -X POST http://localhost:8001/api/v1/connections/${CONNECTION_ID}/sync \
  -H "Content-Type: application/json" \
  -d '{}'

# Response will include job ID
# {"job": {"id": "job-123-abc", "status": "running"}}

# Check job status
JOB_ID="job-123-abc"
curl http://localhost:8001/api/v1/jobs/${JOB_ID} \
  -H "Content-Type: application/json"
```

## Validation Checklist

Use this checklist to confirm Task 3.8 is complete:

- [ ] **Sync Triggered**: Manual sync started via UI or API
- [ ] **Sync Completed**: Job status shows "succeeded"
- [ ] **MinIO Files**: Parquet files exist in bronze/ bucket
- [ ] **File Format**: Files are in Parquet format (Requirement 4.4)
- [ ] **Table Exists**: Bronze Iceberg table can be queried
- [ ] **Row Count**: Table contains expected number of rows
- [ ] **Metadata Columns**: All required columns present:
  - [ ] `_source_name` (populated 100%)
  - [ ] `_ingested_at` (populated 100%)
  - [ ] `_file_path` (populated 100%)
  - [ ] `_has_null_key` (present, may be false)
- [ ] **Partitioning**: Table is partitioned by `_ingested_at` date (Requirement 5.3)
- [ ] **Data Fidelity**: Raw data matches source (Requirement 5.1)

## Troubleshooting

### Issue: Airbyte sync fails

**Solution:**
1. Check Airbyte logs: `docker logs airbyte-worker`
2. Verify source is reachable (API endpoint, database connection)
3. Check Airbyte UI for error messages
4. Verify MinIO credentials in Airbyte destination config

### Issue: No files in MinIO bucket

**Solution:**
1. Verify sync job completed successfully
2. Check MinIO bucket name matches destination config
3. Verify MinIO credentials are correct
4. Check Airbyte worker logs for S3 write errors

### Issue: Cannot query Iceberg table

**Solution:**
1. Verify Iceberg REST Catalog is running: `curl http://localhost:8181/v1/config`
2. Check table was created in Task 3.7
3. Verify DuckDB Iceberg extension is loaded
4. Check S3 credentials in DuckDB connection

### Issue: Metadata columns are NULL

**Solution:**
1. Verify Bronze table schema includes metadata columns
2. Check Airbyte destination config adds metadata
3. Re-create Bronze table with correct schema
4. Re-run sync job

## Success Criteria

Task 3.8 is complete when:

1. ✅ At least one Airbyte sync completes successfully
2. ✅ Data files exist in MinIO bronze/ bucket
3. ✅ Bronze Iceberg table can be queried
4. ✅ All metadata columns are 100% populated
5. ✅ Data matches source records (spot check)

## Next Steps

After completing Task 3.8:

- **Checkpoint 2**: Validate infrastructure is working end-to-end
- **Phase 3**: Begin Silver layer transformations (Task 4.1)
- Document any issues or learnings in project notes

## References

- **Requirements**: 4.2, 4.3, 4.4, 5.1, 5.2, 5.3
- **Design Document**: Section on Bronze Layer Schema
- **Airbyte Docs**: https://docs.airbyte.com/
- **Apache Iceberg**: https://iceberg.apache.org/
