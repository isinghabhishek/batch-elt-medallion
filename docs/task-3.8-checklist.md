# Task 3.8 Completion Checklist

## Overview
This checklist helps you verify that Task 3.8 (Test Airbyte sync to Bronze layer) is complete.

**Requirements Validated**: 4.2, 4.3, 4.4, 5.1, 5.2, 5.3

---

## Prerequisites ✓

Before starting, verify:

- [ ] Docker Desktop is installed and running
- [ ] All services are healthy: `docker compose ps`
- [ ] Airbyte UI is accessible: http://localhost:8000
- [ ] MinIO Console is accessible: http://localhost:9001
- [ ] Iceberg Catalog is accessible: http://localhost:8181/v1/config
- [ ] Bronze Iceberg tables are created (Task 3.7 complete)

---

## Test Execution

### Part 1: Trigger Airbyte Sync ✓

**Requirement 4.2, 4.3**: Airbyte writes raw records to bronze/ bucket

#### Option A: Via Airbyte UI (Recommended for first test)

1. [ ] Open Airbyte UI: http://localhost:8000
2. [ ] Navigate to **Connections** tab
3. [ ] Select a connection (e.g., "Weather API → MinIO Bronze")
4. [ ] Click **"Sync Now"** button
5. [ ] Monitor sync progress in the UI
6. [ ] Wait for status: **"Succeeded"** (green checkmark)
7. [ ] Note the sync duration and records synced

**Expected Result**: Sync completes with status "Succeeded"

#### Option B: Via Airbyte API

```bash
# Get connection ID from UI or API
CONNECTION_ID="your-connection-id-here"

# Trigger sync
curl -X POST http://localhost:8001/api/v1/connections/${CONNECTION_ID}/sync \
  -H "Content-Type: application/json" \
  -d '{}'

# Check status (repeat until succeeded)
JOB_ID="returned-job-id"
curl http://localhost:8001/api/v1/jobs/${JOB_ID}
```

**Expected Result**: API returns job ID and status eventually shows "succeeded"

---

### Part 2: Verify Data in MinIO ✓

**Requirement 4.2, 4.4**: Data lands in bronze/ bucket in Parquet format

1. [ ] Open MinIO Console: http://localhost:9001
2. [ ] Login with credentials from `.env`:
   - Username: `MINIO_ROOT_USER` (default: admin)
   - Password: `MINIO_ROOT_PASSWORD`
3. [ ] Navigate to **bronze** bucket
4. [ ] Browse to source folder (e.g., `weather_api/weather_data/`)
5. [ ] Verify files exist with recent timestamps
6. [ ] Check file extension is `.parquet`
7. [ ] Verify file sizes are non-zero (> 0 bytes)
8. [ ] Note the file path pattern: `bronze/{source_name}/{stream_name}/{timestamp}/`

**Expected Result**: 
- ✅ Parquet files exist in bronze/ bucket
- ✅ Files have recent timestamps (within last hour)
- ✅ File sizes are reasonable (not 0 bytes)
- ✅ Path follows pattern: `bronze/{source}/{stream}/{timestamp}/`

**Screenshot**: Take a screenshot of MinIO showing the files

---

### Part 3: Query Bronze Iceberg Table ✓

**Requirement 5.1, 5.2**: Bronze tables contain raw data with metadata columns

#### Option A: Using Python Script

```bash
# Install dependencies
pip install duckdb boto3 requests

# Set connection IDs (get from Airbyte UI)
export WEATHER_CONNECTION_ID="your-id"
export POSTGRES_CONNECTION_ID="your-id"
export FILE_CONNECTION_ID="your-id"

# Run test script
python scripts/test_bronze_sync.py
```

**Expected Output**: Script shows ✅ for sync, MinIO, and table validation

#### Option B: Using DuckDB CLI

```bash
# Enter dbt container
docker exec -it dbt bash

# Start DuckDB
duckdb :memory:

# Run these commands in DuckDB:
```

```sql
-- Install extensions
INSTALL iceberg;
INSTALL httpfs;
LOAD iceberg;
LOAD httpfs;

-- Configure S3 (replace with your credentials)
SET s3_endpoint='http://minio:9000';
SET s3_access_key_id='admin';
SET s3_secret_access_key='changeme123';
SET s3_use_ssl=false;
SET s3_url_style='path';

-- Create Iceberg catalog connection
CREATE SECRET iceberg_rest (
    TYPE ICEBERG_REST,
    CATALOG_URL 'http://iceberg-rest:8181',
    S3_ENDPOINT 'http://minio:9000',
    S3_ACCESS_KEY_ID 'admin',
    S3_SECRET_ACCESS_KEY 'changeme123',
    S3_USE_SSL false
);

-- Query Bronze table
SELECT COUNT(*) as total_rows 
FROM iceberg_rest.bronze.weather_data;

-- Check table schema
DESCRIBE iceberg_rest.bronze.weather_data;

-- Sample records
SELECT * FROM iceberg_rest.bronze.weather_data LIMIT 5;
```

**Expected Results**:
- [ ] Query returns row count > 0
- [ ] DESCRIBE shows all columns including metadata columns
- [ ] Sample records display correctly

---

### Part 4: Validate Metadata Columns ✓

**Requirement 5.2, 5.3**: Metadata columns are populated correctly

Run this validation query:

```sql
SELECT 
    COUNT(*) as total_rows,
    COUNT(_source_name) as source_name_populated,
    COUNT(_ingested_at) as ingested_at_populated,
    COUNT(_file_path) as file_path_populated,
    COUNT(DISTINCT _source_name) as unique_sources,
    MIN(_ingested_at) as earliest_ingestion,
    MAX(_ingested_at) as latest_ingestion,
    SUM(CASE WHEN _has_null_key THEN 1 ELSE 0 END) as null_key_count
FROM iceberg_rest.bronze.weather_data;
```

**Validation Checklist**:

- [ ] `source_name_populated` = `total_rows` (100% populated)
- [ ] `ingested_at_populated` = `total_rows` (100% populated)
- [ ] `file_path_populated` = `total_rows` (100% populated)
- [ ] `unique_sources` = 1 (single source per table)
- [ ] `earliest_ingestion` and `latest_ingestion` are recent timestamps
- [ ] `null_key_count` is 0 or small number

**Sample Metadata Inspection**:

```sql
SELECT 
    _source_name,
    _ingested_at,
    _file_path,
    _has_null_key
FROM iceberg_rest.bronze.weather_data 
LIMIT 10;
```

**Expected Results**:
- [ ] `_source_name` shows correct source (e.g., "weather_api")
- [ ] `_ingested_at` shows recent timestamp
- [ ] `_file_path` shows S3 path to source file
- [ ] `_has_null_key` is false for most records

---

### Part 5: Verify Partitioning ✓

**Requirement 5.3**: Bronze tables are partitioned by _ingested_at date

Query Iceberg metadata to check partitioning:

```sql
-- Check table metadata
SELECT * FROM iceberg_rest.bronze.weather_data.metadata$snapshots;

-- Check partition spec
SELECT * FROM iceberg_rest.bronze.weather_data.metadata$partition_spec;
```

**Expected Results**:
- [ ] Partition spec shows partitioning by `_ingested_at` (date or day transform)
- [ ] Multiple snapshots exist if multiple syncs were run

---

### Part 6: Validate Data Fidelity ✓

**Requirement 5.1**: Bronze layer preserves raw source data exactly as received

Compare a sample record from Bronze with the source:

1. [ ] Query Bronze table for a specific record
2. [ ] Query the source (API, database, or file) for the same record
3. [ ] Verify all fields match exactly (no transformations applied)
4. [ ] Verify data types are preserved (strings remain strings, etc.)

**Example Comparison**:

```sql
-- Bronze record
SELECT location, date, temperature, humidity 
FROM iceberg_rest.bronze.weather_data 
WHERE location = 'New York' 
LIMIT 1;

-- Compare with source (PostgreSQL example)
-- Run in source database
SELECT location, date, temperature, humidity 
FROM weather_data 
WHERE location = 'New York' 
LIMIT 1;
```

**Expected Result**: Fields match exactly between source and Bronze

---

## Test Results Summary

### Test 1: Weather API Source

- [ ] Sync Status: ✅ Succeeded / ❌ Failed
- [ ] MinIO Files: ✅ Verified / ❌ Not Found
- [ ] Table Query: ✅ Success / ❌ Failed
- [ ] Metadata: ✅ 100% Populated / ❌ Issues
- [ ] Row Count: _______ rows
- [ ] Notes: _________________________________

### Test 2: PostgreSQL Source

- [ ] Sync Status: ✅ Succeeded / ❌ Failed
- [ ] MinIO Files: ✅ Verified / ❌ Not Found
- [ ] Table Query: ✅ Success / ❌ Failed
- [ ] Metadata: ✅ 100% Populated / ❌ Issues
- [ ] Row Count: _______ rows
- [ ] Notes: _________________________________

### Test 3: CSV File Source

- [ ] Sync Status: ✅ Succeeded / ❌ Failed
- [ ] MinIO Files: ✅ Verified / ❌ Not Found
- [ ] Table Query: ✅ Success / ❌ Failed
- [ ] Metadata: ✅ 100% Populated / ❌ Issues
- [ ] Row Count: _______ rows
- [ ] Notes: _________________________________

---

## Troubleshooting

### Issue: Airbyte sync fails

**Symptoms**: Sync status shows "Failed" in UI

**Solutions**:
1. Check Airbyte worker logs: `docker logs airbyte-worker`
2. Verify source is reachable (test API endpoint, database connection)
3. Check Airbyte UI for specific error messages
4. Verify MinIO credentials in Airbyte destination configuration
5. Ensure MinIO service is healthy: `docker compose ps minio`

### Issue: No files in MinIO bucket

**Symptoms**: Bronze bucket is empty or missing expected folders

**Solutions**:
1. Verify sync job actually completed (check Airbyte UI)
2. Check bucket name in Airbyte destination config matches "bronze"
3. Verify MinIO credentials are correct in Airbyte config
4. Check Airbyte worker logs for S3 write errors
5. Manually test MinIO access: `docker exec minio mc ls local/bronze/`

### Issue: Cannot query Iceberg table

**Symptoms**: DuckDB query fails with "table not found" or connection error

**Solutions**:
1. Verify Iceberg REST Catalog is running: `curl http://localhost:8181/v1/config`
2. Check Bronze table was created in Task 3.7: `curl http://localhost:8181/v1/namespaces/bronze/tables`
3. Verify DuckDB extensions are loaded (INSTALL iceberg, LOAD iceberg)
4. Check S3 credentials in DuckDB connection match MinIO credentials
5. Verify network connectivity between containers

### Issue: Metadata columns are NULL

**Symptoms**: Validation query shows < 100% population for metadata columns

**Solutions**:
1. Verify Bronze table schema includes metadata columns (check Task 3.7)
2. Check if Airbyte destination is configured to add metadata
3. Re-create Bronze table with correct schema
4. Re-run Airbyte sync job
5. Check Airbyte version supports metadata columns

### Issue: Wrong data in Bronze table

**Symptoms**: Data doesn't match source or looks transformed

**Solutions**:
1. Verify no transformations are applied in Airbyte connection settings
2. Check "Normalization" is disabled in Airbyte destination
3. Verify source connector is reading correct data
4. Check for any dbt models accidentally running on Bronze layer

---

## Success Criteria

Task 3.8 is **COMPLETE** when ALL of the following are true:

- ✅ At least one Airbyte sync completes with status "Succeeded"
- ✅ Parquet files exist in MinIO bronze/ bucket with recent timestamps
- ✅ Bronze Iceberg table can be queried successfully
- ✅ Table contains expected number of rows (> 0)
- ✅ All metadata columns are 100% populated:
  - `_source_name`
  - `_ingested_at`
  - `_file_path`
- ✅ Table is partitioned by `_ingested_at` date
- ✅ Raw data matches source (spot check confirms fidelity)
- ✅ Documentation updated with any learnings or issues

---

## Sign-Off

**Tester**: _________________________  
**Date**: _________________________  
**Status**: ⬜ Pass / ⬜ Fail / ⬜ Blocked  

**Notes**:
_________________________________________________________________
_________________________________________________________________
_________________________________________________________________

---

## Next Steps

After Task 3.8 is complete:

1. **Checkpoint 2**: Infrastructure Validation
   - Review all Phase 1-3 tasks
   - Confirm end-to-end data flow works
   - Document any issues or improvements

2. **Phase 4**: Silver Layer Transformations
   - Task 4.1: Create dbt project structure
   - Task 4.2: Configure DuckDB connection
   - Task 4.3: Define Bronze sources in dbt

3. **Update Documentation**
   - Add screenshots to docs/
   - Document any deviations from design
   - Note performance observations

---

## References

- **Task File**: `.kiro/specs/batch-elt-medallion-pipeline/tasks.md`
- **Requirements**: Section 4 (Airbyte), Section 5 (Bronze Layer)
- **Design Document**: Bronze Layer Schema section
- **Testing Guide**: `docs/testing-bronze-layer.md`
- **Test Script**: `scripts/test_bronze_sync.py`
