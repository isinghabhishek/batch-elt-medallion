# Task 3.8 Quick Reference Card

## 🎯 Goal
Test Airbyte sync to Bronze layer and validate data ingestion.

## ⚡ Quick Start (5 minutes)

### 1. Trigger Sync
```bash
# Open Airbyte UI
http://localhost:8000

# Click: Connections → Select Connection → Sync Now
# Wait for: Status = "Succeeded" ✅
```

### 2. Check MinIO
```bash
# Open MinIO Console
http://localhost:9001
# Login: admin / changeme123

# Navigate: bronze bucket → weather_api/ → verify .parquet files
```

### 3. Query Table
```bash
# Enter dbt container
docker exec -it dbt duckdb :memory:
```

```sql
-- Quick setup
INSTALL iceberg; INSTALL httpfs; LOAD iceberg; LOAD httpfs;
SET s3_endpoint='http://minio:9000';
SET s3_access_key_id='admin';
SET s3_secret_access_key='changeme123';
SET s3_use_ssl=false; SET s3_url_style='path';

CREATE SECRET iceberg_rest (
    TYPE ICEBERG_REST,
    CATALOG_URL 'http://iceberg-rest:8181',
    S3_ENDPOINT 'http://minio:9000',
    S3_ACCESS_KEY_ID 'admin',
    S3_SECRET_ACCESS_KEY 'changeme123',
    S3_USE_SSL false
);

-- Validate
SELECT COUNT(*) FROM iceberg_rest.bronze.weather_data;
SELECT * FROM iceberg_rest.bronze.weather_data LIMIT 3;
```

## ✅ Success Checklist

- [ ] Sync status = "Succeeded"
- [ ] Files in MinIO bronze/ bucket
- [ ] Table query returns rows
- [ ] Metadata columns populated

## 🔍 Validation Query

```sql
SELECT 
    COUNT(*) as rows,
    COUNT(_source_name) as has_source,
    COUNT(_ingested_at) as has_timestamp,
    COUNT(_file_path) as has_path
FROM iceberg_rest.bronze.weather_data;
```

**Expected**: All counts equal `rows`

## 🚨 Common Issues

| Issue | Solution |
|-------|----------|
| Sync fails | Check `docker logs airbyte-worker` |
| No MinIO files | Verify destination config in Airbyte |
| Table not found | Run Task 3.7 first (create tables) |
| Query fails | Check Iceberg catalog: `curl http://localhost:8181/v1/config` |

## 📊 Test Data Sources

1. **Weather API**: Public weather data
2. **PostgreSQL**: Seed database (orders, customers)
3. **CSV Files**: Sample product data

## 🔗 Quick Links

- Airbyte UI: http://localhost:8000
- MinIO Console: http://localhost:9001
- Iceberg Catalog: http://localhost:8181/v1/config
- Full Guide: `docs/testing-bronze-layer.md`
- Checklist: `docs/task-3.8-checklist.md`

## 📝 Requirements Validated

- ✅ 4.2: Airbyte writes to bronze/ bucket
- ✅ 4.3: Sync completes successfully
- ✅ 4.4: Parquet format
- ✅ 5.1: Raw data preserved
- ✅ 5.2: Metadata columns added
- ✅ 5.3: Partitioned by date

## 🎬 Next Steps

After completion:
1. Mark task 3.8 as complete
2. Proceed to Checkpoint 2
3. Begin Phase 4 (Silver Layer)
