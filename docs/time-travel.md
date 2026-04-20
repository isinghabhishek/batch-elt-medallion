# Time Travel with Apache Iceberg

## Overview

This runbook demonstrates Apache Iceberg's time travel capabilities, which allow you to query historical versions of your data. Every write operation to an Iceberg table creates an immutable snapshot, enabling you to view data as it existed at any point in time.

## What is Time Travel?

Time travel enables you to:
- Query data as it existed at a specific timestamp
- Query data at a specific snapshot ID
- Compare data across different versions
- Recover from accidental data corruption or deletion
- Audit data changes over time
- Debug data quality issues by examining historical states

## Prerequisites

- Docker Compose stack running (`docker compose up -d`)
- Gold layer tables populated with multiple snapshots
- DuckDB CLI or dbt runner container access

## Key Concepts

### Snapshots

Every write operation (INSERT, UPDATE, DELETE, MERGE) creates a new snapshot:
- **Snapshot ID**: Unique identifier for each version
- **Timestamp**: When the snapshot was committed
- **Parent ID**: Previous snapshot (forms a version chain)
- **Operation**: Type of change (append, overwrite, delete)
- **Summary**: Metadata about the change (rows added, files added, etc.)

### Snapshot Retention

By default, Iceberg retains all snapshots indefinitely. In production:
- Configure snapshot retention policies
- Run `expire_snapshots` to remove old versions
- Balance storage costs vs. time travel needs

## Demonstration: Time Travel on Gold Layer

### Step 1: Access DuckDB with Iceberg Support

```bash
# Access the dbt container
docker exec -it dbt bash

# Start DuckDB
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
```

### Step 2: List All Snapshots

View the complete snapshot history for a Gold table:

```sql
-- List all snapshots for order_metrics table
SELECT 
    snapshot_id,
    parent_id,
    committed_at,
    operation,
    summary
FROM iceberg_rest.gold.order_metrics.snapshots
ORDER BY committed_at DESC;
```

**Expected Output:**
```
snapshot_id          | parent_id           | committed_at              | operation | summary
---------------------|---------------------|---------------------------|-----------|------------------
8744736658442645458  | 3055729675574597004 | 2026-04-20 15:30:22.123  | append    | {"added-records":"50",...}
3055729675574597004  | NULL                | 2026-04-20 10:15:10.456  | append    | {"added-records":"100",...}
```

**Interpretation:**
- **Latest snapshot**: ID `8744736658442645458` (most recent data)
- **Previous snapshot**: ID `3055729675574597004` (parent of latest)
- **Operation**: Both are `append` operations (incremental inserts)
- **Summary**: Shows how many records were added

### Step 3: Query Current Data (Latest Snapshot)

First, let's see the current state of the data:

```sql
-- Query current data
SELECT 
    date,
    total_orders,
    total_revenue,
    avg_order_value,
    unique_customers
FROM iceberg_scan('iceberg-rest.gold.order_metrics')
ORDER BY date DESC
LIMIT 10;
```

**Example Output:**
```
date        | total_orders | total_revenue | avg_order_value | unique_customers
------------|--------------|---------------|-----------------|------------------
2026-04-20  | 45           | 2250.00       | 50.00           | 32
2026-04-19  | 38           | 1900.00       | 50.00           | 28
2026-04-18  | 42           | 2100.00       | 50.00           | 30
...
```

### Step 4: Time Travel by Snapshot ID

Query data as it existed at a specific snapshot:

```sql
-- Query data at previous snapshot
SELECT 
    date,
    total_orders,
    total_revenue,
    avg_order_value,
    unique_customers
FROM iceberg_scan('iceberg-rest.gold.order_metrics', version => 3055729675574597004)
ORDER BY date DESC
LIMIT 10;
```

**Syntax Variations:**
```sql
-- DuckDB syntax with version parameter
FROM iceberg_scan('iceberg-rest.gold.order_metrics', version => <snapshot_id>)

-- Standard SQL syntax (if supported)
FROM iceberg_rest.gold.order_metrics FOR SYSTEM_VERSION AS OF <snapshot_id>

-- Spark SQL syntax (for reference)
FROM iceberg_rest.gold.order_metrics VERSION AS OF <snapshot_id>
```

**Expected Result:**
- Data reflects the state at that snapshot
- More recent changes are not visible
- Useful for comparing "before" and "after" states

### Step 5: Time Travel by Timestamp

Query data as it existed at a specific point in time:

```sql
-- Query data as of a specific timestamp
SELECT 
    date,
    total_orders,
    total_revenue
FROM iceberg_scan('iceberg-rest.gold.order_metrics', 
                  timestamp => TIMESTAMP '2026-04-20 10:00:00')
ORDER BY date DESC
LIMIT 10;
```

**Syntax Variations:**
```sql
-- DuckDB syntax with timestamp parameter
FROM iceberg_scan('iceberg-rest.gold.order_metrics', 
                  timestamp => TIMESTAMP '2026-04-20 10:00:00')

-- Standard SQL syntax (if supported)
FROM iceberg_rest.gold.order_metrics FOR SYSTEM_TIME AS OF TIMESTAMP '2026-04-20 10:00:00'

-- Spark SQL syntax (for reference)
FROM iceberg_rest.gold.order_metrics TIMESTAMP AS OF '2026-04-20 10:00:00'
```

**How It Works:**
- Iceberg finds the snapshot committed closest to (but not after) the specified timestamp
- Returns data from that snapshot
- If no snapshot exists before the timestamp, returns an error

### Step 6: Compare Data Across Snapshots

Compare current data with historical data to see what changed:

```sql
-- Compare current vs. previous snapshot
WITH current_data AS (
    SELECT 
        date,
        total_orders as current_orders,
        total_revenue as current_revenue
    FROM iceberg_scan('iceberg-rest.gold.order_metrics')
),
historical_data AS (
    SELECT 
        date,
        total_orders as historical_orders,
        total_revenue as historical_revenue
    FROM iceberg_scan('iceberg-rest.gold.order_metrics', version => 3055729675574597004)
)
SELECT 
    COALESCE(c.date, h.date) as date,
    h.historical_orders,
    c.current_orders,
    c.current_orders - COALESCE(h.historical_orders, 0) as orders_added,
    h.historical_revenue,
    c.current_revenue,
    c.current_revenue - COALESCE(h.historical_revenue, 0) as revenue_added
FROM current_data c
FULL OUTER JOIN historical_data h ON c.date = h.date
ORDER BY date DESC
LIMIT 10;
```

**Expected Output:**
```
date        | historical_orders | current_orders | orders_added | historical_revenue | current_revenue | revenue_added
------------|-------------------|----------------|--------------|--------------------|-----------------|--------------
2026-04-20  | NULL              | 45             | 45           | NULL               | 2250.00         | 2250.00
2026-04-19  | 38                | 38             | 0            | 1900.00            | 1900.00         | 0.00
2026-04-18  | 42                | 42             | 0            | 2100.00            | 2100.00         | 0.00
```

**Interpretation:**
- New dates appear in current snapshot (orders_added > 0)
- Unchanged dates show 0 difference
- Useful for auditing incremental processing

### Step 7: List Snapshot Metadata

Get detailed metadata about each snapshot:

```sql
-- Detailed snapshot information
SELECT 
    snapshot_id,
    committed_at,
    operation,
    summary['added-records'] as records_added,
    summary['added-data-files'] as files_added,
    summary['total-records'] as total_records,
    summary['total-data-files'] as total_files
FROM iceberg_rest.gold.order_metrics.snapshots
ORDER BY committed_at DESC;
```

**Expected Output:**
```
snapshot_id          | committed_at              | operation | records_added | files_added | total_records | total_files
---------------------|---------------------------|-----------|---------------|-------------|---------------|-------------
8744736658442645458  | 2026-04-20 15:30:22.123  | append    | 50            | 1           | 150           | 3
3055729675574597004  | 2026-04-20 10:15:10.456  | append    | 100           | 2           | 100           | 2
```

### Step 8: Query Snapshot Lineage

Trace the parent-child relationship between snapshots:

```sql
-- Build snapshot lineage chain
WITH RECURSIVE snapshot_chain AS (
    -- Start with the latest snapshot
    SELECT 
        snapshot_id,
        parent_id,
        committed_at,
        operation,
        1 as depth
    FROM iceberg_rest.gold.order_metrics.snapshots
    WHERE parent_id IS NULL  -- Root snapshot
    
    UNION ALL
    
    -- Recursively find children
    SELECT 
        s.snapshot_id,
        s.parent_id,
        s.committed_at,
        s.operation,
        sc.depth + 1
    FROM iceberg_rest.gold.order_metrics.snapshots s
    INNER JOIN snapshot_chain sc ON s.parent_id = sc.snapshot_id
)
SELECT 
    depth,
    snapshot_id,
    parent_id,
    committed_at,
    operation
FROM snapshot_chain
ORDER BY depth;
```

## Advanced Time Travel Patterns

### Pattern 1: Audit Trail Query

Find all changes to a specific record over time:

```sql
-- Track changes to a specific date's metrics
SELECT 
    s.snapshot_id,
    s.committed_at,
    m.date,
    m.total_orders,
    m.total_revenue
FROM iceberg_rest.gold.order_metrics.snapshots s
CROSS JOIN LATERAL (
    SELECT * 
    FROM iceberg_scan('iceberg-rest.gold.order_metrics', version => s.snapshot_id)
    WHERE date = DATE '2026-04-19'
) m
ORDER BY s.committed_at;
```

### Pattern 2: Data Recovery

Recover accidentally deleted or corrupted data:

```sql
-- Find data that existed in a previous snapshot but not in current
WITH previous_data AS (
    SELECT * 
    FROM iceberg_scan('iceberg-rest.gold.order_metrics', version => 3055729675574597004)
),
current_data AS (
    SELECT * 
    FROM iceberg_scan('iceberg-rest.gold.order_metrics')
)
SELECT p.*
FROM previous_data p
LEFT JOIN current_data c ON p.date = c.date
WHERE c.date IS NULL;  -- Records that disappeared
```

### Pattern 3: Incremental Change Detection

Identify exactly what changed between two snapshots:

```sql
-- Find new, modified, and deleted records
WITH snapshot_a AS (
    SELECT *, 'A' as source
    FROM iceberg_scan('iceberg-rest.gold.order_metrics', version => 3055729675574597004)
),
snapshot_b AS (
    SELECT *, 'B' as source
    FROM iceberg_scan('iceberg-rest.gold.order_metrics', version => 8744736658442645458)
)
SELECT 
    COALESCE(a.date, b.date) as date,
    CASE 
        WHEN a.date IS NULL THEN 'INSERTED'
        WHEN b.date IS NULL THEN 'DELETED'
        WHEN a.total_orders != b.total_orders THEN 'MODIFIED'
        ELSE 'UNCHANGED'
    END as change_type,
    a.total_orders as old_orders,
    b.total_orders as new_orders
FROM snapshot_a a
FULL OUTER JOIN snapshot_b b ON a.date = b.date
WHERE a.date IS NULL OR b.date IS NULL OR a.total_orders != b.total_orders
ORDER BY date;
```

## Error Handling

### Error: Invalid Snapshot ID

```sql
SELECT * FROM iceberg_scan('iceberg-rest.gold.order_metrics', version => 9999999999999999999);
-- Error: Snapshot 9999999999999999999 not found
```

**Solution**: List valid snapshots first:
```sql
SELECT snapshot_id FROM iceberg_rest.gold.order_metrics.snapshots;
```

### Error: Timestamp Before First Snapshot

```sql
SELECT * FROM iceberg_scan('iceberg-rest.gold.order_metrics', 
                          timestamp => TIMESTAMP '2020-01-01 00:00:00');
-- Error: No snapshot exists before the specified timestamp
```

**Solution**: Check the earliest snapshot timestamp:
```sql
SELECT MIN(committed_at) FROM iceberg_rest.gold.order_metrics.snapshots;
```

### Error: Expired Snapshot

If snapshot retention policies have removed old snapshots:
```sql
SELECT * FROM iceberg_scan('iceberg-rest.gold.order_metrics', version => 1234567890);
-- Error: Snapshot 1234567890 has been expired
```

**Solution**: Query available snapshots and use a more recent one.

## Snapshot Management

### Expire Old Snapshots

In production, configure snapshot retention to manage storage:

```sql
-- Expire snapshots older than 7 days (Spark SQL example)
CALL iceberg_rest.system.expire_snapshots(
    table => 'gold.order_metrics',
    older_than => TIMESTAMP '2026-04-13 00:00:00',
    retain_last => 5  -- Always keep at least 5 snapshots
);
```

### Rollback to Previous Snapshot

Revert a table to a previous state:

```sql
-- Rollback to a specific snapshot (Spark SQL example)
CALL iceberg_rest.system.rollback_to_snapshot(
    table => 'gold.order_metrics',
    snapshot_id => 3055729675574597004
);
```

**Warning**: This creates a new snapshot that restores the old state. It doesn't delete the bad snapshot.

## Use Cases

### 1. Debugging Data Quality Issues

When a data quality issue is discovered:
1. Query current data to identify the problem
2. Use time travel to find when the issue was introduced
3. Compare snapshots to identify the root cause
4. Fix the pipeline and reprocess

### 2. Regulatory Compliance

For audit requirements:
- Maintain complete history of all data changes
- Query data as it existed at any point in time
- Prove data lineage and transformations
- Demonstrate compliance with retention policies

### 3. A/B Testing Analysis

Compare metrics before and after a change:
- Snapshot before deploying new feature
- Snapshot after deployment
- Compare metrics across snapshots
- Rollback if metrics degrade

### 4. Disaster Recovery

Recover from accidental data corruption:
- Identify the last good snapshot
- Query data from that snapshot
- Restore or reprocess from that point
- Investigate what caused the corruption

## Best Practices

1. **Document Snapshots**: Add meaningful commit messages when creating snapshots
2. **Retention Policies**: Configure appropriate snapshot retention for your use case
3. **Performance**: Time travel queries may be slower for very old snapshots
4. **Storage**: Each snapshot adds metadata overhead (but not data duplication)
5. **Testing**: Use time travel to validate incremental processing logic
6. **Monitoring**: Track snapshot growth and storage usage

## Validation Queries

### Check Snapshot Count

```sql
-- Count total snapshots
SELECT COUNT(*) as total_snapshots
FROM iceberg_rest.gold.order_metrics.snapshots;
```

### Check Snapshot Age

```sql
-- Find oldest and newest snapshots
SELECT 
    MIN(committed_at) as oldest_snapshot,
    MAX(committed_at) as newest_snapshot,
    MAX(committed_at) - MIN(committed_at) as retention_period
FROM iceberg_rest.gold.order_metrics.snapshots;
```

### Check Snapshot Size

```sql
-- Estimate storage per snapshot
SELECT 
    snapshot_id,
    committed_at,
    summary['total-data-files'] as data_files,
    summary['total-records'] as records,
    summary['total-data-files']::INTEGER * 1024 * 1024 as estimated_size_bytes
FROM iceberg_rest.gold.order_metrics.snapshots
ORDER BY committed_at DESC;
```

## Comparison: Time Travel vs. Backups

| Feature | Time Travel | Traditional Backups |
|---------|-------------|---------------------|
| Granularity | Per-transaction | Periodic (hourly/daily) |
| Storage Overhead | Minimal (metadata + new data) | Full copy each time |
| Query Performance | Native SQL queries | Restore then query |
| Point-in-Time Recovery | Any snapshot | Backup intervals only |
| Retention Management | Expire old snapshots | Delete old backups |

## Summary

Time travel with Iceberg provides:
- **Audit Trail**: Complete history of all data changes
- **Data Recovery**: Restore from any point in time
- **Debugging**: Compare data across versions to find issues
- **Compliance**: Meet regulatory requirements for data retention
- **Testing**: Validate incremental processing and transformations

This capability is essential for production data pipelines where data quality, auditability, and recoverability are critical.

## Next Steps

1. Run the time travel demonstration script: `python dbt/test_time_travel.py`
2. Experiment with different snapshot IDs and timestamps
3. Compare data across multiple snapshots
4. Configure snapshot retention policies for your use case
5. Integrate time travel queries into your monitoring and alerting

For more information, see:
- [Apache Iceberg Time Travel Documentation](https://iceberg.apache.org/docs/latest/spark-queries/#time-travel)
- [DuckDB Iceberg Extension](https://duckdb.org/docs/extensions/iceberg)
