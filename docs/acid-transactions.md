# ACID Transaction Guarantees with Apache Iceberg

> **Requirement 3.5** — The Iceberg table SHALL enforce ACID semantics: WHEN two
> concurrent writes target the same table, THE Catalog SHALL serialize them and
> reject the conflicting write with an optimistic concurrency error.

---

## Overview

Apache Iceberg provides full ACID guarantees through a combination of immutable
data files and atomic metadata commits.  This runbook explains each property,
shows how Iceberg implements it, and maps it to the DuckDB-based validation
tests in `dbt/test_acid_transactions.py`.

---

## ACID Properties in Apache Iceberg

### Atomicity

A write either fully commits or has no effect at all.

**How Iceberg implements it:**
Every write job produces new data files in object storage (MinIO) and then
attempts a single atomic metadata swap — replacing the current snapshot pointer
with a new one.  If the metadata swap fails for any reason (network error,
constraint violation, OCC conflict), the new data files are simply abandoned;
the table pointer never moves, so readers never see partial data.

**DuckDB analogy (Test 1):**
```sql
BEGIN;
INSERT INTO orders VALUES (3, 300.0);   -- valid
INSERT INTO orders VALUES (4, -50.0);   -- violates CHECK (amount > 0)
COMMIT;                                 -- whole transaction rolls back
```
After the rollback, `order_id = 3` is not visible — the partial insert never
happened.

---

### Consistency

Constraints and schema rules are enforced at commit time, even under concurrent
load.

**How Iceberg implements it:**
The Iceberg catalog validates the new snapshot against the current table schema
before accepting the commit.  Writers that produce files with incompatible
schemas or that violate partition specs are rejected.

**DuckDB analogy (Test 5):**
Five concurrent writers attempt inserts; two carry `amount <= 0` which violates
the `CHECK (amount > 0)` constraint.  Both are rejected — only the three valid
rows land in the table.

---

### Isolation — Snapshot Isolation

Readers always see a consistent, point-in-time snapshot of the table.

**How Iceberg implements it:**
When a query starts, the catalog resolves the current snapshot ID.  The query
reads only the data files referenced by that snapshot.  Even if a new snapshot
is committed mid-query, the running query continues to read the original set of
files — it is completely isolated from the concurrent write.

This is called **snapshot isolation** and is the default read consistency level
in Iceberg.

**DuckDB analogy (Test 3):**

```
Timeline:
  t=0  Reader opens transaction, reads total_orders = 100  (snapshot N)
  t=1  Writer commits total_orders = 999                   (snapshot N+1)
  t=2  Reader (still in same transaction) sees 100         (still on snapshot N)
  t=3  Reader commits; new query sees 999                  (snapshot N+1)
```

The reader's open transaction is pinned to snapshot N.  Only after it closes
and a new query begins does snapshot N+1 become visible.

---

### Durability

Once a write is committed, the data is permanent and survives process restarts,
connection drops, and subsequent operations.

**How Iceberg implements it:**
Data files are written to durable object storage (MinIO / S3) before the
metadata commit.  The metadata JSON files are also written to object storage.
Even if the catalog process restarts, the data files and metadata are intact and
the table can be read from any new connection.

**DuckDB analogy (Test 4):**
A row committed in one connection is visible in a brand-new connection opened
after the first one closes — the data survived the connection lifecycle.

---

### Optimistic Concurrency Control (OCC)

Iceberg uses OCC rather than pessimistic locking.  Writers do not hold locks
while writing data files; they only lock briefly during the metadata commit.

**How it works:**

```
Writer A                          Writer B
  │                                  │
  ├─ Read current snapshot (S0)      ├─ Read current snapshot (S0)
  ├─ Write data files to MinIO       ├─ Write data files to MinIO
  ├─ Attempt metadata commit         │
  │   (S0 → S1) ✅ SUCCESS           ├─ Attempt metadata commit
  │                                  │   (S0 → S1) ❌ CONFLICT
  │                                  │   (S1 already exists)
  │                                  ├─ Retry: read S1, re-validate
  │                                  ├─ Attempt metadata commit
  │                                  │   (S1 → S2) ✅ SUCCESS
```

The loser detects that the base snapshot changed, re-reads the new state, and
retries.  If the conflict cannot be resolved (e.g., both writers modified the
same partition key), the catalog rejects the second commit with an
`OptimisticConcurrencyException`.

**DuckDB analogy (Test 2):**
Two threads race to insert a row with `run_id = 1` (PRIMARY KEY).  The first
thread to commit wins; the second receives a constraint violation and is
rejected.  The table contains exactly one row for `run_id = 1`.

---

## Running the Validation Tests

### Prerequisites

```bash
pip install duckdb
```

### Run all ACID checks

```bash
python dbt/test_acid_transactions.py
```

### Expected output

```
======================================================================
  ACID TRANSACTION GUARANTEES — VALIDATION
  Requirements: 3.5
======================================================================

======================================================================
  TEST 1: Atomicity — failed write leaves table unchanged
======================================================================
  ✅ Row count unchanged after failed transaction — before=2, after=2
  ✅ Partial insert (order_id=3) not visible after rollback

======================================================================
  TEST 2: Optimistic Concurrency Control — concurrent writes serialised
======================================================================
  ✅ Exactly one writer committed — committed=['writer_a']
  ✅ Exactly one writer was rejected (conflict) — failed=['writer_b']
  ✅ Table contains exactly one row for the contested run_id — rows=1
  ✅ Winning writer's data is durable (written_by='writer_a')

======================================================================
  TEST 3: Snapshot Isolation — reader sees pre-write snapshot
======================================================================
  ✅ Reader captured pre-write value (100) before writer committed
  ✅ Writer's committed value (999) is visible to subsequent reader
  ✅ Pre-write and post-write values differ (isolation boundary confirmed)

======================================================================
  TEST 4: Durability — committed data survives connection close
======================================================================
  ✅ Committed row visible after connection close and re-open

======================================================================
  TEST 5: Consistency — constraints enforced under concurrent load
======================================================================
  ✅ Only valid rows committed (3 valid out of 5 attempts)
  ✅ Invalid rows were all rejected (2 rejections expected)
  ✅ No row with amount <= 0 was ever committed

======================================================================
  SUMMARY
======================================================================
  Passed : 13/13
  Failed : 0/13

  ✅ ALL ACID CHECKS PASSED
```

---

## Iceberg OCC in Production (REST Catalog)

When the full Iceberg REST catalog is available (requires DuckDB ≥ 1.0 with
catalog secret support), concurrent write conflicts surface as explicit catalog
errors.

### Listing snapshots

```sql
-- Connect to the Iceberg REST catalog
CREATE SECRET iceberg_rest (
    TYPE ICEBERG_REST,
    CATALOG_URL 'http://iceberg-rest:8181',
    S3_ENDPOINT 'minio:9000',
    S3_ACCESS_KEY_ID 'admin',
    S3_SECRET_ACCESS_KEY 'changeme123',
    S3_USE_SSL false
);

-- List all snapshots for the gold.order_metrics table
SELECT snapshot_id, committed_at, operation
FROM iceberg_rest.gold.order_metrics.snapshots
ORDER BY committed_at DESC;
```

### Simulating an OCC conflict

```python
import pyiceberg.catalog

catalog = load_catalog("rest", uri="http://iceberg-rest:8181")
table   = catalog.load_table("gold.order_metrics")

# Writer A: read base snapshot, write files, commit
with table.transaction() as txn:
    txn.append(new_data_a)   # commits snapshot S1

# Writer B: started from the same base snapshot as A
# When it tries to commit, the catalog detects S0 → S1 already happened
with table.transaction() as txn:
    txn.append(new_data_b)   # raises CommitFailedException (OCC conflict)
```

The `CommitFailedException` tells Writer B to re-read the current snapshot and
retry.  If the conflict is irreconcilable (same partition key), the write is
permanently rejected.

---

## Mapping: DuckDB Tests → Iceberg Production Behaviour

| Test | DuckDB mechanism | Iceberg equivalent |
|------|------------------|--------------------|
| Atomicity (T1) | `ROLLBACK` on constraint violation | Failed metadata commit; data files abandoned |
| OCC (T2) | PRIMARY KEY conflict between threads | `CommitFailedException` from REST catalog |
| Snapshot Isolation (T3) | Transaction pinned to snapshot at `BEGIN` | Query pinned to snapshot ID at query start |
| Durability (T4) | Data visible after connection close | Data files in MinIO survive catalog restart |
| Consistency (T5) | `CHECK` constraint enforced per writer | Schema validation at catalog commit time |

---

## Troubleshooting

### OCC conflicts in production

If you see frequent `CommitFailedException` errors:
- Reduce write parallelism (fewer concurrent dbt runs)
- Partition tables to reduce write contention (different partitions never conflict)
- Enable Iceberg's merge-on-read mode to allow concurrent appends

### Snapshot accumulation

Each write creates a new snapshot.  Clean up old snapshots periodically:

```sql
-- Expire snapshots older than 7 days (PyIceberg)
table.expire_snapshots().expire_older_than(
    datetime.now() - timedelta(days=7)
).commit()
```

### Read performance after many snapshots

If queries slow down due to many small files across snapshots, run a compaction:

```sql
-- Compact small files (PyIceberg)
table.rewrite_data_files().execute()
```

---

## References

- [Apache Iceberg — Reliability](https://iceberg.apache.org/docs/latest/reliability/)
- [Apache Iceberg — Transactions](https://iceberg.apache.org/docs/latest/transactions/)
- [DuckDB Transaction Semantics](https://duckdb.org/docs/sql/statements/transactions)
- Requirement 3.5 in `.kiro/specs/batch-elt-medallion-pipeline/requirements.md`
- Validation script: `dbt/test_acid_transactions.py`
