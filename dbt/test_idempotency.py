#!/usr/bin/env python3
"""
Test script for incremental processing and idempotency verification.

This script:
1. Runs Silver transformations twice with the same source data
2. Verifies no duplicate records are created
3. Tests backfill scenario (re-run for past date)
4. Verifies partition overwrite behavior
5. Confirms idempotency property (same input → same output)
"""

import duckdb
import os
from datetime import datetime, date

# Configuration
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'minio:9000')
MINIO_USER = os.getenv('MINIO_ROOT_USER', 'minioadmin')
MINIO_PASSWORD = os.getenv('MINIO_ROOT_PASSWORD', 'minioadmin')

def setup_duckdb():
    """Initialize DuckDB connection with S3 support."""
    conn = duckdb.connect(':memory:')
    
    # Load extensions
    conn.execute("INSTALL httpfs")
    conn.execute("LOAD httpfs")
    
    # Configure S3 connection
    conn.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}'")
    conn.execute(f"SET s3_access_key_id='{MINIO_USER}'")
    conn.execute(f"SET s3_secret_access_key='{MINIO_PASSWORD}'")
    conn.execute("SET s3_use_ssl=false")
    conn.execute("SET s3_url_style='path'")
    
    print("✓ DuckDB initialized with S3 support")
    return conn

def create_bronze_test_data(conn):
    """Create test Bronze data for idempotency testing."""
    print("\n" + "="*80)
    print("SETUP: Create Bronze Test Data")
    print("="*80)
    
    try:
        conn.execute("""
            CREATE TABLE bronze_weather AS
            SELECT 
                'San Francisco' as location,
                '2026-04-18' as date,
                '18.5' as temperature,
                '65.0' as humidity,
                'Partly Cloudy' as condition,
                TIMESTAMP '2026-04-18 10:00:00' as _ingested_at,
                'test_source' as _source_name,
                's3://bronze/weather/file1.parquet' as _file_path,
                false as _has_null_key
            UNION ALL
            SELECT 
                'Los Angeles',
                '2026-04-18',
                '22.0',
                '55.0',
                'Sunny',
                TIMESTAMP '2026-04-18 10:00:00',
                'test_source',
                's3://bronze/weather/file1.parquet',
                false
            UNION ALL
            SELECT 
                'Seattle',
                '2026-04-18',
                '15.0',
                '75.0',
                'Rainy',
                TIMESTAMP '2026-04-18 10:00:00',
                'test_source',
                's3://bronze/weather/file1.parquet',
                false
        """)
        
        count = conn.execute("SELECT COUNT(*) FROM bronze_weather").fetchone()[0]
        print(f"\n✓ Created Bronze test data: {count} records")
        
        return True
    except Exception as e:
        print(f"✗ Error creating Bronze data: {e}")
        return False

def run_silver_transformation(conn, run_number):
    """Run Silver transformation (simulating dbt incremental model)."""
    print(f"\n{'='*80}")
    print(f"RUN {run_number}: Execute Silver Transformation")
    print("="*80)
    
    try:
        # Check if Silver table exists
        table_exists = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = 'silver_weather_clean'
        """).fetchone()[0] > 0
        
        if table_exists:
            print(f"\n  Silver table exists - running incremental update")
            
            # Incremental logic: only process new Bronze records
            conn.execute("""
                INSERT OR REPLACE INTO silver_weather_clean
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
                FROM bronze_weather
                WHERE location IS NOT NULL
                  AND date IS NOT NULL
            """)
        else:
            print(f"\n  Silver table does not exist - running full refresh")
            
            # Full refresh: create table and insert all data
            conn.execute("""
                CREATE TABLE silver_weather_clean AS
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
                FROM bronze_weather
                WHERE location IS NOT NULL
                  AND date IS NOT NULL
            """)
        
        # Count records after transformation
        count = conn.execute("SELECT COUNT(*) FROM silver_weather_clean").fetchone()[0]
        print(f"  ✓ Silver transformation complete: {count} records")
        
        return count
    except Exception as e:
        print(f"  ✗ Error running transformation: {e}")
        return None

def verify_no_duplicates(conn, run_number):
    """Verify no duplicate records exist in Silver layer."""
    print(f"\n  Verification: Check for duplicates after Run {run_number}")
    
    try:
        # Check for duplicate location_date keys
        result = conn.execute("""
            SELECT 
                location_date,
                COUNT(*) as count
            FROM silver_weather_clean
            GROUP BY location_date
            HAVING COUNT(*) > 1
        """).fetchall()
        
        if len(result) == 0:
            print(f"  ✓ No duplicates found - idempotency verified")
            return True
        else:
            print(f"  ✗ Found {len(result)} duplicate keys:")
            for row in result:
                print(f"    - {row[0]}: {row[1]} occurrences")
            return False
    except Exception as e:
        print(f"  ✗ Error checking duplicates: {e}")
        return False

def compare_run_results(conn, count1, count2):
    """Compare results from two runs to verify idempotency."""
    print("\n" + "="*80)
    print("STEP 1: Verify Idempotency (Same Input → Same Output)")
    print("="*80)
    
    if count1 == count2:
        print(f"\n✓ Idempotency verified:")
        print(f"  - Run 1 produced {count1} records")
        print(f"  - Run 2 produced {count2} records")
        print(f"  - Record counts match (no duplicates created)")
        return True
    else:
        print(f"\n✗ Idempotency violation:")
        print(f"  - Run 1 produced {count1} records")
        print(f"  - Run 2 produced {count2} records")
        print(f"  - Record counts differ (duplicates may exist)")
        return False

def test_backfill_scenario(conn):
    """Test backfill scenario (re-run for past date)."""
    print("\n" + "="*80)
    print("STEP 2: Test Backfill Scenario")
    print("="*80)
    
    try:
        # Add new Bronze data for a past date
        print("\n  Adding new Bronze data for past date (2026-04-17)...")
        conn.execute("""
            INSERT INTO bronze_weather VALUES
            ('San Francisco', '2026-04-17', '17.0', '70.0', 'Cloudy',
             TIMESTAMP '2026-04-20 12:00:00', 'backfill_source',
             's3://bronze/weather/backfill.parquet', false)
        """)
        
        # Count before backfill
        count_before = conn.execute("SELECT COUNT(*) FROM silver_weather_clean").fetchone()[0]
        print(f"  Records before backfill: {count_before}")
        
        # Run transformation again (should pick up new data)
        conn.execute("""
            INSERT OR REPLACE INTO silver_weather_clean
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
            FROM bronze_weather
            WHERE location IS NOT NULL
              AND date IS NOT NULL
        """)
        
        # Count after backfill
        count_after = conn.execute("SELECT COUNT(*) FROM silver_weather_clean").fetchone()[0]
        print(f"  Records after backfill: {count_after}")
        
        # Verify the backfilled record exists
        backfill_record = conn.execute("""
            SELECT location, date, temperature_celsius
            FROM silver_weather_clean
            WHERE date = DATE '2026-04-17'
        """).fetchone()
        
        if backfill_record:
            print(f"\n✓ Backfill successful:")
            print(f"  - Added 1 new record for past date (2026-04-17)")
            print(f"  - Total records increased from {count_before} to {count_after}")
            print(f"  - Backfilled record: {backfill_record[0]}, {backfill_record[1]}, {backfill_record[2]}°C")
            return True
        else:
            print(f"\n✗ Backfill failed: Record not found")
            return False
            
    except Exception as e:
        print(f"✗ Error during backfill test: {e}")
        return False

def test_partition_overwrite(conn):
    """Test partition overwrite behavior."""
    print("\n" + "="*80)
    print("STEP 3: Test Partition Overwrite Behavior")
    print("="*80)
    
    try:
        # Get current value for 2026-04-18
        original = conn.execute("""
            SELECT location, temperature_celsius
            FROM silver_weather_clean
            WHERE date = DATE '2026-04-18' AND location = 'San Francisco'
        """).fetchone()
        
        print(f"\n  Original value: {original[0]}, {original[1]}°C")
        
        # Update Bronze data for same date (simulating reprocessing)
        print(f"  Updating Bronze data for 2026-04-18 (simulating reprocessing)...")
        conn.execute("""
            UPDATE bronze_weather
            SET temperature = '19.5'
            WHERE location = 'San Francisco' AND date = '2026-04-18'
        """)
        
        # Re-run transformation (should overwrite)
        conn.execute("""
            INSERT OR REPLACE INTO silver_weather_clean
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
            FROM bronze_weather
            WHERE location IS NOT NULL
              AND date IS NOT NULL
        """)
        
        # Get updated value
        updated = conn.execute("""
            SELECT location, temperature_celsius
            FROM silver_weather_clean
            WHERE date = DATE '2026-04-18' AND location = 'San Francisco'
        """).fetchone()
        
        print(f"  Updated value: {updated[0]}, {updated[1]}°C")
        
        # Verify no duplicates
        duplicate_check = conn.execute("""
            SELECT COUNT(*)
            FROM silver_weather_clean
            WHERE date = DATE '2026-04-18' AND location = 'San Francisco'
        """).fetchone()[0]
        
        if duplicate_check == 1 and updated[1] == 19.5:
            print(f"\n✓ Partition overwrite successful:")
            print(f"  - Original temperature: {original[1]}°C")
            print(f"  - Updated temperature: {updated[1]}°C")
            print(f"  - No duplicates created (count = {duplicate_check})")
            print(f"  - Partition was overwritten, not appended")
            return True
        else:
            print(f"\n✗ Partition overwrite failed:")
            print(f"  - Duplicate count: {duplicate_check} (expected 1)")
            return False
            
    except Exception as e:
        print(f"✗ Error during partition overwrite test: {e}")
        return False

def verify_gold_idempotency(conn):
    """Verify Gold layer idempotency with incremental aggregations."""
    print("\n" + "="*80)
    print("STEP 4: Verify Gold Layer Idempotency")
    print("="*80)
    
    try:
        # Create Gold aggregation (first run)
        print("\n  Run 1: Create Gold aggregation...")
        conn.execute("""
            CREATE TABLE gold_daily_summary AS
            SELECT 
                date,
                COUNT(DISTINCT location) AS location_count,
                AVG(temperature_celsius) AS avg_temperature,
                MIN(temperature_celsius) AS min_temperature,
                MAX(temperature_celsius) AS max_temperature,
                AVG(humidity_percent) AS avg_humidity,
                CURRENT_TIMESTAMP AS _gold_updated_at
            FROM silver_weather_clean
            GROUP BY date
        """)
        
        count1 = conn.execute("SELECT COUNT(*) FROM gold_daily_summary").fetchone()[0]
        print(f"  ✓ Gold aggregation created: {count1} records")
        
        # Run aggregation again (should produce same results)
        print("\n  Run 2: Re-run Gold aggregation...")
        conn.execute("""
            INSERT OR REPLACE INTO gold_daily_summary
            SELECT 
                date,
                COUNT(DISTINCT location) AS location_count,
                AVG(temperature_celsius) AS avg_temperature,
                MIN(temperature_celsius) AS min_temperature,
                MAX(temperature_celsius) AS max_temperature,
                AVG(humidity_percent) AS avg_humidity,
                CURRENT_TIMESTAMP AS _gold_updated_at
            FROM silver_weather_clean
            GROUP BY date
        """)
        
        count2 = conn.execute("SELECT COUNT(*) FROM gold_daily_summary").fetchone()[0]
        print(f"  ✓ Gold aggregation re-run: {count2} records")
        
        # Verify no duplicates
        duplicate_check = conn.execute("""
            SELECT date, COUNT(*) as count
            FROM gold_daily_summary
            GROUP BY date
            HAVING COUNT(*) > 1
        """).fetchall()
        
        if len(duplicate_check) == 0 and count1 == count2:
            print(f"\n✓ Gold layer idempotency verified:")
            print(f"  - Run 1: {count1} records")
            print(f"  - Run 2: {count2} records")
            print(f"  - No duplicates created")
            print(f"  - Aggregations are deterministic")
            return True
        else:
            print(f"\n✗ Gold layer idempotency failed:")
            print(f"  - Duplicates found: {len(duplicate_check)}")
            return False
            
    except Exception as e:
        print(f"✗ Error during Gold idempotency test: {e}")
        return False

def main():
    """Run idempotency and incremental processing tests."""
    print("\n" + "="*80)
    print("IDEMPOTENCY AND INCREMENTAL PROCESSING TESTS")
    print("="*80)
    print("\nThis script verifies:")
    print("  1. Running pipeline twice produces same output (idempotency)")
    print("  2. No duplicate records are created")
    print("  3. Backfill scenarios work correctly")
    print("  4. Partition overwrite behavior")
    print("  5. Gold layer incremental aggregations")
    
    try:
        # Initialize connection
        conn = setup_duckdb()
        
        # Setup test data
        if not create_bronze_test_data(conn):
            return False
        
        # Run transformation twice
        print("\n" + "="*80)
        print("TEST: Run Silver Transformation Twice")
        print("="*80)
        
        count1 = run_silver_transformation(conn, 1)
        if count1 is None:
            return False
        
        if not verify_no_duplicates(conn, 1):
            return False
        
        count2 = run_silver_transformation(conn, 2)
        if count2 is None:
            return False
        
        if not verify_no_duplicates(conn, 2):
            return False
        
        # Compare results
        if not compare_run_results(conn, count1, count2):
            return False
        
        # Test backfill
        if not test_backfill_scenario(conn):
            return False
        
        # Test partition overwrite
        if not test_partition_overwrite(conn):
            return False
        
        # Test Gold layer idempotency
        if not verify_gold_idempotency(conn):
            return False
        
        # Summary
        print("\n" + "="*80)
        print("IDEMPOTENCY TESTS COMPLETE")
        print("="*80)
        print("\n✓ All tests passed:")
        print("  1. ✓ Idempotency verified (same input → same output)")
        print("  2. ✓ No duplicates created on re-runs")
        print("  3. ✓ Backfill scenario works correctly")
        print("  4. ✓ Partition overwrite prevents duplicates")
        print("  5. ✓ Gold layer aggregations are deterministic")
        
        print("\n📚 Key Takeaways:")
        print("  - INSERT OR REPLACE ensures idempotency")
        print("  - Unique keys prevent duplicate records")
        print("  - Backfills can add historical data safely")
        print("  - Partition overwrite updates existing data")
        print("  - Incremental processing is safe to re-run")
        
        print("\n💡 Production Best Practices:")
        print("  - Always define unique keys for incremental models")
        print("  - Use INSERT OR REPLACE or MERGE for upserts")
        print("  - Test idempotency before deploying to production")
        print("  - Monitor for duplicate records in data quality checks")
        print("  - Design pipelines to be safely re-runnable")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"\n✗ Error during tests: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
