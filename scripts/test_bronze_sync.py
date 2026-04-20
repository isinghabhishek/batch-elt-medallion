#!/usr/bin/env python3
"""
Test script for Airbyte sync to Bronze layer (Task 3.8)

This script:
1. Triggers Airbyte sync jobs via API
2. Verifies data lands in MinIO bronze/ bucket
3. Queries Bronze Iceberg tables to confirm ingestion
4. Validates metadata columns are populated correctly

Requirements: 4.2, 4.3, 4.4, 5.1, 5.2, 5.3
"""

import requests
import time
import json
from datetime import datetime
import duckdb
import os

# Configuration
AIRBYTE_API_URL = "http://localhost:8001/api/v1"
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_USER = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "changeme123")
ICEBERG_CATALOG_URL = "http://localhost:8181"

# Connection IDs (these need to be set after creating connections in Airbyte)
WEATHER_CONNECTION_ID = os.getenv("WEATHER_CONNECTION_ID", "")
POSTGRES_CONNECTION_ID = os.getenv("POSTGRES_CONNECTION_ID", "")
FILE_CONNECTION_ID = os.getenv("FILE_CONNECTION_ID", "")


def trigger_airbyte_sync(connection_id, connection_name):
    """Trigger an Airbyte sync job and wait for completion"""
    print(f"\n{'='*60}")
    print(f"Testing {connection_name} sync...")
    print(f"{'='*60}")
    
    if not connection_id:
        print(f"⚠️  WARNING: {connection_name} connection ID not set")
        print(f"   Please set the environment variable and re-run")
        return None
    
    # Trigger sync
    print(f"🚀 Triggering sync for connection: {connection_id}")
    try:
        response = requests.post(
            f"{AIRBYTE_API_URL}/connections/{connection_id}/sync",
            headers={"Content-Type": "application/json"},
            json={}
        )
        response.raise_for_status()
        job_data = response.json()
        job_id = job_data.get("job", {}).get("id")
        print(f"✅ Sync job started: {job_id}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to trigger sync: {e}")
        return None
    
    # Poll for completion
    print(f"⏳ Waiting for sync to complete...")
    max_wait = 600  # 10 minutes
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        try:
            response = requests.get(
                f"{AIRBYTE_API_URL}/jobs/{job_id}",
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            job_status = response.json()
            
            status = job_status.get("job", {}).get("status")
            print(f"   Status: {status}")
            
            if status == "succeeded":
                print(f"✅ Sync completed successfully!")
                return job_status
            elif status in ["failed", "cancelled"]:
                print(f"❌ Sync {status}")
                return None
            
            time.sleep(10)
        except requests.exceptions.RequestException as e:
            print(f"⚠️  Error checking job status: {e}")
            time.sleep(10)
    
    print(f"⏱️  Sync timed out after {max_wait} seconds")
    return None


def verify_minio_bucket(bucket_name, source_name):
    """Verify data landed in MinIO bucket"""
    print(f"\n📦 Verifying MinIO bucket: {bucket_name}/{source_name}/")
    
    try:
        import boto3
        from botocore.client import Config
        
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_USER,
            aws_secret_access_key=MINIO_PASSWORD,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'
        )
        
        # List objects in bronze bucket
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=f"{source_name}/"
        )
        
        if 'Contents' in response:
            file_count = len(response['Contents'])
            total_size = sum(obj['Size'] for obj in response['Contents'])
            print(f"✅ Found {file_count} files, total size: {total_size:,} bytes")
            
            # Show first few files
            print(f"\n   Sample files:")
            for obj in response['Contents'][:5]:
                print(f"   - {obj['Key']} ({obj['Size']:,} bytes)")
            
            return True
        else:
            print(f"❌ No files found in {bucket_name}/{source_name}/")
            return False
            
    except Exception as e:
        print(f"❌ Error accessing MinIO: {e}")
        print(f"   Make sure boto3 is installed: pip install boto3")
        return False


def query_bronze_table(table_name):
    """Query Bronze Iceberg table and validate metadata columns"""
    print(f"\n🔍 Querying Bronze table: {table_name}")
    
    try:
        # Initialize DuckDB with Iceberg extension
        conn = duckdb.connect(':memory:')
        
        # Install and load extensions
        conn.execute("INSTALL iceberg;")
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD iceberg;")
        conn.execute("LOAD httpfs;")
        
        # Configure S3 settings
        conn.execute(f"SET s3_endpoint='{MINIO_ENDPOINT}';")
        conn.execute(f"SET s3_access_key_id='{MINIO_USER}';")
        conn.execute(f"SET s3_secret_access_key='{MINIO_PASSWORD}';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_url_style='path';")
        
        # Create Iceberg REST catalog secret
        conn.execute(f"""
            CREATE SECRET iceberg_rest (
                TYPE ICEBERG_REST,
                CATALOG_URL '{ICEBERG_CATALOG_URL}',
                S3_ENDPOINT '{MINIO_ENDPOINT}',
                S3_ACCESS_KEY_ID '{MINIO_USER}',
                S3_SECRET_ACCESS_KEY '{MINIO_PASSWORD}',
                S3_USE_SSL false
            );
        """)
        
        # Query the table
        full_table_name = f"iceberg_rest.bronze.{table_name}"
        
        # Check if table exists
        try:
            result = conn.execute(f"SELECT COUNT(*) as row_count FROM {full_table_name}").fetchone()
            row_count = result[0]
            print(f"✅ Table exists with {row_count:,} rows")
        except Exception as e:
            print(f"❌ Table does not exist or cannot be queried: {e}")
            return False
        
        # Validate metadata columns (Requirement 5.2)
        print(f"\n   Validating metadata columns...")
        required_columns = ['_source_name', '_ingested_at', '_file_path']
        
        try:
            # Get table schema
            schema_result = conn.execute(f"DESCRIBE {full_table_name}").fetchall()
            column_names = [row[0] for row in schema_result]
            
            print(f"   Table columns: {', '.join(column_names)}")
            
            # Check for required metadata columns
            missing_columns = [col for col in required_columns if col not in column_names]
            if missing_columns:
                print(f"❌ Missing required metadata columns: {', '.join(missing_columns)}")
                return False
            else:
                print(f"✅ All required metadata columns present")
            
            # Sample data with metadata
            print(f"\n   Sample records with metadata:")
            sample = conn.execute(f"""
                SELECT * FROM {full_table_name} 
                LIMIT 3
            """).fetchall()
            
            for i, row in enumerate(sample, 1):
                print(f"\n   Record {i}:")
                for col_name, value in zip(column_names, row):
                    if col_name in required_columns:
                        print(f"      {col_name}: {value}")
            
            # Validate metadata is populated (Requirement 5.2, 5.3)
            validation_result = conn.execute(f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(_source_name) as source_name_count,
                    COUNT(_ingested_at) as ingested_at_count,
                    COUNT(_file_path) as file_path_count,
                    MIN(_ingested_at) as earliest_ingestion,
                    MAX(_ingested_at) as latest_ingestion
                FROM {full_table_name}
            """).fetchone()
            
            print(f"\n   Metadata validation:")
            print(f"      Total rows: {validation_result[0]:,}")
            print(f"      _source_name populated: {validation_result[1]:,} ({validation_result[1]/validation_result[0]*100:.1f}%)")
            print(f"      _ingested_at populated: {validation_result[2]:,} ({validation_result[2]/validation_result[0]*100:.1f}%)")
            print(f"      _file_path populated: {validation_result[3]:,} ({validation_result[3]/validation_result[0]*100:.1f}%)")
            print(f"      Ingestion time range: {validation_result[4]} to {validation_result[5]}")
            
            if validation_result[1] == validation_result[0] and \
               validation_result[2] == validation_result[0] and \
               validation_result[3] == validation_result[0]:
                print(f"✅ All metadata columns are 100% populated")
                return True
            else:
                print(f"⚠️  Some metadata columns have NULL values")
                return False
                
        except Exception as e:
            print(f"❌ Error validating metadata: {e}")
            return False
        
    except Exception as e:
        print(f"❌ Error querying table: {e}")
        print(f"   Make sure duckdb is installed: pip install duckdb")
        return False
    finally:
        if 'conn' in locals():
            conn.close()


def main():
    """Main test execution"""
    print("="*60)
    print("Task 3.8: Test Airbyte Sync to Bronze Layer")
    print("="*60)
    print(f"Started at: {datetime.now().isoformat()}")
    
    # Test configuration
    test_sources = [
        {
            "name": "Weather API",
            "connection_id": WEATHER_CONNECTION_ID,
            "bucket": "bronze",
            "prefix": "weather_api",
            "table": "weather_data"
        },
        {
            "name": "PostgreSQL",
            "connection_id": POSTGRES_CONNECTION_ID,
            "bucket": "bronze",
            "prefix": "postgres_source",
            "table": "orders"
        },
        {
            "name": "CSV Files",
            "connection_id": FILE_CONNECTION_ID,
            "bucket": "bronze",
            "prefix": "file_source",
            "table": "products"
        }
    ]
    
    results = []
    
    for source in test_sources:
        print(f"\n\n{'#'*60}")
        print(f"# Testing: {source['name']}")
        print(f"{'#'*60}")
        
        # Step 1: Trigger sync
        sync_result = trigger_airbyte_sync(source['connection_id'], source['name'])
        
        if sync_result:
            # Step 2: Verify MinIO bucket
            minio_ok = verify_minio_bucket(source['bucket'], source['prefix'])
            
            # Step 3: Query Bronze table
            table_ok = query_bronze_table(source['table'])
            
            results.append({
                "source": source['name'],
                "sync": "✅ Success",
                "minio": "✅ Verified" if minio_ok else "❌ Failed",
                "table": "✅ Validated" if table_ok else "❌ Failed"
            })
        else:
            results.append({
                "source": source['name'],
                "sync": "❌ Failed",
                "minio": "⏭️  Skipped",
                "table": "⏭️  Skipped"
            })
    
    # Summary
    print(f"\n\n{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}")
    print(f"{'Source':<20} {'Sync':<15} {'MinIO':<15} {'Table':<15}")
    print(f"{'-'*60}")
    for result in results:
        print(f"{result['source']:<20} {result['sync']:<15} {result['minio']:<15} {result['table']:<15}")
    
    print(f"\n{'='*60}")
    print(f"Completed at: {datetime.now().isoformat()}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
