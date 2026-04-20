#!/usr/bin/env python3
"""Check if Bronze data exists in MinIO"""

import duckdb
import os

def check_bronze_data():
    conn = duckdb.connect(':memory:')
    
    # Install and load extensions
    conn.execute('INSTALL httpfs')
    conn.execute('LOAD httpfs')
    
    # Configure S3 settings
    minio_user = os.getenv('MINIO_ROOT_USER', 'admin')
    minio_password = os.getenv('MINIO_ROOT_PASSWORD', 'changeme123')
    
    conn.execute(f"SET s3_endpoint='minio:9000'")
    conn.execute(f"SET s3_access_key_id='{minio_user}'")
    conn.execute(f"SET s3_secret_access_key='{minio_password}'")
    conn.execute("SET s3_use_ssl=false")
    conn.execute("SET s3_url_style='path'")
    
    # Check for data in bronze bucket
    try:
        result = conn.execute("SELECT * FROM read_parquet('s3://bronze/**/*.parquet') LIMIT 5").fetchall()
        if result:
            print(f"✅ Found {len(result)} records in Bronze bucket")
            print("Sample data:")
            for row in result:
                print(row)
        else:
            print("⚠️  No data found in Bronze bucket")
    except Exception as e:
        print(f"❌ Error reading Bronze data: {e}")
        print("\nThis likely means no Bronze tables have been created yet.")
        print("You need to run task 3.7 (Create Bronze Iceberg table schemas)")
        print("and task 3.8 (Test Airbyte sync to Bronze layer) first.")
    
    conn.close()

if __name__ == '__main__':
    check_bronze_data()
