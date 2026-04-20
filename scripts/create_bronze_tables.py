#!/usr/bin/env python3
"""
Create Bronze Layer Iceberg Tables
===================================
This script creates the Bronze layer Iceberg table schemas with metadata columns.

Bronze tables preserve raw source data with minimal transformation, adding only
metadata columns for lineage tracking.

Usage:
    python scripts/create_bronze_tables.py
"""

import duckdb
import os
from datetime import datetime

# Get credentials from environment
MINIO_USER = os.getenv('MINIO_ROOT_USER', 'admin')
MINIO_PASSWORD = os.getenv('MINIO_ROOT_PASSWORD', 'changeme123')

def create_bronze_tables():
    """Create Bronze layer Iceberg tables with metadata columns."""
    
    # Connect to DuckDB
    conn = duckdb.connect(':memory:')
    
    # Install and load required extensions
    print("Loading DuckDB extensions...")
    conn.execute("INSTALL iceberg;")
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD iceberg;")
    conn.execute("LOAD httpfs;")
    
    # Configure S3 connection to MinIO
    print("Configuring S3 connection to MinIO...")
    conn.execute(f"SET s3_endpoint='minio:9000';")
    conn.execute(f"SET s3_access_key_id='{MINIO_USER}';")
    conn.execute(f"SET s3_secret_access_key='{MINIO_PASSWORD}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")
    
    # Create Iceberg catalog connection
    print("Connecting to Iceberg REST Catalog...")
    conn.execute("""
        CREATE SECRET iceberg_rest (
            TYPE ICEBERG_REST,
            CATALOG_URL 'http://iceberg-rest:8181',
            S3_ENDPOINT 'minio:9000',
            S3_ACCESS_KEY_ID '{}',
            S3_SECRET_ACCESS_KEY '{}',
            S3_USE_SSL false
        );
    """.format(MINIO_USER, MINIO_PASSWORD))
    
    # Create Bronze namespace
    print("Creating Bronze namespace...")
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS iceberg_rest.bronze;")
        print("✓ Bronze namespace created")
    except Exception as e:
        print(f"Note: {e}")
    
    # Define Bronze table schemas
    bronze_tables = {
        'weather_data': """
            CREATE TABLE IF NOT EXISTS iceberg_rest.bronze.weather_data (
                latitude DOUBLE,
                longitude DOUBLE,
                timezone VARCHAR,
                current_time VARCHAR,
                temperature_2m DOUBLE,
                relative_humidity_2m DOUBLE,
                weather_code INTEGER,
                wind_speed_10m DOUBLE,
                _source_name VARCHAR,
                _ingested_at TIMESTAMP,
                _file_path VARCHAR,
                _has_null_key BOOLEAN
            )
            USING iceberg
            PARTITIONED BY (days(_ingested_at))
            LOCATION 's3://bronze/weather_data/';
        """,
        
        'customers': """
            CREATE TABLE IF NOT EXISTS iceberg_rest.bronze.customers (
                customer_id INTEGER,
                first_name VARCHAR,
                last_name VARCHAR,
                email VARCHAR,
                phone VARCHAR,
                city VARCHAR,
                state VARCHAR,
                country VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                _source_name VARCHAR,
                _ingested_at TIMESTAMP,
                _file_path VARCHAR,
                _has_null_key BOOLEAN
            )
            USING iceberg
            PARTITIONED BY (days(_ingested_at))
            LOCATION 's3://bronze/customers/';
        """,
        
        'orders': """
            CREATE TABLE IF NOT EXISTS iceberg_rest.bronze.orders (
                order_id INTEGER,
                customer_id INTEGER,
                order_date DATE,
                order_status VARCHAR,
                total_amount DECIMAL(10, 2),
                payment_method VARCHAR,
                shipping_address VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                _source_name VARCHAR,
                _ingested_at TIMESTAMP,
                _file_path VARCHAR,
                _has_null_key BOOLEAN
            )
            USING iceberg
            PARTITIONED BY (days(_ingested_at))
            LOCATION 's3://bronze/orders/';
        """,
        
        'products': """
            CREATE TABLE IF NOT EXISTS iceberg_rest.bronze.products (
                product_id INTEGER,
                product_name VARCHAR,
                category VARCHAR,
                price DECIMAL(10, 2),
                stock_quantity INTEGER,
                supplier VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                _source_name VARCHAR,
                _ingested_at TIMESTAMP,
                _file_path VARCHAR,
                _has_null_key BOOLEAN
            )
            USING iceberg
            PARTITIONED BY (days(_ingested_at))
            LOCATION 's3://bronze/products/';
        """,
        
        'inventory_movements': """
            CREATE TABLE IF NOT EXISTS iceberg_rest.bronze.inventory_movements (
                movement_id INTEGER,
                product_id INTEGER,
                movement_type VARCHAR,
                quantity INTEGER,
                movement_date DATE,
                notes VARCHAR,
                warehouse VARCHAR,
                _source_name VARCHAR,
                _ingested_at TIMESTAMP,
                _file_path VARCHAR,
                _has_null_key BOOLEAN
            )
            USING iceberg
            PARTITIONED BY (days(_ingested_at))
            LOCATION 's3://bronze/inventory_movements/';
        """
    }
    
    # Create each Bronze table
    print("\nCreating Bronze tables...")
    for table_name, create_sql in bronze_tables.items():
        try:
            print(f"  Creating bronze.{table_name}...")
            conn.execute(create_sql)
            print(f"  ✓ bronze.{table_name} created")
        except Exception as e:
            print(f"  ✗ Error creating bronze.{table_name}: {e}")
    
    # List created tables
    print("\nVerifying Bronze tables...")
    try:
        result = conn.execute("SHOW TABLES FROM iceberg_rest.bronze;").fetchall()
        print(f"Found {len(result)} tables in Bronze namespace:")
        for row in result:
            print(f"  - {row[0]}")
    except Exception as e:
        print(f"Could not list tables: {e}")
    
    conn.close()
    print("\n✓ Bronze table creation complete!")

if __name__ == "__main__":
    print("=" * 60)
    print("Bronze Layer Iceberg Table Creation")
    print("=" * 60)
    create_bronze_tables()
