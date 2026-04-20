#!/usr/bin/env python3
"""Create mock Bronze data for testing Silver transformations"""

import duckdb
import os
from datetime import datetime, timedelta

def create_mock_bronze_data():
    conn = duckdb.connect(':memory:')
    
    # Install and load extensions
    print("Installing extensions...")
    conn.execute('INSTALL httpfs')
    conn.execute('INSTALL iceberg')
    conn.execute('LOAD httpfs')
    conn.execute('LOAD iceberg')
    
    # Configure S3 settings
    minio_user = os.getenv('MINIO_ROOT_USER', 'admin')
    minio_password = os.getenv('MINIO_ROOT_PASSWORD', 'changeme123')
    
    print("Configuring S3 connection...")
    conn.execute(f"SET s3_endpoint='minio:9000'")
    conn.execute(f"SET s3_access_key_id='{minio_user}'")
    conn.execute(f"SET s3_secret_access_key='{minio_password}'")
    conn.execute("SET s3_use_ssl=false")
    conn.execute("SET s3_url_style='path'")
    
    # Create mock weather data
    print("\nCreating mock weather data...")
    weather_data = []
    base_date = datetime.now() - timedelta(days=7)
    locations = ['New York', 'London', 'Tokyo', 'Sydney']
    
    for i in range(20):
        date = base_date + timedelta(days=i % 7)
        for location in locations:
            weather_data.append({
                'location': location,
                'date': date.strftime('%Y-%m-%d'),
                'temperature': str(15 + (i % 15)),
                'humidity': str(50 + (i % 30)),
                'condition': 'Sunny' if i % 3 == 0 else 'Cloudy',
                '_source_name': 'weather_api',
                '_ingested_at': datetime.now().isoformat(),
                '_file_path': f's3://bronze/weather_data/{date.strftime("%Y%m%d")}/data.parquet',
                '_has_null_key': False
            })
    
    # Create mock customers data
    print("Creating mock customers data...")
    customers_data = []
    for i in range(10):
        customers_data.append({
            'customer_id': str(1000 + i),
            'customer_name': f'Customer {i+1}',
            'email': f'customer{i+1}@example.com' if i % 2 == 0 else '',  # Some empty emails
            'signup_date': (datetime.now() - timedelta(days=365-i*30)).strftime('%Y-%m-%d'),
            'country': 'USA' if i % 3 == 0 else 'UK',
            '_source_name': 'postgres_source',
            '_ingested_at': datetime.now().isoformat(),
            '_file_path': 's3://bronze/customers/20260420/data.parquet',
            '_has_null_key': False
        })
    
    # Create mock orders data
    print("Creating mock orders data...")
    orders_data = []
    for i in range(30):
        orders_data.append({
            'order_id': str(5000 + i),
            'customer_id': str(1000 + (i % 10)),
            'order_date': (datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d'),
            'order_amount': str(50.0 + (i * 10.5)),
            'status': 'completed' if i % 3 == 0 else 'pending',
            '_source_name': 'postgres_source',
            '_ingested_at': datetime.now().isoformat(),
            '_file_path': 's3://bronze/orders/20260420/data.parquet',
            '_has_null_key': False
        })
    
    # Write data to Bronze bucket as Parquet files
    print("\nWriting weather data to S3...")
    # Create table from values
    weather_values = ", ".join([
        f"('{w['location']}', '{w['date']}', '{w['temperature']}', '{w['humidity']}', '{w['condition']}', '{w['_source_name']}', '{w['_ingested_at']}', '{w['_file_path']}', {w['_has_null_key']})"
        for w in weather_data
    ])
    conn.execute(f"""
        CREATE TEMP TABLE temp_weather AS 
        SELECT * FROM (VALUES {weather_values}) 
        AS t(location, date, temperature, humidity, condition, _source_name, _ingested_at, _file_path, _has_null_key)
    """)
    conn.execute("COPY temp_weather TO 's3://bronze/weather_data/20260420/data.parquet' (FORMAT PARQUET)")
    print(f"✅ Wrote {len(weather_data)} weather records")
    
    print("Writing customers data to S3...")
    customers_values = ", ".join([
        f"('{c['customer_id']}', '{c['customer_name']}', '{c['email']}', '{c['signup_date']}', '{c['country']}', '{c['_source_name']}', '{c['_ingested_at']}', '{c['_file_path']}', {c['_has_null_key']})"
        for c in customers_data
    ])
    conn.execute(f"""
        CREATE TEMP TABLE temp_customers AS 
        SELECT * FROM (VALUES {customers_values}) 
        AS t(customer_id, customer_name, email, signup_date, country, _source_name, _ingested_at, _file_path, _has_null_key)
    """)
    conn.execute("COPY temp_customers TO 's3://bronze/customers/20260420/data.parquet' (FORMAT PARQUET)")
    print(f"✅ Wrote {len(customers_data)} customer records")
    
    print("Writing orders data to S3...")
    orders_values = ", ".join([
        f"('{o['order_id']}', '{o['customer_id']}', '{o['order_date']}', '{o['order_amount']}', '{o['status']}', '{o['_source_name']}', '{o['_ingested_at']}', '{o['_file_path']}', {o['_has_null_key']})"
        for o in orders_data
    ])
    conn.execute(f"""
        CREATE TEMP TABLE temp_orders AS 
        SELECT * FROM (VALUES {orders_values}) 
        AS t(order_id, customer_id, order_date, order_amount, status, _source_name, _ingested_at, _file_path, _has_null_key)
    """)
    conn.execute("COPY temp_orders TO 's3://bronze/orders/20260420/data.parquet' (FORMAT PARQUET)")
    print(f"✅ Wrote {len(orders_data)} order records")
    
    # Verify data was written
    print("\nVerifying data...")
    weather_count = conn.execute("SELECT COUNT(*) FROM read_parquet('s3://bronze/weather_data/**/*.parquet')").fetchone()[0]
    customers_count = conn.execute("SELECT COUNT(*) FROM read_parquet('s3://bronze/customers/**/*.parquet')").fetchone()[0]
    orders_count = conn.execute("SELECT COUNT(*) FROM read_parquet('s3://bronze/orders/**/*.parquet')").fetchone()[0]
    
    print(f"\n✅ Bronze data created successfully!")
    print(f"   - Weather records: {weather_count}")
    print(f"   - Customer records: {customers_count}")
    print(f"   - Order records: {orders_count}")
    
    conn.close()

if __name__ == '__main__':
    try:
        create_mock_bronze_data()
    except Exception as e:
        print(f"\n❌ Error creating Bronze data: {e}")
        import traceback
        traceback.print_exc()
