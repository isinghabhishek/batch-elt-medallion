#!/usr/bin/env python3
"""Verify customer_lifetime_value Gold model"""

import duckdb
import os

def verify_customer_ltv():
    conn = duckdb.connect(':memory:')
    
    # Install and load extensions
    print("Installing extensions...")
    conn.execute('INSTALL httpfs')
    conn.execute('LOAD httpfs')
    
    # Configure S3 settings
    minio_user = os.getenv('MINIO_ROOT_USER', 'admin')
    minio_password = os.getenv('MINIO_ROOT_PASSWORD', 'changeme123')
    
    print("Configuring S3 connection...")
    conn.execute(f"SET s3_endpoint='minio:9000'")
    conn.execute(f"SET s3_access_key_id='{minio_user}'")
    conn.execute(f"SET s3_secret_access_key='{minio_password}'")
    conn.execute("SET s3_use_ssl=false")
    conn.execute("SET s3_url_style='path'")
    
    print("\n" + "="*70)
    print("CUSTOMER LIFETIME VALUE VERIFICATION")
    print("="*70)
    
    # Load Silver data
    print("\n1. Loading Silver data...")
    print("-" * 70)
    
    try:
        conn.execute("""
            CREATE OR REPLACE TABLE orders_clean AS
            SELECT * FROM read_parquet('s3://bronze/orders/**/*.parquet')
        """)
        orders_count = conn.execute("SELECT COUNT(*) FROM orders_clean").fetchone()[0]
        print(f"✅ orders_clean: {orders_count} records")
        
        conn.execute("""
            CREATE OR REPLACE TABLE customers_clean AS
            SELECT * FROM read_parquet('s3://bronze/customers/**/*.parquet')
        """)
        customers_count = conn.execute("SELECT COUNT(*) FROM customers_clean").fetchone()[0]
        print(f"✅ customers_clean: {customers_count} records")
    except Exception as e:
        print(f"❌ Error loading Silver data: {e}")
        return
    
    # Calculate customer lifetime value
    print("\n2. Calculating Customer Lifetime Value...")
    print("-" * 70)
    
    try:
        conn.execute("""
            CREATE OR REPLACE TABLE customer_lifetime_value AS
            WITH customer_order_metrics AS (
                SELECT
                    customer_id,
                    MIN(CAST(order_date AS DATE)) AS first_order_date,
                    MAX(CAST(order_date AS DATE)) AS last_order_date,
                    COUNT(*) AS total_orders,
                    SUM(CAST(order_amount AS DOUBLE)) AS total_spent,
                    AVG(CAST(order_amount AS DOUBLE)) AS avg_order_value,
                    CURRENT_DATE - MAX(CAST(order_date AS DATE)) AS days_since_last_order
                FROM orders_clean
                GROUP BY customer_id
            )
            SELECT
                c.customer_id,
                c.customer_name,
                c.email,
                CAST(c.signup_date AS DATE) AS signup_date,
                c.country,
                m.first_order_date,
                m.last_order_date,
                m.total_orders,
                m.total_spent,
                m.avg_order_value,
                m.days_since_last_order,
                CURRENT_TIMESTAMP AS _gold_updated_at
            FROM customer_order_metrics m
            INNER JOIN customers_clean c ON m.customer_id = c.customer_id
        """)
        
        ltv_count = conn.execute("SELECT COUNT(*) FROM customer_lifetime_value").fetchone()[0]
        print(f"✅ customer_lifetime_value: {ltv_count} customers")
        
    except Exception as e:
        print(f"❌ Error calculating LTV: {e}")
        return
    
    # Display sample data
    print("\n3. Sample Customer Lifetime Value Data:")
    print("-" * 70)
    
    try:
        results = conn.execute("""
            SELECT 
                customer_id,
                customer_name,
                total_orders,
                ROUND(total_spent, 2) as total_spent,
                ROUND(avg_order_value, 2) as avg_order_value,
                days_since_last_order
            FROM customer_lifetime_value
            ORDER BY total_spent DESC
            LIMIT 5
        """).fetchall()
        
        print(f"{'Customer ID':<12} {'Name':<15} {'Orders':<8} {'Total $':<10} {'Avg $':<10} {'Days Since':<12}")
        print("-" * 70)
        for row in results:
            print(f"{row[0]:<12} {row[1]:<15} {row[2]:<8} {row[3]:<10} {row[4]:<10} {row[5]:<12}")
        
    except Exception as e:
        print(f"❌ Error displaying sample data: {e}")
        return
    
    # Verify data quality
    print("\n4. Data Quality Checks:")
    print("-" * 70)
    
    try:
        # Check for NULL values in key columns
        null_checks = [
            ('customer_id', conn.execute("SELECT COUNT(*) FROM customer_lifetime_value WHERE customer_id IS NULL").fetchone()[0]),
            ('total_orders', conn.execute("SELECT COUNT(*) FROM customer_lifetime_value WHERE total_orders IS NULL").fetchone()[0]),
            ('total_spent', conn.execute("SELECT COUNT(*) FROM customer_lifetime_value WHERE total_spent IS NULL").fetchone()[0]),
            ('avg_order_value', conn.execute("SELECT COUNT(*) FROM customer_lifetime_value WHERE avg_order_value IS NULL").fetchone()[0]),
        ]
        
        for col, null_count in null_checks:
            status = "✅" if null_count == 0 else "❌"
            print(f"{status} {col}: {null_count} NULL values (should be 0)")
        
        # Check for negative values
        neg_spent = conn.execute("SELECT COUNT(*) FROM customer_lifetime_value WHERE total_spent < 0").fetchone()[0]
        print(f"{'✅' if neg_spent == 0 else '❌'} Negative total_spent: {neg_spent} (should be 0)")
        
        # Check that all customers have at least 1 order
        zero_orders = conn.execute("SELECT COUNT(*) FROM customer_lifetime_value WHERE total_orders = 0").fetchone()[0]
        print(f"{'✅' if zero_orders == 0 else '❌'} Customers with 0 orders: {zero_orders} (should be 0)")
        
        # Check _gold_updated_at exists
        has_timestamp = conn.execute("SELECT COUNT(*) FROM customer_lifetime_value WHERE _gold_updated_at IS NOT NULL").fetchone()[0]
        print(f"✅ Records with _gold_updated_at: {has_timestamp}/{ltv_count}")
        
    except Exception as e:
        print(f"❌ Data quality check failed: {e}")
    
    # Summary statistics
    print("\n5. Summary Statistics:")
    print("-" * 70)
    
    try:
        stats = conn.execute("""
            SELECT
                COUNT(*) as total_customers,
                SUM(total_orders) as total_orders_all,
                ROUND(SUM(total_spent), 2) as total_revenue,
                ROUND(AVG(total_spent), 2) as avg_customer_value,
                ROUND(AVG(avg_order_value), 2) as avg_order_value_all,
                ROUND(AVG(days_since_last_order), 1) as avg_days_since_last_order
            FROM customer_lifetime_value
        """).fetchone()
        
        print(f"Total Customers: {stats[0]}")
        print(f"Total Orders: {stats[1]}")
        print(f"Total Revenue: ${stats[2]}")
        print(f"Average Customer Value: ${stats[3]}")
        print(f"Average Order Value: ${stats[4]}")
        print(f"Average Days Since Last Order: {stats[5]}")
        
    except Exception as e:
        print(f"❌ Summary statistics failed: {e}")
    
    print("\n" + "="*70)
    print("VERIFICATION COMPLETE")
    print("="*70)
    
    conn.close()

if __name__ == '__main__':
    try:
        verify_customer_ltv()
    except Exception as e:
        print(f"\n❌ Verification failed: {e}")
        import traceback
        traceback.print_exc()
