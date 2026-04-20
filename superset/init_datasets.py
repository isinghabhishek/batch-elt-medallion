#!/usr/bin/env python3
"""
Superset Dataset Initialization Script
=======================================

This script automates the creation of Superset datasets for Gold layer tables.

Usage:
    python init_datasets.py

Prerequisites:
    - Superset running and accessible
    - Database connection configured in Superset
    - pip install requests
"""

import requests
import json
import time
import sys

# Superset configuration
SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"

# Dataset definitions
DATASETS = [
    {
        "name": "pipeline_runs",
        "sql": "SELECT * FROM read_parquet('s3://gold/pipeline_runs/*.parquet')",
        "description": "Pipeline run metadata including status, duration, and row counts",
        "columns": [
            {"name": "dag_run_id", "type": "STRING", "is_filterable": True},
            {"name": "start_time", "type": "DATETIME", "is_temporal": True, "is_filterable": True},
            {"name": "end_time", "type": "DATETIME", "is_temporal": True, "is_filterable": True},
            {"name": "status", "type": "STRING", "is_filterable": True},
            {"name": "bronze_row_count", "type": "BIGINT", "is_filterable": False},
            {"name": "silver_row_count", "type": "BIGINT", "is_filterable": False},
            {"name": "gold_row_count", "type": "BIGINT", "is_filterable": False},
            {"name": "duration_seconds", "type": "BIGINT", "is_filterable": False},
        ]
    },
    {
        "name": "order_metrics",
        "sql": "SELECT * FROM read_parquet('s3://gold/order_metrics/*.parquet')",
        "description": "Daily order aggregations including revenue and customer counts",
        "columns": [
            {"name": "date", "type": "DATE", "is_temporal": True, "is_filterable": True},
            {"name": "total_orders", "type": "BIGINT", "is_filterable": False},
            {"name": "total_revenue", "type": "DOUBLE", "is_filterable": False},
            {"name": "avg_order_value", "type": "DOUBLE", "is_filterable": False},
            {"name": "unique_customers", "type": "BIGINT", "is_filterable": False},
            {"name": "_gold_updated_at", "type": "DATETIME", "is_temporal": True, "is_filterable": True},
        ]
    },
    {
        "name": "customer_lifetime_value",
        "sql": "SELECT * FROM read_parquet('s3://gold/customer_lifetime_value/*.parquet')",
        "description": "Customer metrics including total orders, spend, and recency",
        "columns": [
            {"name": "customer_id", "type": "STRING", "is_filterable": True},
            {"name": "first_order_date", "type": "DATE", "is_temporal": True, "is_filterable": True},
            {"name": "last_order_date", "type": "DATE", "is_temporal": True, "is_filterable": True},
            {"name": "total_orders", "type": "BIGINT", "is_filterable": False},
            {"name": "total_spent", "type": "DOUBLE", "is_filterable": False},
            {"name": "avg_order_value", "type": "DOUBLE", "is_filterable": False},
            {"name": "days_since_last_order", "type": "BIGINT", "is_filterable": False},
            {"name": "_gold_updated_at", "type": "DATETIME", "is_temporal": True, "is_filterable": True},
        ]
    },
    {
        "name": "dq_results",
        "sql": "SELECT * FROM read_parquet('s3://gold/dq_results/*.parquet')",
        "description": "Data quality check results for all pipeline layers",
        "columns": [
            {"name": "check_id", "type": "STRING", "is_filterable": True},
            {"name": "check_name", "type": "STRING", "is_filterable": True},
            {"name": "layer", "type": "STRING", "is_filterable": True},
            {"name": "table_name", "type": "STRING", "is_filterable": True},
            {"name": "check_timestamp", "type": "DATETIME", "is_temporal": True, "is_filterable": True},
            {"name": "status", "type": "STRING", "is_filterable": True},
            {"name": "row_count", "type": "BIGINT", "is_filterable": False},
            {"name": "error_message", "type": "STRING", "is_filterable": False},
        ]
    },
]


class SupersetClient:
    """Client for interacting with Superset API"""
    
    def __init__(self, base_url, username, password):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.access_token = None
        self.csrf_token = None
        
    def login(self):
        """Authenticate with Superset and get access token"""
        print(f"Logging in to Superset at {self.base_url}...")
        
        # Get CSRF token
        response = self.session.get(f"{self.base_url}/login/")
        if response.status_code != 200:
            print(f"Error: Could not access Superset login page (status {response.status_code})")
            return False
        
        # Login to get access token
        login_data = {
            "username": self.username,
            "password": self.password,
            "provider": "db",
            "refresh": True
        }
        
        response = self.session.post(
            f"{self.base_url}/api/v1/security/login",
            json=login_data
        )
        
        if response.status_code == 200:
            data = response.json()
            self.access_token = data.get("access_token")
            self.session.headers.update({
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            })
            print("✓ Successfully logged in")
            return True
        else:
            print(f"✗ Login failed (status {response.status_code}): {response.text}")
            return False
    
    def get_csrf_token(self):
        """Get CSRF token for POST requests"""
        response = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/")
        if response.status_code == 200:
            self.csrf_token = response.json().get("result")
            self.session.headers.update({"X-CSRFToken": self.csrf_token})
            return True
        return False
    
    def get_database_id(self, database_name="Gold Layer (DuckDB)"):
        """Get database ID by name"""
        response = self.session.get(f"{self.base_url}/api/v1/database/")
        if response.status_code == 200:
            databases = response.json().get("result", [])
            for db in databases:
                if db.get("database_name") == database_name:
                    return db.get("id")
        return None
    
    def create_dataset(self, dataset_config, database_id):
        """Create a dataset in Superset"""
        dataset_name = dataset_config["name"]
        print(f"\nCreating dataset: {dataset_name}")
        
        # Check if dataset already exists
        response = self.session.get(
            f"{self.base_url}/api/v1/dataset/",
            params={"q": json.dumps({"filters": [{"col": "table_name", "opr": "eq", "value": dataset_name}]})}
        )
        
        if response.status_code == 200:
            existing = response.json().get("result", [])
            if existing:
                print(f"  ✓ Dataset '{dataset_name}' already exists (ID: {existing[0]['id']})")
                return existing[0]["id"]
        
        # Create dataset
        dataset_data = {
            "database": database_id,
            "schema": "",
            "table_name": dataset_name,
            "sql": dataset_config["sql"],
            "description": dataset_config.get("description", ""),
            "cache_timeout": 300,  # 5 minutes
            "is_sqllab_view": False,
        }
        
        response = self.session.post(
            f"{self.base_url}/api/v1/dataset/",
            json=dataset_data
        )
        
        if response.status_code == 201:
            dataset_id = response.json().get("id")
            print(f"  ✓ Created dataset '{dataset_name}' (ID: {dataset_id})")
            return dataset_id
        else:
            print(f"  ✗ Failed to create dataset '{dataset_name}': {response.text}")
            return None
    
    def refresh_dataset(self, dataset_id):
        """Refresh dataset schema"""
        response = self.session.put(
            f"{self.base_url}/api/v1/dataset/{dataset_id}/refresh"
        )
        if response.status_code == 200:
            print(f"  ✓ Refreshed dataset schema")
            return True
        return False


def main():
    """Main execution function"""
    print("=" * 60)
    print("Superset Dataset Initialization")
    print("=" * 60)
    
    # Create client and login
    client = SupersetClient(SUPERSET_URL, USERNAME, PASSWORD)
    
    if not client.login():
        print("\n✗ Failed to login to Superset")
        print("  Make sure Superset is running at", SUPERSET_URL)
        print("  and credentials are correct")
        sys.exit(1)
    
    # Get CSRF token
    if not client.get_csrf_token():
        print("\n✗ Failed to get CSRF token")
        sys.exit(1)
    
    # Get database ID
    print("\nLooking for database connection...")
    database_id = client.get_database_id()
    
    if not database_id:
        print("✗ Database 'Gold Layer (DuckDB)' not found")
        print("  Please configure the database connection first")
        print("  See docs/superset-setup.md for instructions")
        sys.exit(1)
    
    print(f"✓ Found database (ID: {database_id})")
    
    # Create datasets
    print("\n" + "=" * 60)
    print("Creating Datasets")
    print("=" * 60)
    
    created_count = 0
    for dataset_config in DATASETS:
        dataset_id = client.create_dataset(dataset_config, database_id)
        if dataset_id:
            created_count += 1
            # Refresh schema
            client.refresh_dataset(dataset_id)
            time.sleep(1)  # Rate limiting
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"✓ Successfully created/verified {created_count}/{len(DATASETS)} datasets")
    print("\nNext steps:")
    print("  1. Navigate to Superset UI:", SUPERSET_URL)
    print("  2. Go to Data > Datasets to view created datasets")
    print("  3. Create charts and dashboards using these datasets")
    print("  4. See docs/superset-setup.md for dashboard creation guide")
    print("=" * 60)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
