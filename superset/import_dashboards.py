#!/usr/bin/env python3
"""
Superset Dashboard Import Script
=================================

This script automates the import of dashboard definitions into Superset.

Usage:
    python import_dashboards.py

Prerequisites:
    - Superset running and accessible
    - Datasets already created
    - Dashboard JSON files in superset/dashboards/
    - pip install requests
"""

import requests
import json
import os
import sys
import time

# Superset configuration
SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"
DASHBOARDS_DIR = "/app/dashboards"

# Dashboard files to import
DASHBOARD_FILES = [
    "pipeline_metrics.json",
    "business_kpis.json"
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
    
    def get_dataset_id(self, dataset_name):
        """Get dataset ID by name"""
        response = self.session.get(
            f"{self.base_url}/api/v1/dataset/",
            params={"q": json.dumps({"filters": [{"col": "table_name", "opr": "eq", "value": dataset_name}]})}
        )
        
        if response.status_code == 200:
            datasets = response.json().get("result", [])
            if datasets:
                return datasets[0].get("id")
        return None
    
    def create_chart(self, chart_config, dataset_id, database_id):
        """Create a chart in Superset"""
        chart_name = chart_config["slice_name"]
        print(f"  Creating chart: {chart_name}")
        
        # Check if chart already exists
        response = self.session.get(
            f"{self.base_url}/api/v1/chart/",
            params={"q": json.dumps({"filters": [{"col": "slice_name", "opr": "eq", "value": chart_name}]})}
        )
        
        if response.status_code == 200:
            existing = response.json().get("result", [])
            if existing:
                print(f"    ✓ Chart '{chart_name}' already exists (ID: {existing[0]['id']})")
                return existing[0]["id"]
        
        # Create chart
        chart_data = {
            "slice_name": chart_name,
            "viz_type": chart_config["viz_type"],
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "params": json.dumps(chart_config["params"]),
            "description": chart_config.get("description", ""),
            "cache_timeout": 300,
        }
        
        response = self.session.post(
            f"{self.base_url}/api/v1/chart/",
            json=chart_data
        )
        
        if response.status_code == 201:
            chart_id = response.json().get("id")
            print(f"    ✓ Created chart '{chart_name}' (ID: {chart_id})")
            return chart_id
        else:
            print(f"    ✗ Failed to create chart '{chart_name}': {response.text}")
            return None
    
    def create_dashboard(self, dashboard_config, chart_ids):
        """Create a dashboard in Superset"""
        dashboard_title = dashboard_config["dashboard_title"]
        print(f"\nCreating dashboard: {dashboard_title}")
        
        # Check if dashboard already exists
        response = self.session.get(
            f"{self.base_url}/api/v1/dashboard/",
            params={"q": json.dumps({"filters": [{"col": "dashboard_title", "opr": "eq", "value": dashboard_title}]})}
        )
        
        if response.status_code == 200:
            existing = response.json().get("result", [])
            if existing:
                print(f"  ✓ Dashboard '{dashboard_title}' already exists (ID: {existing[0]['id']})")
                return existing[0]["id"]
        
        # Update position_json with actual chart IDs
        position_json = dashboard_config["position_json"].copy()
        for i, chart_id in enumerate(chart_ids):
            chart_key = f"CHART-{i}"
            if chart_key in position_json:
                position_json[chart_key]["meta"]["chartId"] = chart_id
        
        # Create dashboard
        dashboard_data = {
            "dashboard_title": dashboard_title,
            "slug": dashboard_config.get("slug", dashboard_title.lower().replace(" ", "-")),
            "published": dashboard_config.get("published", True),
            "position_json": json.dumps(position_json),
            "json_metadata": json.dumps(dashboard_config.get("json_metadata", {})),
            "css": dashboard_config.get("css", ""),
            "description": dashboard_config.get("description", ""),
        }
        
        response = self.session.post(
            f"{self.base_url}/api/v1/dashboard/",
            json=dashboard_data
        )
        
        if response.status_code == 201:
            dashboard_id = response.json().get("id")
            print(f"  ✓ Created dashboard '{dashboard_title}' (ID: {dashboard_id})")
            
            # Add charts to dashboard
            self.add_charts_to_dashboard(dashboard_id, chart_ids)
            
            return dashboard_id
        else:
            print(f"  ✗ Failed to create dashboard '{dashboard_title}': {response.text}")
            return None
    
    def add_charts_to_dashboard(self, dashboard_id, chart_ids):
        """Add charts to a dashboard"""
        response = self.session.put(
            f"{self.base_url}/api/v1/dashboard/{dashboard_id}",
            json={"slices": chart_ids}
        )
        
        if response.status_code == 200:
            print(f"  ✓ Added {len(chart_ids)} charts to dashboard")
            return True
        else:
            print(f"  ✗ Failed to add charts to dashboard: {response.text}")
            return False
    
    def import_dashboard(self, dashboard_file, database_id):
        """Import a dashboard from JSON file"""
        print(f"\n{'=' * 60}")
        print(f"Importing: {dashboard_file}")
        print(f"{'=' * 60}")
        
        # Load dashboard definition
        file_path = os.path.join(DASHBOARDS_DIR, dashboard_file)
        if not os.path.exists(file_path):
            print(f"✗ Dashboard file not found: {file_path}")
            return False
        
        with open(file_path, 'r') as f:
            dashboard_config = json.load(f)
        
        # Create charts
        chart_ids = []
        for chart_config in dashboard_config.get("slices", []):
            # Get dataset ID
            dataset_name = chart_config["datasource_name"]
            dataset_id = self.get_dataset_id(dataset_name)
            
            if not dataset_id:
                print(f"  ✗ Dataset '{dataset_name}' not found. Please create datasets first.")
                continue
            
            # Create chart
            chart_id = self.create_chart(chart_config, dataset_id, database_id)
            if chart_id:
                chart_ids.append(chart_id)
            
            time.sleep(0.5)  # Rate limiting
        
        if not chart_ids:
            print(f"✗ No charts created for dashboard '{dashboard_config['dashboard_title']}'")
            return False
        
        # Create dashboard
        dashboard_id = self.create_dashboard(dashboard_config, chart_ids)
        
        return dashboard_id is not None


def main():
    """Main execution function"""
    print("=" * 60)
    print("Superset Dashboard Import")
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
    
    # Import dashboards
    success_count = 0
    for dashboard_file in DASHBOARD_FILES:
        if client.import_dashboard(dashboard_file, database_id):
            success_count += 1
        time.sleep(1)  # Rate limiting
    
    # Summary
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"✓ Successfully imported {success_count}/{len(DASHBOARD_FILES)} dashboards")
    print("\nNext steps:")
    print("  1. Navigate to Superset UI:", SUPERSET_URL)
    print("  2. Go to Dashboards to view imported dashboards")
    print("  3. Customize charts and layouts as needed")
    print("  4. Share dashboard URLs with stakeholders")
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
