#!/usr/bin/env python3
"""
Superset Full Setup Script
===========================

Provisions the Gold Layer database connection, datasets, and imports dashboards.
Run this once after Superset starts.

Usage:
    python superset/setup_superset.py
"""

import requests
import json
import os
import sys
import time

SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"

# MinIO credentials (match .env)
MINIO_USER = os.environ.get("MINIO_ROOT_USER", "admin")
MINIO_PASS = os.environ.get("MINIO_ROOT_PASSWORD", "changeme123")

DATASETS = [
    {
        "name": "pipeline_runs",
        "sql": "SELECT * FROM read_parquet('s3://gold/pipeline_runs/*.parquet')",
        "description": "Pipeline run metadata including status, duration, and row counts",
    },
    {
        "name": "order_metrics",
        "sql": "SELECT * FROM read_parquet('s3://gold/order_metrics/*.parquet')",
        "description": "Daily order aggregations including revenue and customer counts",
    },
    {
        "name": "customer_lifetime_value",
        "sql": "SELECT * FROM read_parquet('s3://gold/customer_lifetime_value/*.parquet')",
        "description": "Customer metrics including total orders, spend, and recency",
    },
    {
        "name": "dq_results",
        "sql": "SELECT * FROM read_parquet('s3://gold/dq_results/*.parquet')",
        "description": "Data quality check results for all pipeline layers",
    },
]

DASHBOARD_FILES = [
    os.path.join(os.path.dirname(__file__), "dashboards", "pipeline_metrics.json"),
    os.path.join(os.path.dirname(__file__), "dashboards", "business_kpis.json"),
]


class SupersetClient:
    def __init__(self):
        self.session = requests.Session()

    def login(self):
        resp = self.session.post(
            f"{SUPERSET_URL}/api/v1/security/login",
            json={"username": USERNAME, "password": PASSWORD, "provider": "db", "refresh": True},
            timeout=15,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"Login failed: {resp.text}")
        token = resp.json()["access_token"]
        self.session.headers.update({
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        })
        # Get CSRF token
        resp2 = self.session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/", timeout=10)
        if resp2.status_code == 200:
            self.session.headers["X-CSRFToken"] = resp2.json().get("result")
        print("✓ Logged in to Superset")

    def get_or_create_database(self):
        """Create the Gold Layer DuckDB database connection."""
        resp = self.session.get(f"{SUPERSET_URL}/api/v1/database/", timeout=10)
        databases = resp.json().get("result", [])
        for db in databases:
            if db.get("database_name") == "Gold Layer (DuckDB)":
                print(f"✓ Database already exists (ID: {db['id']})")
                return db["id"]

        # Create it
        payload = {
            "database_name": "Gold Layer (DuckDB)",
            "sqlalchemy_uri": "duckdb:////tmp/gold_layer.db",
            "expose_in_sqllab": True,
            "allow_run_async": False,
            "allow_ctas": True,
            "allow_cvas": True,
            "allow_dml": True,
            "cache_timeout": None,
            "extra": json.dumps({
                "engine_params": {
                    "connect_args": {
                        "read_only": False,
                        "config": {
                            "s3_endpoint": "minio:9000",
                            "s3_access_key_id": MINIO_USER,
                            "s3_secret_access_key": MINIO_PASS,
                            "s3_use_ssl": "false",
                            "s3_url_style": "path",
                        }
                    }
                }
            }),
        }
        resp = self.session.post(f"{SUPERSET_URL}/api/v1/database/", json=payload, timeout=30)
        if resp.status_code == 201:
            db_id = resp.json()["id"]
            print(f"✓ Created database 'Gold Layer (DuckDB)' (ID: {db_id})")
            return db_id
        else:
            raise RuntimeError(f"Failed to create database: {resp.text}")

    def get_or_create_dataset(self, db_id, ds_config):
        """Create a virtual dataset (SQL-based)."""
        name = ds_config["name"]
        # Check if exists
        resp = self.session.get(
            f"{SUPERSET_URL}/api/v1/dataset/",
            params={"q": json.dumps({"filters": [{"col": "table_name", "opr": "eq", "value": name}]})},
            timeout=10,
        )
        existing = resp.json().get("result", [])
        if existing:
            ds_id = existing[0]["id"]
            # Update cache_timeout if not set
            if existing[0].get("cache_timeout") is None:
                self.session.put(
                    f"{SUPERSET_URL}/api/v1/dataset/{ds_id}",
                    json={"cache_timeout": 300},
                    timeout=10,
                )
                print(f"  ✓ Updated cache_timeout=300s for '{name}'")
            print(f"✓ Dataset '{name}' already exists (ID: {ds_id})")
            return ds_id

        payload = {
            "database": db_id,
            "schema": "",
            "table_name": name,
            "sql": ds_config["sql"],
        }
        resp = self.session.post(f"{SUPERSET_URL}/api/v1/dataset/", json=payload, timeout=30)
        if resp.status_code == 201:
            ds_id = resp.json()["id"]
            print(f"✓ Created dataset '{name}' (ID: {ds_id})")
            # Set cache_timeout via PUT (Superset 3.x separates creation from metadata)
            self.session.put(
                f"{SUPERSET_URL}/api/v1/dataset/{ds_id}",
                json={"cache_timeout": 300},
                timeout=10,
            )
            return ds_id
        else:
            print(f"✗ Failed to create dataset '{name}': {resp.text}")
            return None

    def get_dataset_id(self, name):
        resp = self.session.get(
            f"{SUPERSET_URL}/api/v1/dataset/",
            params={"q": json.dumps({"filters": [{"col": "table_name", "opr": "eq", "value": name}]})},
            timeout=10,
        )
        existing = resp.json().get("result", [])
        return existing[0]["id"] if existing else None

    def get_or_create_chart(self, chart_config, dataset_id):
        name = chart_config["slice_name"]
        resp = self.session.get(
            f"{SUPERSET_URL}/api/v1/chart/",
            params={"q": json.dumps({"filters": [{"col": "slice_name", "opr": "eq", "value": name}]})},
            timeout=10,
        )
        existing = resp.json().get("result", [])
        if existing:
            print(f"  ✓ Chart '{name}' already exists (ID: {existing[0]['id']})")
            return existing[0]["id"]

        payload = {
            "slice_name": name,
            "viz_type": chart_config["viz_type"],
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "params": json.dumps(chart_config["params"]),
            "description": chart_config.get("description", ""),
            "cache_timeout": 300,
        }
        resp = self.session.post(f"{SUPERSET_URL}/api/v1/chart/", json=payload, timeout=30)
        if resp.status_code == 201:
            chart_id = resp.json()["id"]
            print(f"  ✓ Created chart '{name}' (ID: {chart_id})")
            return chart_id
        else:
            print(f"  ✗ Failed to create chart '{name}': {resp.text[:200]}")
            return None

    def get_or_create_dashboard(self, title, slug, chart_ids):
        resp = self.session.get(
            f"{SUPERSET_URL}/api/v1/dashboard/",
            params={"q": json.dumps({"filters": [{"col": "dashboard_title", "opr": "eq", "value": title}]})},
            timeout=10,
        )
        existing = resp.json().get("result", [])
        if existing:
            print(f"✓ Dashboard '{title}' already exists (ID: {existing[0]['id']})")
            return existing[0]["id"]

        payload = {
            "dashboard_title": title,
            "slug": slug,
            "published": True,
            "position_json": "{}",
            "json_metadata": json.dumps({
                "color_scheme": "supersetColors",
                "refresh_frequency": 300,
            }),
        }
        resp = self.session.post(f"{SUPERSET_URL}/api/v1/dashboard/", json=payload, timeout=30)
        if resp.status_code == 201:
            dash_id = resp.json()["id"]
            print(f"✓ Created dashboard '{title}' (ID: {dash_id})")
            # Attach charts
            if chart_ids:
                self.session.put(
                    f"{SUPERSET_URL}/api/v1/dashboard/{dash_id}",
                    json={"slices": chart_ids},
                    timeout=15,
                )
                print(f"  ✓ Attached {len(chart_ids)} charts")
            return dash_id
        else:
            print(f"✗ Failed to create dashboard '{title}': {resp.text[:200]}")
            return None

    def import_dashboard_from_file(self, filepath, db_id):
        with open(filepath) as f:
            config = json.load(f)

        title = config["dashboard_title"]
        slug = config.get("slug", title.lower().replace(" ", "-"))
        print(f"\nImporting dashboard: {title}")

        chart_ids = []
        for chart_config in config.get("slices", []):
            ds_name = chart_config["datasource_name"]
            ds_id = self.get_dataset_id(ds_name)
            if not ds_id:
                print(f"  ✗ Dataset '{ds_name}' not found, skipping chart")
                continue
            chart_id = self.get_or_create_chart(chart_config, ds_id)
            if chart_id:
                chart_ids.append(chart_id)
            time.sleep(0.3)

        self.get_or_create_dashboard(title, slug, chart_ids)


def main():
    print("=" * 60)
    print("Superset Full Setup")
    print("=" * 60)

    client = SupersetClient()
    client.login()

    print("\n[1] Database Connection")
    db_id = client.get_or_create_database()

    print("\n[2] Datasets")
    for ds in DATASETS:
        client.get_or_create_dataset(db_id, ds)
        time.sleep(0.5)

    print("\n[3] Dashboards")
    for filepath in DASHBOARD_FILES:
        if os.path.exists(filepath):
            client.import_dashboard_from_file(filepath, db_id)
        else:
            print(f"✗ Dashboard file not found: {filepath}")

    print("\n" + "=" * 60)
    print("✓ Setup complete. Run test_dashboard_functionality.py to verify.")
    print("=" * 60)


if __name__ == "__main__":
    main()
