#!/bin/bash
# Superset Initialization Script
# This script runs on Superset container startup to:
# 1. Initialize the database
# 2. Create admin user
# 3. Install required dependencies
# 4. Import datasets and dashboards

set -e

echo "=========================================="
echo "Superset Initialization"
echo "=========================================="

# Install DuckDB driver
echo "Installing DuckDB SQLAlchemy driver..."
pip install duckdb-engine --quiet
echo "✓ DuckDB driver installed"

# Upgrade Superset database
echo "Upgrading Superset database..."
superset db upgrade

# Create admin user (ignore if already exists)
echo "Creating admin user..."
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password admin || echo "Admin user already exists"

# Initialize Superset
echo "Initializing Superset..."
superset init

echo "✓ Superset initialization complete"

# Wait for Superset to be fully ready
echo "Waiting for Superset to be ready..."
sleep 10

# Import datasets (if script exists)
if [ -f /app/dashboards/init_datasets.py ]; then
    echo "Importing datasets..."
    python /app/dashboards/init_datasets.py || echo "Dataset import failed (may need manual setup)"
fi

# Set cache TTL on datasets via internal API (bypasses dialect load issue)
if [ -f /app/create_datasets.py ]; then
    echo "Setting cache TTL on datasets..."
    python /app/create_datasets.py || echo "Cache TTL setup failed"
fi

# Import dashboards (if script exists)
if [ -f /app/dashboards/import_dashboards.py ]; then
    echo "Importing dashboards..."
    python /app/dashboards/import_dashboards.py || echo "Dashboard import failed (may need manual setup)"
fi

echo "=========================================="
echo "Superset is ready!"
echo "Access at: http://localhost:8088"
echo "Username: admin"
echo "Password: admin"
echo "=========================================="

# Start Superset
exec gunicorn \
  --bind 0.0.0.0:8088 \
  --workers 4 \
  --timeout 120 \
  --limit-request-line 0 \
  --limit-request-field_size 0 \
  'superset.app:create_app()'
