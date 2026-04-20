#!/bin/bash
# Manual verification script for Bronze layer testing (Task 3.8)
# Run each section step-by-step to verify the Bronze layer

set -e

echo "============================================================"
echo "Task 3.8: Manual Bronze Layer Verification"
echo "============================================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Load environment variables
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

echo "Step 1: Check Docker Services"
echo "------------------------------------------------------------"
docker compose ps
echo ""

echo "Step 2: Check MinIO Buckets"
echo "------------------------------------------------------------"
echo "Checking if bronze bucket exists..."
docker exec minio mc ls local/bronze/ || echo "Bronze bucket not found or empty"
echo ""

echo "Step 3: List Airbyte Connections"
echo "------------------------------------------------------------"
echo "Fetching Airbyte connections..."
curl -s http://localhost:8001/api/v1/connections 2>/dev/null | \
    python3 -c "import sys, json; data=json.load(sys.stdin); [print(f\"  - {c['name']}: {c['connectionId']}\") for c in data.get('connections', [])]" || \
    echo "Could not fetch connections. Is Airbyte running?"
echo ""

echo "Step 4: Check Iceberg Catalog"
echo "------------------------------------------------------------"
echo "Checking Iceberg REST Catalog..."
curl -s http://localhost:8181/v1/config | python3 -m json.tool || echo "Catalog not responding"
echo ""

echo "Step 5: List Bronze Namespace Tables"
echo "------------------------------------------------------------"
echo "Fetching tables in bronze namespace..."
curl -s http://localhost:8181/v1/namespaces/bronze/tables 2>/dev/null | python3 -m json.tool || \
    echo "Could not fetch tables. Bronze namespace may not exist yet."
echo ""

echo "============================================================"
echo "Manual Steps Required:"
echo "============================================================"
echo ""
echo "1. Open Airbyte UI: http://localhost:8000"
echo "   - Navigate to Connections"
echo "   - Click 'Sync Now' on a connection"
echo "   - Wait for sync to complete"
echo ""
echo "2. Open MinIO Console: http://localhost:9001"
echo "   - Login with: ${MINIO_ROOT_USER} / ${MINIO_ROOT_PASSWORD}"
echo "   - Browse to 'bronze' bucket"
echo "   - Verify Parquet files exist"
echo ""
echo "3. Query Bronze table with DuckDB:"
echo "   docker exec -it dbt duckdb :memory: << 'EOF'"
echo "   INSTALL iceberg;"
echo "   INSTALL httpfs;"
echo "   LOAD iceberg;"
echo "   LOAD httpfs;"
echo "   SET s3_endpoint='http://minio:9000';"
echo "   SET s3_access_key_id='${MINIO_ROOT_USER}';"
echo "   SET s3_secret_access_key='${MINIO_ROOT_PASSWORD}';"
echo "   SET s3_use_ssl=false;"
echo "   SET s3_url_style='path';"
echo "   SELECT COUNT(*) FROM iceberg_scan('s3://bronze/weather_data/');"
echo "   EOF"
echo ""
echo "============================================================"
