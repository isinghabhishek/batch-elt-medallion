#!/bin/bash
# scripts/init-multiple-dbs.sh
# =============================
# PostgreSQL initialization script that creates multiple databases on first startup.
#
# Reads the POSTGRES_MULTIPLE_DATABASES environment variable (comma-separated list)
# and creates each database, granting full privileges to POSTGRES_USER.
# Also creates a dedicated 'airflow' user for the Airflow metadata database.
#
# Mounted into the PostgreSQL container at:
#   /docker-entrypoint-initdb.d/init-multiple-dbs.sh
#
# Databases created:
#   - airbyte  : Airbyte job history and connection configs
#   - airflow  : Airflow metadata (DAG runs, task states, logs)
#   - seed_db  : Sample source data for pipeline ingestion testing
#
# This script runs automatically on first container start (empty data volume).
# On subsequent starts it is skipped (PostgreSQL data directory already exists).

set -e
set -u

# Create multiple databases for different services
function create_database() {
    local database=$1
    echo "Creating database '$database'"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
        CREATE DATABASE $database;
        GRANT ALL PRIVILEGES ON DATABASE $database TO $POSTGRES_USER;
EOSQL
}

# Parse comma-separated database names from environment variable
if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
    echo "Multiple database creation requested: $POSTGRES_MULTIPLE_DATABASES"
    for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
        create_database $db
    done
    echo "Multiple databases created"
fi

# Create airflow user for Airflow database
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "airflow" <<-EOSQL
    CREATE USER airflow WITH PASSWORD 'airflow';
    GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
    GRANT ALL PRIVILEGES ON SCHEMA public TO airflow;
EOSQL

echo "Database initialization complete"
