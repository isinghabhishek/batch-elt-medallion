# Airflow Configuration Guide

This document describes how to configure Airflow connections and variables for the Medallion Pipeline.

## Overview

The Medallion Pipeline DAG requires several Airflow connections and variables to be configured before it can run successfully. These configurations enable Airflow to:

1. Connect to Airbyte for triggering data extraction jobs
2. Access MinIO credentials for S3 operations
3. Reference Airbyte connection IDs for each data source

## Required Airflow Variables

Airflow Variables are key-value pairs used to store configuration values that can be referenced in DAGs.

### Setting Variables via Airflow UI

1. Navigate to **Admin > Variables** in the Airflow UI (http://localhost:8080)
2. Click **Add a new record** (+)
3. Add each of the following variables:

| Variable Key | Example Value | Description |
|-------------|---------------|-------------|
| `minio_root_user` | `minioadmin` | MinIO access key (should match `.env` file) |
| `minio_root_password` | `minioadmin` | MinIO secret key (should match `.env` file) |
| `weather_connection_id` | `<uuid>` | Airbyte connection ID for weather API source |
| `postgres_connection_id` | `<uuid>` | Airbyte connection ID for PostgreSQL source |
| `file_connection_id` | `<uuid>` | Airbyte connection ID for file source |

### Setting Variables via CLI

Alternatively, you can set variables using the Airflow CLI:

```bash
# Set MinIO credentials
docker exec -it airflow-webserver airflow variables set minio_root_user minioadmin
docker exec -it airflow-webserver airflow variables set minio_root_password minioadmin

# Set Airbyte connection IDs (replace with actual UUIDs from Airbyte)
docker exec -it airflow-webserver airflow variables set weather_connection_id <uuid>
docker exec -it airflow-webserver airflow variables set postgres_connection_id <uuid>
docker exec -it airflow-webserver airflow variables set file_connection_id <uuid>
```

### Setting Variables via Environment Variables

You can also set Airflow variables using environment variables with the prefix `AIRFLOW_VAR_`:

```bash
# In docker-compose.yml or .env file
AIRFLOW_VAR_MINIO_ROOT_USER=minioadmin
AIRFLOW_VAR_MINIO_ROOT_PASSWORD=minioadmin
AIRFLOW_VAR_WEATHER_CONNECTION_ID=<uuid>
AIRFLOW_VAR_POSTGRES_CONNECTION_ID=<uuid>
AIRFLOW_VAR_FILE_CONNECTION_ID=<uuid>
```

## Required Airflow Connections

Airflow Connections store credentials and connection details for external systems.

### 1. Airbyte Connection

**Connection ID**: `airbyte_default`

This connection allows Airflow to communicate with the Airbyte API to trigger sync jobs.

#### Setting via Airflow UI

1. Navigate to **Admin > Connections** in the Airflow UI
2. Click **Add a new record** (+)
3. Fill in the following details:

| Field | Value |
|-------|-------|
| Connection Id | `airbyte_default` |
| Connection Type | `HTTP` |
| Host | `airbyte-server` |
| Port | `8001` |
| Schema | `http` |

4. Click **Save**

#### Setting via CLI

```bash
docker exec -it airflow-webserver airflow connections add airbyte_default \
    --conn-type http \
    --conn-host airbyte-server \
    --conn-port 8001 \
    --conn-schema http
```

#### Setting via Environment Variable

```bash
# In docker-compose.yml or .env file
AIRFLOW_CONN_AIRBYTE_DEFAULT=http://airbyte-server:8001
```

## Getting Airbyte Connection IDs

Airbyte assigns a unique UUID to each connection (source + destination pair). You need to retrieve these IDs and configure them as Airflow variables.

### Method 1: Via Airbyte UI

1. Open Airbyte UI at http://localhost:8000
2. Navigate to **Connections**
3. Click on a connection (e.g., "Weather API → MinIO Bronze")
4. The connection ID is in the URL: `http://localhost:8000/connections/<connection-id>`
5. Copy the UUID and set it as an Airflow variable

### Method 2: Via Airbyte API

```bash
# List all connections
curl -X GET http://localhost:8001/api/v1/connections \
  -H "Content-Type: application/json"

# Response will include connection IDs:
# {
#   "connections": [
#     {
#       "connectionId": "12345678-1234-1234-1234-123456789012",
#       "name": "Weather API → MinIO Bronze",
#       ...
#     }
#   ]
# }
```

### Method 3: Create Connections Programmatically

You can create Airbyte connections using the Airbyte API and configuration files in the `connections/` directory:

```bash
# Example: Create weather API connection
curl -X POST http://localhost:8001/api/v1/connections \
  -H "Content-Type: application/json" \
  -d @connections/weather_api.yaml
```

## Verification

After configuring all connections and variables, verify the setup:

### 1. Check Variables

```bash
# List all variables
docker exec -it airflow-webserver airflow variables list

# Get specific variable
docker exec -it airflow-webserver airflow variables get minio_root_user
```

### 2. Check Connections

```bash
# List all connections
docker exec -it airflow-webserver airflow connections list

# Test Airbyte connection
docker exec -it airflow-webserver airflow connections test airbyte_default
```

### 3. Test DAG

1. Navigate to the Airflow UI at http://localhost:8080
2. Find the `medallion_pipeline` DAG
3. Click **Trigger DAG** (play button)
4. Monitor task execution in the **Graph** or **Grid** view
5. Check task logs for any configuration errors

## Troubleshooting

### Issue: "Variable does not exist"

**Solution**: Ensure all required variables are set. Check variable names for typos (they are case-sensitive).

```bash
# Verify variable exists
docker exec -it airflow-webserver airflow variables get minio_root_user
```

### Issue: "Connection does not exist"

**Solution**: Ensure the `airbyte_default` connection is created.

```bash
# List connections
docker exec -it airflow-webserver airflow connections list | grep airbyte
```

### Issue: "Cannot connect to Airbyte"

**Solution**: Verify Airbyte services are running and accessible.

```bash
# Check Airbyte server health
curl http://localhost:8001/api/v1/health

# Check Docker network connectivity
docker exec -it airflow-webserver ping airbyte-server
```

### Issue: "Airbyte connection ID not found"

**Solution**: Verify the connection exists in Airbyte and the UUID is correct.

```bash
# List Airbyte connections
curl http://localhost:8001/api/v1/connections
```

## Security Best Practices

1. **Use Airflow Secrets Backend**: For production, use a secrets backend (e.g., AWS Secrets Manager, HashiCorp Vault) instead of storing credentials in variables.

2. **Encrypt Variables**: Enable Airflow's Fernet key encryption for sensitive variables:
   ```bash
   # Generate Fernet key
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   
   # Set in airflow.cfg or environment variable
   AIRFLOW__CORE__FERNET_KEY=<generated-key>
   ```

3. **Restrict Access**: Use Airflow RBAC to limit who can view/edit connections and variables.

4. **Rotate Credentials**: Regularly rotate MinIO and Airbyte credentials.

## Next Steps

After completing this configuration:

1. ✅ All Airflow variables are set
2. ✅ Airbyte connection is configured
3. ✅ Airbyte connection IDs are retrieved and set as variables
4. ✅ Configuration is verified

You can now trigger the `medallion_pipeline` DAG and monitor its execution through the Airflow UI.

## Reference

- [Airflow Variables Documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html)
- [Airflow Connections Documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
- [Airbyte API Documentation](https://docs.airbyte.com/api-documentation)
