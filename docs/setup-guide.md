# Setup Guide

## Phase 1 & 2 Complete! 🎉

You've successfully completed:
- ✅ Infrastructure setup (Docker Compose with 10+ services)
- ✅ Data ingestion configuration (3 source connectors)
- ✅ Bronze layer table schemas

## What's Been Created

### Infrastructure Files
- `docker-compose.yml` - Complete service orchestration
- `.env` - Environment configuration
- `docker/dbt/Dockerfile` - Custom dbt runner image

### Data Sources
- `data/products.csv` - 15 products across 3 categories
- `data/inventory_movements.json` - 12 inventory transactions
- `scripts/seed-data.sql` - PostgreSQL seed data (10 customers, 13 orders)

### Airbyte Connections
- `connections/weather_api.yaml` - Open-Meteo Weather API
- `connections/postgres_source.yaml` - PostgreSQL seed database
- `connections/file_source.yaml` - CSV/JSON file sources

### Airflow DAG
- `dags/medallion_pipeline.py` - Complete pipeline orchestration (with placeholders)

### Bronze Layer
- `scripts/create_bronze_tables.py` - Iceberg table creation script
- 5 Bronze tables: weather_data, customers, orders, products, inventory_movements

## Next Steps

### 1. Start the Infrastructure

```bash
# Start all services
docker compose up -d

# Wait for services to be healthy (5-10 minutes)
docker compose ps

# Check logs if needed
docker compose logs -f minio
docker compose logs -f iceberg-rest
docker compose logs -f airflow-webserver
```

### 2. Create Bronze Tables

```bash
# Run the Bronze table creation script
docker compose exec dbt python /scripts/create_bronze_tables.py
```

### 3. Access the UIs

- **MinIO Console**: http://localhost:9001
  - Username: `admin`
  - Password: (from your `.env` file)
  - Verify buckets: bronze, silver, gold, iceberg-warehouse

- **Iceberg Catalog**: http://localhost:8181/v1/config
  - Should return JSON configuration

- **Airflow**: http://localhost:8080
  - Username: `admin`
  - Password: `admin`
  - Check that `medallion_pipeline` DAG appears

- **Airbyte**: http://localhost:8000
  - Configure sources manually using the YAML files as reference

- **Superset**: http://localhost:8088
  - Username: `admin`
  - Password: `admin`

### 4. Configure Airbyte (Manual Step)

Airbyte connections need to be configured via the UI:

1. Go to http://localhost:8000
2. Create a new source for each:
   - **Weather API**: Use HTTP source connector
   - **PostgreSQL**: Connect to `postgres:5432`, database `seed_db`
   - **File Source**: Use File connector (mount `/data` volume)
3. Create S3 destination pointing to MinIO:
   - Endpoint: `http://minio:9000`
   - Bucket: `bronze`
   - Access Key: (from `.env`)
   - Secret Key: (from `.env`)
4. Create connections and note the connection IDs
5. Add connection IDs to Airflow Variables:
   - `weather_connection_id`
   - `postgres_connection_id`
   - `file_connection_id`

## Troubleshooting

### Services Not Starting
```bash
# Check service logs
docker compose logs <service-name>

# Restart a specific service
docker compose restart <service-name>

# Rebuild if needed
docker compose up -d --build
```

### MinIO Buckets Not Created
```bash
# Check minio_init logs
docker compose logs minio_init

# Manually create buckets
docker compose exec minio mc alias set local http://localhost:9000 admin <password>
docker compose exec minio mc mb local/bronze local/silver local/gold local/iceberg-warehouse
```

### Airflow DAG Not Appearing
```bash
# Check scheduler logs
docker compose logs airflow-scheduler

# Verify dags directory is mounted
docker compose exec airflow-webserver ls -la /opt/airflow/dags
```

### PostgreSQL Seed Data Not Loading
```bash
# Check postgres logs
docker compose logs postgres

# Manually run seed script
docker compose exec postgres psql -U postgres -d seed_db -f /docker-entrypoint-initdb.d/seed-data.sql
```

## What's Next?

**Phase 3: Silver Layer Transformations**
- Create dbt project structure
- Define Silver transformation models
- Add data quality tests
- Integrate with Airflow

Ready to continue? Let me know and we'll move on to Phase 3!
