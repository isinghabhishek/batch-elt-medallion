# docker/dbt/Dockerfile
# ====================
# Custom dbt runner image for the Medallion Pipeline.
#
# Installs dbt-duckdb (v1.7.0) and DuckDB (v0.10.0) on a slim Python 3.11 base.
# The dbt project directory is mounted as a volume at runtime (./dbt:/dbt),
# so no source files are baked into the image — changes take effect immediately.
#
# Build  : docker compose build dbt
# Run    : docker compose up -d dbt
# Execute: docker exec dbt dbt run --project-dir /dbt

FROM python:3.11-slim


# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install dbt and DuckDB
RUN pip install --no-cache-dir \
    dbt-duckdb==1.7.0 \
    duckdb==0.10.0

# Set working directory
WORKDIR /dbt

# Copy dbt project files (will be mounted as volume in docker-compose)
# COPY dbt/ /dbt/

# Default command (overridden in docker-compose to keep container running)
CMD ["dbt", "run"]
