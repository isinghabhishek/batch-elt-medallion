{#
  init_iceberg()
  ==============
  Loads the DuckDB extensions required to read/write Parquet files on MinIO
  and configures the S3 connection settings.

  Usage:
    Call this macro at the top of any model that reads from or writes to S3:

      {% if execute %}
        {{ init_iceberg() }}
      {% endif %}

    The `{% if execute %}` guard prevents the macro from running during
    dbt's compilation phase (e.g., `dbt compile`, `dbt docs generate`),
    which would fail because there is no active DuckDB connection.

  Environment Variables Required:
    MINIO_ROOT_USER     — MinIO access key (set in .env)
    MINIO_ROOT_PASSWORD — MinIO secret key (set in .env)

  Notes:
    - DuckDB 0.10.0 does not support Iceberg REST catalog secrets.
      Models read directly from S3 paths using read_parquet() instead.
    - s3_url_style='path' is required for MinIO (path-style addressing).
    - s3_use_ssl=false because MinIO runs over plain HTTP locally.
#}
{% macro init_iceberg() %}
{#
  init_iceberg()
  ==============
  Initializes DuckDB extensions and S3 connection settings required to read
  Parquet files from MinIO (S3-compatible object storage).

  Called at the top of every Silver and Gold model via:
    {% if execute %}{{ init_iceberg() }}{% endif %}

  Loads:
    - httpfs  : Enables reading/writing files over HTTP/S3
    - iceberg : Enables reading Apache Iceberg table format

  Configures:
    - s3_endpoint        : MinIO API endpoint (minio:9000 inside Docker network)
    - s3_access_key_id   : From MINIO_ROOT_USER env var
    - s3_secret_access_key: From MINIO_ROOT_PASSWORD env var
    - s3_use_ssl         : false (local dev, no TLS)
    - s3_url_style       : path (required for MinIO path-style access)

  Note: DuckDB 0.10.0 does not support Iceberg REST catalog secrets.
        Models read directly from S3 Parquet files using read_parquet().
#}
  {% set init_sql %}
    -- Install and load required extensions
    INSTALL iceberg;
    INSTALL httpfs;
    LOAD iceberg;
    LOAD httpfs;

    -- Configure S3 connection settings for MinIO.
    -- All containers reach MinIO at http://minio:9000 on the Docker network.
    SET s3_endpoint='minio:9000';
    SET s3_access_key_id='{{ env_var("MINIO_ROOT_USER") }}';
    SET s3_secret_access_key='{{ env_var("MINIO_ROOT_PASSWORD") }}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
  {% endset %}

  {% do run_query(init_sql) %}
  {{ log("Iceberg extensions loaded and S3 configured", info=True) }}
{% endmacro %}
