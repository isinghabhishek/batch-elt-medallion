#!/usr/bin/env python3
"""
Create Superset datasets using the internal Python API (bypasses SQL validation).
Run inside the Superset container: python3 /app/create_datasets.py
"""
from superset import create_app
from superset.extensions import db as superset_db

DATASETS = [
    {
        "table_name": "pipeline_runs",
        "sql": "SELECT * FROM read_parquet('s3://gold/pipeline_runs/*.parquet')",
        "description": "Pipeline run metadata including status, duration, and row counts",
    },
    {
        "table_name": "order_metrics",
        "sql": "SELECT * FROM read_parquet('s3://gold/order_metrics/*.parquet')",
        "description": "Daily order aggregations including revenue and customer counts",
    },
    {
        "table_name": "customer_lifetime_value",
        "sql": "SELECT * FROM read_parquet('s3://gold/customer_lifetime_value/*.parquet')",
        "description": "Customer metrics including total orders, spend, and recency",
    },
    {
        "table_name": "dq_results",
        "sql": "SELECT * FROM read_parquet('s3://gold/dq_results/*.parquet')",
        "description": "Data quality check results for all pipeline layers",
    },
]

app = create_app()
with app.app_context():
    from superset.models.core import Database
    from superset.connectors.sqla.models import SqlaTable

    db_obj = superset_db.session.query(Database).filter_by(database_name='Gold Layer (DuckDB)').first()
    if not db_obj:
        print("ERROR: Database 'Gold Layer (DuckDB)' not found. Run create_db_connection.py first.")
        exit(1)

    print(f"Using database ID: {db_obj.id}")

    for ds in DATASETS:
        name = ds["table_name"]
        existing = superset_db.session.query(SqlaTable).filter_by(
            table_name=name, database_id=db_obj.id
        ).first()

        if existing:
            # Update cache_timeout
            existing.cache_timeout = 300
            superset_db.session.commit()
            print(f"✓ Dataset '{name}' already exists (ID: {existing.id}), updated cache_timeout=300s")
        else:
            new_ds = SqlaTable(
                table_name=name,
                sql=ds["sql"],
                database_id=db_obj.id,
                schema="",
                description=ds["description"],
                cache_timeout=300,
                is_sqllab_view=True,
            )
            superset_db.session.add(new_ds)
            superset_db.session.commit()
            print(f"✓ Created dataset '{name}' (ID: {new_ds.id}, cache_timeout=300s)")

    print("\nAll datasets created/verified.")
