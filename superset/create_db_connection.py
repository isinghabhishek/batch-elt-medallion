#!/usr/bin/env python3
"""Create the Gold Layer DuckDB database connection in Superset using the internal Python API."""
from superset import create_app
from superset.extensions import db as superset_db

app = create_app()
with app.app_context():
    from superset.models.core import Database
    existing = superset_db.session.query(Database).filter_by(database_name='Gold Layer (DuckDB)').first()
    if existing:
        print(f'Already exists, ID: {existing.id}')
    else:
        new_db = Database(
            database_name='Gold Layer (DuckDB)',
            sqlalchemy_uri='duckdb:////tmp/gold_layer.db',
            expose_in_sqllab=True,
            allow_run_async=False,
            allow_ctas=True,
            allow_cvas=True,
            allow_dml=True,
        )
        superset_db.session.add(new_db)
        superset_db.session.commit()
        print(f'Created database, ID: {new_db.id}')
