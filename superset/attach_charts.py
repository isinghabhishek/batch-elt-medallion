#!/usr/bin/env python3
"""Attach charts to dashboards using Superset internal Python API."""
from superset import create_app
from superset.extensions import db as superset_db

app = create_app()
with app.app_context():
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice

    # Pipeline Metrics dashboard (ID 3) -> charts 1,2,3,4
    dash1 = superset_db.session.query(Dashboard).filter_by(id=3).first()
    if dash1:
        charts = superset_db.session.query(Slice).filter(Slice.id.in_([1, 2, 3, 4])).all()
        dash1.slices = charts
        superset_db.session.commit()
        print(f"✓ Attached {len(charts)} charts to 'Pipeline Metrics'")
    else:
        print("✗ Dashboard 'Pipeline Metrics' (ID 3) not found")

    # Business KPIs dashboard (ID 4) -> charts 5,6,7
    dash2 = superset_db.session.query(Dashboard).filter_by(id=4).first()
    if dash2:
        charts = superset_db.session.query(Slice).filter(Slice.id.in_([5, 6, 7])).all()
        dash2.slices = charts
        superset_db.session.commit()
        print(f"✓ Attached {len(charts)} charts to 'Business KPIs'")
    else:
        print("✗ Dashboard 'Business KPIs' (ID 4) not found")
