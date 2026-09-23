from __future__ import annotations
import json, os
import sqlalchemy as sa

db=os.environ["DATABASE_URL"]
engine=sa.create_engine(db,pool_pre_ping=True)
with engine.connect() as conn:
    rows=conn.execute(sa.text("""
      SELECT r.id,r.wo,r.contractor_name,r.contractor_id,r.updated_at,c.pod_color
      FROM routes r
      LEFT JOIN contractors c ON c.id=r.contractor_id
      WHERE r.status::text='accepted'
        AND lower(coalesce(c.pod_color,r.payload->>'pod',r.payload->>'pod_name',''))='orange'
      ORDER BY r.updated_at DESC
      LIMIT 10
    """)).mappings().all()
print("ORANGE_ACCEPTED="+json.dumps([dict(x) for x in rows],default=str,separators=(",",":")),flush=True)
