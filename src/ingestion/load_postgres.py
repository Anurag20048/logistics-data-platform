from pathlib import Path
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

ROOT = Path(__file__).resolve().parents[2]
df = pd.read_csv(ROOT / "data" / "raw" / "logistics_events.csv")

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    dbname=os.getenv("POSTGRES_DB", "logistics"),
    user=os.getenv("POSTGRES_USER", "logistics"),
    password=os.getenv("POSTGRES_PASSWORD", "change-me"),
)
cur = conn.cursor()
cur.execute(open(ROOT / "sql" / "warehouse_schema.sql", encoding="utf-8").read())
rows = [
    (r.event_id, r.shipment_id, r.truck_id, r.customer_id, r.event_type,
     r.status, int(r.delay_minutes), float(r.distance_km), r.event_ts)
    for r in df.itertuples(index=False)
]
execute_values(cur, """
insert into public.logistics_events
(event_id, shipment_id, truck_id, customer_id, event_type, status,
 delay_minutes, distance_km, event_ts)
values %s
on conflict (event_id) do nothing
""", rows)
conn.commit()
cur.close()
conn.close()
print(f"Loaded {len(rows)} logistics events into PostgreSQL.")
