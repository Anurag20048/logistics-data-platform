from pathlib import Path
import random
from datetime import datetime, timedelta
import pandas as pd

random.seed(42)
ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

CUSTOMERS = 1000
EVENTS = 50000
start = datetime(2026, 1, 1)

regions = ["North", "South", "East", "West"]
cities = ["Delhi", "Mumbai", "Ahmedabad", "Pune", "Jaipur", "Bengaluru", "Surat", "Hyderabad"]
event_types = ["picked_up", "in_transit", "at_hub", "delivered", "delayed"]
statuses = ["on_time", "delayed", "at_hub"]

customers = pd.DataFrame({
    "customer_id": [f"C{i:05d}" for i in range(1, CUSTOMERS + 1)],
    "customer_name": [f"Customer {i}" for i in range(1, CUSTOMERS + 1)],
    "region": [random.choice(regions) for _ in range(CUSTOMERS)],
})

events = []
for i in range(1, EVENTS + 1):
    ts = start + timedelta(minutes=random.randint(0, 90 * 24 * 60))
    event_type = random.choice(event_types)
    delay = random.randint(1, 180) if event_type == "delayed" else random.randint(0, 45)
    events.append({
        "event_id": f"E{i:08d}",
        "shipment_id": f"S{random.randint(1, 8000):07d}",
        "truck_id": f"T{random.randint(1, 600):05d}",
        "customer_id": random.choice(customers["customer_id"].tolist()),
        "origin": random.choice(cities),
        "destination": random.choice(cities),
        "event_type": event_type,
        "status": "delayed" if event_type == "delayed" else random.choice(statuses),
        "delay_minutes": delay,
        "distance_km": round(random.uniform(5, 2500), 2),
        "event_ts": ts.isoformat(),
    })

pd.DataFrame(events).sort_values("event_ts").to_csv(RAW / "logistics_events.csv", index=False)
customers.to_csv(RAW / "customers.csv", index=False)
print(f"Generated {CUSTOMERS} customers and {EVENTS} logistics events.")
