from pathlib import Path
import pandas as pd
from src.quality.quality_report import build_report

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw"

CUSTOMER_SCHEMA = ["customer_id", "customer_name", "region"]
EVENT_SCHEMA = [
    "event_id", "shipment_id", "truck_id", "customer_id", "origin",
    "destination", "event_type", "status", "delay_minutes",
    "distance_km", "event_ts"
]

def validate():
    customers = pd.read_csv(RAW / "customers.csv")
    events = pd.read_csv(RAW / "logistics_events.csv")
    errors = []

    if list(customers.columns) != CUSTOMER_SCHEMA:
        errors.append("customer schema mismatch")
    if list(events.columns) != EVENT_SCHEMA:
        errors.append("event schema mismatch")
    if customers["customer_id"].duplicated().any():
        errors.append("duplicate customer_id")
    if events["event_id"].duplicated().any():
        errors.append("duplicate event_id")
    if events["event_id"].isna().any():
        errors.append("null event_id")
    if events["customer_id"].isna().any():
        errors.append("null customer_id")
    if not events["customer_id"].isin(set(customers["customer_id"])).all():
        errors.append("orphan customer_id")
    if (events["delay_minutes"] < 0).any() or (events["delay_minutes"] > 180).any():
        errors.append("delay outside 0-180 minutes")
    if (events["distance_km"] <= 0).any():
        errors.append("invalid distance")
    if pd.to_datetime(events["event_ts"], errors="coerce").isna().any():
        errors.append("invalid event timestamp")

    if errors:
        for e in errors:
            print("DATA QUALITY ERROR:", e)
        raise SystemExit(1)

    report = build_report()
    print(f"DATA QUALITY: {report['quality_status']} | customers={len(customers)} events={len(events)}")

if __name__ == "__main__":
    validate()
