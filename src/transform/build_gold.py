from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw"
GOLD = ROOT / "data" / "gold"
GOLD.mkdir(parents=True, exist_ok=True)

customers = pd.read_csv(RAW / "customers.csv")
events = pd.read_csv(RAW / "logistics_events.csv", parse_dates=["event_ts"])

shipment = (
    events.groupby("shipment_id")
    .agg(
        customer_id=("customer_id", "first"),
        truck_id=("truck_id", "nunique"),
        event_count=("event_id", "count"),
        total_distance_km=("distance_km", "max"),
        max_delay_minutes=("delay_minutes", "max"),
        delayed_events=("status", lambda s: (s == "delayed").sum()),
        first_event_ts=("event_ts", "min"),
        last_event_ts=("event_ts", "max"),
    )
    .reset_index()
)
shipment["transit_hours"] = (
    (shipment["last_event_ts"] - shipment["first_event_ts"]).dt.total_seconds() / 3600
)

gold = (
    events.groupby("customer_id")
    .agg(
        event_count=("event_id", "count"),
        shipment_count=("shipment_id", "nunique"),
        truck_count=("truck_id", "nunique"),
        total_distance_km=("distance_km", "sum"),
        avg_delay_minutes=("delay_minutes", "mean"),
        max_delay_minutes=("delay_minutes", "max"),
        delayed_events=("status", lambda s: (s == "delayed").sum()),
        delivered_events=("event_type", lambda s: (s == "delivered").sum()),
        last_event_ts=("event_ts", "max"),
    )
    .reset_index()
    .merge(customers, on="customer_id", how="left")
)
gold["delay_rate"] = (gold["delayed_events"] / gold["event_count"]).round(4)
gold["delivery_rate"] = (gold["delivered_events"] / gold["event_count"]).round(4)

gold.to_csv(GOLD / "customer_logistics_360.csv", index=False)
shipment.to_csv(GOLD / "shipment_performance.csv", index=False)
print(f"Built Customer Logistics 360: {len(gold)} rows")
print(f"Built Shipment Performance: {len(shipment)} rows")
