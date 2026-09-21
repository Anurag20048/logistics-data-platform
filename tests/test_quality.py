from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

def test_generated_data_exists():
    assert (ROOT / "data/raw/customers.csv").exists()
    assert (ROOT / "data/raw/logistics_events.csv").exists()

def test_event_ids_unique():
    df = pd.read_csv(ROOT / "data/raw/logistics_events.csv")
    assert df["event_id"].is_unique

def test_customer_referential_integrity():
    c = pd.read_csv(ROOT / "data/raw/customers.csv")
    e = pd.read_csv(ROOT / "data/raw/logistics_events.csv")
    assert set(e.customer_id).issubset(set(c.customer_id))

def test_business_ranges():
    df = pd.read_csv(ROOT / "data/raw/logistics_events.csv")
    assert df["delay_minutes"].between(0, 180).all()
    assert (df["distance_km"] > 0).all()
