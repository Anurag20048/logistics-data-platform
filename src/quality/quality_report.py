from pathlib import Path
from datetime import datetime, timezone
import json
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
RAW = ROOT / "data" / "raw"
REPORT_DIR = ROOT / "data" / "quality"
REPORT_DIR.mkdir(parents=True, exist_ok=True)

def build_report():
    customers = pd.read_csv(RAW / "customers.csv")
    events = pd.read_csv(RAW / "logistics_events.csv")
    parsed_ts = pd.to_datetime(events["event_ts"], errors="coerce")

    checks = {
        "customers": len(customers),
        "events": len(events),
        "duplicate_customer_ids": int(customers["customer_id"].duplicated().sum()),
        "duplicate_event_ids": int(events["event_id"].duplicated().sum()),
        "null_event_ids": int(events["event_id"].isna().sum()),
        "null_customer_ids": int(events["customer_id"].isna().sum()),
        "orphan_customer_ids": int((~events["customer_id"].isin(set(customers["customer_id"]))).sum()),
        "invalid_timestamps": int(parsed_ts.isna().sum()),
        "negative_delay": int((events["delay_minutes"] < 0).sum()),
        "distance_out_of_range": int((events["distance_km"] <= 0).sum()),
        "delay_out_of_range": int((events["delay_minutes"] > 180).sum()),
    }
    checks["passed"] = all(v == 0 for k, v in checks.items()
                           if k not in {"customers", "events"})
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "quality_status": "PASS" if checks["passed"] else "FAIL",
        "checks": checks,
    }
    (REPORT_DIR / "latest_quality_report.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )
    return report

if __name__ == "__main__":
    print(json.dumps(build_report(), indent=2))
