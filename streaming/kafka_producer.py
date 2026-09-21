import json, os, time, random
from datetime import datetime, timezone
from kafka import KafkaProducer

BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "logistics-events")
COUNT = int(os.getenv("EVENT_COUNT", "100"))

producer = KafkaProducer(
    bootstrap_servers=BROKER,
    acks="all",
    retries=5,
    compression_type="gzip",
    linger_ms=10,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

for i in range(COUNT):
    event = {
        "event_id": f"STREAM-{int(time.time()*1000)}-{i}",
        "shipment_id": f"S{random.randint(1,8000):07d}",
        "truck_id": f"T{random.randint(1,600):05d}",
        "customer_id": f"C{random.randint(1,1000):05d}",
        "event_type": random.choice(["picked_up","in_transit","at_hub","delivered","delayed"]),
        "delay_minutes": random.randint(0, 180),
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }
    producer.send(TOPIC, key=event["shipment_id"].encode(), value=event)
    time.sleep(0.05)

producer.flush()
producer.close()
print(f"Published {COUNT} events to {TOPIC}.")
