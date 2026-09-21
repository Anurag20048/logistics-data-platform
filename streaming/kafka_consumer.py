import json, os
from kafka import KafkaConsumer

BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "logistics-events")

consumer = KafkaConsumer(
    TOPIC, bootstrap_servers=BROKER, group_id="logistics-quality-consumer",
    auto_offset_reset="earliest", enable_auto_commit=True,
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

for msg in consumer:
    event = msg.value
    if not event.get("event_id"):
        continue
    print(event)
