import json
from datetime import datetime, timezone

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

now = datetime.now(timezone.utc).isoformat()

producer.send("sensor_telemetry", {
    "device_id": "TEST_BAD", "timestamp": now,
    "temperature": 999, "vibration": 1.0, "pressure": 50, "rpm": 2000,
})
producer.send("sensor_telemetry", {
    "device_id": "TEST_GOOD", "timestamp": now,
    "temperature": 70, "vibration": 1.0, "pressure": 50, "rpm": 2000,
})
producer.flush()
print("Sent TEST_BAD (temperature 999) and TEST_GOOD (temperature 70)")