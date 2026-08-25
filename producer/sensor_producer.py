"""
Simulates telemetry from 10 industrial turbines and publishes each
reading as a JSON event to the Kafka topic `sensor_telemetry`.

Run:
    python producer/sensor_producer.py
"""
import json
import time
import random
from datetime import datetime, timezone

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "sensor_telemetry"
DEVICE_IDS = [f"TURBINE_{i:03d}" for i in range(1, 11)]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def generate_telemetry() -> dict:
    device_id = random.choice(DEVICE_IDS)
    # Inject a sporadic anomaly ~5% of the time so downstream has
    # something real to detect and score.
    is_anomaly = random.random() < 0.05

    return {
        "device_id": device_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature": random.uniform(85, 115) if is_anomaly else random.uniform(60, 80),
        "vibration": random.uniform(4.5, 8.0) if is_anomaly else random.uniform(0.5, 2.5),
        "pressure": random.uniform(20, 35) if is_anomaly else random.uniform(40, 60),
        "rpm": random.uniform(3200, 4500) if is_anomaly else random.uniform(1500, 2500),
    }


if __name__ == "__main__":
    print(f"Publishing telemetry to '{TOPIC}' on {KAFKA_BOOTSTRAP_SERVERS} ... Ctrl+C to stop")
    try:
        while True:
            event = generate_telemetry()
            producer.send(TOPIC, value=event)
            print(event)
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\nStopping producer.")
    finally:
        producer.flush()
        producer.close()