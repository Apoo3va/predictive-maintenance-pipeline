"""
Simulated edge node: samples a 2 kHz accelerometer, computes DSP features
once per second, and publishes a compact JSON message to Kafka.
"""
import json
import random
import time
from datetime import datetime, timezone

import numpy as np
from kafka import KafkaProducer

from sensor_signal import simulate_vibration, extract_features

TOPIC = "sensor_features"
DEVICE_IDS = [f"TURBINE_{i:03d}" for i in range(1, 11)]

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
rng = np.random.default_rng()


def one_reading():
    device_id = random.choice(DEVICE_IDS)
    fault = random.random() < 0.05
    rpm = random.uniform(1500, 2500)
    feats = extract_features(simulate_vibration(rpm, fault=fault, rng=rng))
    temperature = random.uniform(85, 115) if fault else random.uniform(60, 80)
    return {
        "device_id": device_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "rpm": rpm,
        "temperature": temperature,
        **feats,
    }


if __name__ == "__main__":
    print(f"Publishing features to '{TOPIC}' ... Ctrl+C to stop")
    try:
        while True:
            msg = one_reading()
            producer.send(TOPIC, msg)
            print({k: (round(v, 2) if isinstance(v, float) else v) for k, v in msg.items()})
            time.sleep(0.5)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()