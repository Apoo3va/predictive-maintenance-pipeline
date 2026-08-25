CREATE TABLE IF NOT EXISTS raw_telemetry (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    temperature DOUBLE PRECISION,
    vibration DOUBLE PRECISION,
    pressure DOUBLE PRECISION,
    rpm DOUBLE PRECISION,
    is_anomaly BOOLEAN,
    alert_level VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS sensor_alerts (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    anomaly_score DOUBLE PRECISION,
    is_anomaly BOOLEAN,
    alert_level VARCHAR(20)
);

CREATE TABLE IF NOT EXISTS device_daily_kpis (
    device_id VARCHAR(50),
    metric_date DATE,
    avg_temperature DOUBLE PRECISION,
    max_vibration DOUBLE PRECISION,
    total_anomalies INT,
    health_index DOUBLE PRECISION,
    PRIMARY KEY (device_id, metric_date)
);

CREATE INDEX IF NOT EXISTS idx_telemetry_device_time ON raw_telemetry(device_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_alerts_device_time ON sensor_alerts(device_id, timestamp);