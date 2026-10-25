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
CREATE INDEX IF NOT EXISTS idx_telemetry_ts_brin ON raw_telemetry USING BRIN (timestamp);
CREATE TABLE IF NOT EXISTS device_rolling_features (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    avg_temperature DOUBLE PRECISION,
    std_temperature DOUBLE PRECISION,
    avg_vibration DOUBLE PRECISION,
    std_vibration DOUBLE PRECISION,
    reading_count INT
);
CREATE TABLE IF NOT EXISTS vibration_features (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    rpm DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    rms DOUBLE PRECISION,
    peak DOUBLE PRECISION,
    crest_factor DOUBLE PRECISION,
    kurtosis DOUBLE PRECISION,
    dominant_hz DOUBLE PRECISION,
    band_energy DOUBLE PRECISION,
    anomaly_score DOUBLE PRECISION,
    model_anomaly BOOLEAN,
    is_fault BOOLEAN
);
CREATE INDEX IF NOT EXISTS idx_vib_device_time ON vibration_features(device_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_vib_device_time ON vibration_features(device_id, timestamp);