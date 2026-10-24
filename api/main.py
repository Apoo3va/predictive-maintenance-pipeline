from fastapi import FastAPI, Query
from typing import Optional
import os
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(title="Predictive Maintenance API")

# Postgres is exposed on localhost:5432 by docker-compose
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": 5432,
    "dbname": "telemetry_db",
    "user": "postgres",
    "password": "postgres",
}


def run_query(sql, params=None):
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/machines")
def list_machines():
    """Latest daily health summary for every turbine."""
    return run_query(
        """
        SELECT DISTINCT ON (device_id)
            device_id, metric_date, avg_temperature,
            max_vibration, total_anomalies, health_index
        FROM device_daily_kpis
        ORDER BY device_id, metric_date DESC
        """
    )
@app.get("/alerts")
def list_alerts(
    device_id: Optional[str] = None,
    alert_level: Optional[str] = Query(None, description="WARNING or CRITICAL"),
    limit: int = Query(20, ge=1, le=200),
):
    """Most recent alerts, newest first. Optionally filter by turbine or severity."""
    sql = """
        SELECT device_id, timestamp, round(anomaly_score::numeric, 2) AS anomaly_score, alert_level
        FROM sensor_alerts
        WHERE (%(device_id)s IS NULL OR device_id = %(device_id)s)
          AND (%(alert_level)s IS NULL OR alert_level = %(alert_level)s)
        ORDER BY timestamp DESC
        LIMIT %(limit)s
    """
    return run_query(
        sql,
        {"device_id": device_id, "alert_level": alert_level, "limit": limit},
    )