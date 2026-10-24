from fastapi import FastAPI
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI(title="Predictive Maintenance API")

# Postgres is exposed on localhost:5432 by docker-compose
DB_CONFIG = {
    "host": "localhost",
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