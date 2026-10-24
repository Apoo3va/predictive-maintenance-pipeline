"""
Daily batch job: summarises raw_telemetry into one row per device per day
in device_daily_kpis. Safe to re-run: existing rows are updated, not duplicated.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

AGGREGATE_SQL = """
INSERT INTO device_daily_kpis
    (device_id, metric_date, avg_temperature, max_vibration, total_anomalies, health_index)
SELECT
    device_id,
    timestamp::date AS metric_date,
    AVG(temperature) AS avg_temperature,
    MAX(vibration) AS max_vibration,
    COUNT(*) FILTER (WHERE is_anomaly) AS total_anomalies,
    GREATEST(0, 100 - 100.0 * COUNT(*) FILTER (WHERE is_anomaly) / COUNT(*)) AS health_index
FROM raw_telemetry
GROUP BY device_id, timestamp::date
ON CONFLICT (device_id, metric_date) DO UPDATE SET
    avg_temperature = EXCLUDED.avg_temperature,
    max_vibration = EXCLUDED.max_vibration,
    total_anomalies = EXCLUDED.total_anomalies,
    health_index = EXCLUDED.health_index;
"""

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="batch_aggregation",
    description="Roll up raw telemetry into daily device KPIs",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    aggregate_daily_kpis = SQLExecuteQueryOperator(
        task_id="aggregate_daily_kpis",
        conn_id="telemetry_db",
        sql=AGGREGATE_SQL,
    )