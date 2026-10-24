# Real-Time IoT Telemetry & Predictive Anomaly Pipeline

A real-time IoT data platform that simulates industrial turbine telemetry, detects anomalies as the data streams in, builds daily health KPIs, and serves the results through a REST API.

## Architecture

```
Sensor simulator (Python)
        |
        v
   Apache Kafka  (topic: sensor_telemetry)
        |
        v
Spark Structured Streaming  --- rule-based anomaly scoring
        |
        v
   PostgreSQL  <---------- Apache Airflow (daily KPI rollup)
 raw_telemetry
 sensor_alerts
 device_daily_kpis
        ^
        |
  FastAPI service  --->  /machines  /alerts  /health
```

## Tech stack

| Layer | Tool |
|---|---|
| Streaming ingestion | Apache Kafka + Zookeeper |
| Stream processing | PySpark Structured Streaming |
| Storage | PostgreSQL 15 |
| Orchestration | Apache Airflow 2.10 |
| API | FastAPI + Uvicorn |
| Infrastructure | Docker Compose |

## How it works

1. `producer/sensor_producer.py` publishes readings (temperature, vibration, pressure, rpm) for 10 turbines to Kafka. About 5% are injected faults.
2. `spark/streaming_pipeline.py` reads the topic, flags anomalies (temperature > 85 or vibration > 4), assigns a `WARNING` or `CRITICAL` level, and writes to Postgres.
3. `airflow/dags/batch_aggregation_dag.py` runs daily and builds one health summary per turbine per day. It is idempotent, so re-runs update rows instead of duplicating them.
4. `api/main.py` serves the results over HTTP.

## Run it locally

Requires Docker Desktop and Python 3.11+.

```bash
# 1. Start Kafka, Zookeeper, Postgres, Spark and Airflow
docker compose up -d

# 2. Create the Airflow metadata database (first time only)
docker exec -i <postgres-container> psql -U postgres -c "CREATE DATABASE airflow_db;"

# 3. Start the Spark streaming job
docker exec -it <spark-container> /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2,org.postgresql:postgresql:42.6.0 \
  /spark-jobs/streaming_pipeline.py

# 4. Generate sensor data (new terminal)
python -m venv venv && venv\Scripts\activate
pip install -r requirements.txt
python producer/sensor_producer.py

# 5. Trigger the Airflow DAG at http://localhost:8080 (batch_aggregation)

# 6. Start the API
uvicorn api.main:app --reload
```

Then open http://localhost:8000/docs.

## API endpoints

| Endpoint | Description |
|---|---|
| `GET /health` | Service health check |
| `GET /machines` | Latest daily health summary for each turbine |
| `GET /alerts?device_id=&alert_level=&limit=` | Recent alerts, newest first, with optional filters |

## Design decisions

- **Two Kafka listeners** so the host machine (`localhost:9092`) and containers (`kafka:29092`) can both connect.
- **Idempotent batch job** using `ON CONFLICT DO UPDATE`, so Airflow retries are safe.
- **Parameterised SQL** in the API to prevent SQL injection.
- **Rule-based scoring** keeps the pipeline simple and testable. The scoring step can be swapped for a trained model such as Isolation Forest.

## Possible next steps

- Replace thresholds with a trained anomaly-detection model
- Add a dashboard on top of the API
- Add tests and a CI workflow