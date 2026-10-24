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
docker exec -i predictive-maintenance-pipeline-postgres-1 psql -U postgres -c "CREATE DATABASE airflow_db;"

# 3. Start the Spark streaming job (leave this terminal running)
docker exec -it predictive-maintenance-pipeline-spark-1 /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2,org.postgresql:postgresql:42.6.0 \
  /spark-jobs/streaming_pipeline.py

# 4. Generate sensor data (new terminal)
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
python producer/sensor_producer.py

# 5. Get the Airflow admin password, then open http://localhost:8080
#    Username: admin. Trigger the "batch_aggregation" DAG.
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt

# 6. Start the API (new terminal)
uvicorn api.main:app --reload
```

Then open http://localhost:8000/docs.

To shut down cleanly, run `docker compose stop`. This lets Kafka unregister from Zookeeper, which avoids a startup crash next time.

## API endpoints

| Endpoint | Description |
|---|---|
| `GET /health` | Service health check |
| `GET /machines` | Latest daily health summary for each turbine |
| `GET /alerts?device_id=&alert_level=&limit=` | Recent alerts, newest first, with optional filters |

## Screenshots

**Alerts stored in PostgreSQL**

![Postgres alerts](images/postgres-alerts.png)

**FastAPI interactive docs**

![API docs](images/docs.png)

**Airflow DAG run**

![Airflow DAG](images/dag.png)

## Design decisions

- **Two Kafka listeners** so the host machine (`localhost:9092`) and containers (`kafka:29092`) can both connect.
- **Idempotent batch job** using `ON CONFLICT DO UPDATE`, so Airflow retries are safe.
- **Parameterised SQL** in the API to prevent SQL injection.
- **Rule-based scoring** keeps the pipeline simple and testable. The scoring step can be swapped for a trained model such as Isolation Forest.

## Possible next steps

- Replace thresholds with a trained anomaly-detection model
- Add a dashboard on top of the API
- Run the API as a Docker service
- Add tests and a CI workflow