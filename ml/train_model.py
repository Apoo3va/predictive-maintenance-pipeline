"""
Trains an Isolation Forest on DSP vibration features from Postgres
and saves the model. Trained on the whole dataset: the model is unsupervised,
so it doesn't need fault labels, only the assumption that faults are rare.
"""
import joblib
import pandas as pd
import psycopg2
from sklearn.ensemble import IsolationForest

FEATURES = ["rms", "crest_factor", "kurtosis", "band_energy"]

conn = psycopg2.connect(host="localhost", port=5432, dbname="telemetry_db",
                        user="postgres", password="postgres")
df = pd.read_sql("SELECT * FROM vibration_features", conn)
conn.close()
print(f"Loaded {len(df)} rows")

model = IsolationForest(n_estimators=200, contamination=0.06, random_state=42)
model.fit(df[FEATURES])

df["pred_anomaly"] = model.predict(df[FEATURES]) == -1
df["score"] = -model.score_samples(df[FEATURES])  # higher = more anomalous

print("\nModel vs rule-based flag:")
print(pd.crosstab(df["is_fault"], df["pred_anomaly"],
                  rownames=["rule is_fault"], colnames=["model anomaly"]))

joblib.dump(model, "ml/isolation_forest.joblib")
print("\nSaved ml/isolation_forest.joblib")
tp = int(((df["is_fault"]) & (df["pred_anomaly"])).sum())
fn = int(((df["is_fault"]) & (~df["pred_anomaly"])).sum())
fp = int(((~df["is_fault"]) & (df["pred_anomaly"])).sum())
print(f"\nRule faults caught by model: {tp}/{tp + fn}")
print(f"Healthy rows flagged by model (false alarms): {fp}")