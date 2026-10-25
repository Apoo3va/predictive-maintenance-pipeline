"""Scores every row in vibration_features with the trained Isolation Forest."""
import joblib
import numpy as np
import psycopg2

FEATURES = ["rms", "crest_factor", "kurtosis", "band_energy"]

model = joblib.load("ml/isolation_forest.joblib")

conn = psycopg2.connect(host="localhost", port=5432, dbname="telemetry_db",
                        user="postgres", password="postgres")
cur = conn.cursor()
cur.execute("SELECT id, rms, crest_factor, kurtosis, band_energy FROM vibration_features")
rows = cur.fetchall()

ids = [r[0] for r in rows]
X = np.array([r[1:] for r in rows], dtype=float)

scores = -model.score_samples(X)          # higher = more anomalous
flags = model.predict(X) == -1

cur.executemany(
    "UPDATE vibration_features SET anomaly_score = %s, model_anomaly = %s WHERE id = %s",
    [(float(s), bool(f), int(i)) for s, f, i in zip(scores, flags, ids)],
)
conn.commit()
print(f"Scored {len(ids)} rows, {int(flags.sum())} flagged as anomalies")
conn.close()