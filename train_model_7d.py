import pandas as pd
import psycopg2
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
import numpy as np

# =============================
# CONFIG DB
# =============================
conn = psycopg2.connect(
    host="192.168.10.41",
    port=5432,
    dbname="cartografia",
    user="python",
    password="PyT0n#"
)

# =============================
# Cargar datos
# =============================
query = """
SELECT *
FROM analisis.model_data
WHERE fecha >= '2019-01-01'
"""

df = pd.read_sql(query, conn)
df["fecha"] = pd.to_datetime(df["fecha"])
conn.close()

print("Filas cargadas:", len(df))

# =============================
# Split temporal
# =============================
cutoff = pd.Timestamp("2024-01-01")

train = df[df["fecha"] < cutoff]
test  = df[df["fecha"] >= cutoff]

print("Train:", len(train))
print("Test:", len(test))

# =============================
# Features
# =============================
features = [
    "lag_1",
    "lag_7",
    "roll_7",
    "roll_14",
    "roll_30",
    "dow",
    "mes"
]

X_train = train[features]
y_train = train["target_7d"]

X_test  = test[features]
y_test  = test["target_7d"]

# =============================
# Modelo baseline
# =============================
model = LogisticRegression(max_iter=1000, n_jobs=-1)
model.fit(X_train, y_train)

# =============================
# Predicción
# =============================
probs = model.predict_proba(X_test)[:,1]

# =============================
# Métrica ROC AUC
# =============================
auc = roc_auc_score(y_test, probs)
print("ROC AUC:", round(auc,4))

# =============================
# Precision @ Top 10%
# =============================
threshold = np.percentile(probs, 90)
top_mask = probs >= threshold

precision_top10 = y_test[top_mask].mean()
print("Precision @ Top 10%:", round(precision_top10,4))

# =============================
# Guardar modelo simple
# =============================
import joblib
joblib.dump(model, "model_riesgo_7d.pkl")

print("Modelo guardado.")