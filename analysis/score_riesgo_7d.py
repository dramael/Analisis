import joblib
import pandas as pd
import psycopg2
import psycopg2.extras

DB = {
    "host": "192.168.10.41",
    "port": 5432,
    "dbname": "cartografia",
    "user": "python",
    "password": "PyT0n#"
}

FEATURES = ["lag_1", "lag_7", "roll_7", "roll_14", "roll_30", "dow", "mes"]

def get_conn():
    return psycopg2.connect(**DB)

def main(fecha_base="2025-12-31"):
    model = joblib.load("model_riesgo_7d.pkl")

    conn = get_conn()

    # 1) leer features del día
    q = """
    SELECT id_celda, fecha, lag_1, lag_7, roll_7, roll_14, roll_30, dow, mes
    FROM analisis.features_celda_dia
    WHERE fecha = %s
    """
    df = pd.read_sql(q, conn, params=(fecha_base,))
    if df.empty:
        raise RuntimeError(f"No hay features para fecha_base={fecha_base}")

    # 2) predecir probabilidad
    X = df[FEATURES]
    probs = model.predict_proba(X)[:, 1]
    df_out = df[["id_celda"]].copy()
    df_out["fecha_base"] = pd.to_datetime(fecha_base).date()
    df_out["prob_riesgo"] = probs.astype(float)

    # 3) upsert en Postgres
    records = list(df_out[["fecha_base", "id_celda", "prob_riesgo"]].itertuples(index=False, name=None))

    cur = conn.cursor()
    psycopg2.extras.execute_values(
        cur,
        """
        INSERT INTO analisis.riesgo_7d (fecha_base, id_celda, prob_riesgo)
        VALUES %s
        ON CONFLICT (fecha_base, id_celda)
        DO UPDATE SET prob_riesgo = EXCLUDED.prob_riesgo;
        """,
        records,
        page_size=5000
    )
    conn.commit()
    cur.close()
    conn.close()

    print("OK. Riesgo guardado. Filas:", len(records), "fecha_base:", fecha_base)

if __name__ == "__main__":
    main("2025-12-31")