import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
import joblib
import io
import json
from datetime import timedelta
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
import yaml
import geopandas as gpd
from shapely.geometry import mapping

with open("config.yaml","r") as f:
    config = yaml.safe_load(f)

DB = config["database"]
SCHEMA = DB["schema"]

FEATURES = ["lag1","sum7","dow","month"]


# =============================
# CONFIGURACION ENTRENAMIENTO
# =============================

TRAIN_YEARS_BACK = 2   # 👈 CAMBIAR ACA (1, 2, 3, etc.)

def get_connection():
    return psycopg2.connect(
        host=DB["host"],
        port=DB["port"],
        dbname=DB["dbname"],
        user=DB["user"],
        password=DB["password"]
    )


def get_T():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT max(fecha)
            FROM {SCHEMA}.celda_dia_200m_all;
        """)
        T = cur.fetchone()[0]
    finally:
        conn.close()
    return T


# =============================
# ENTRENAR MODELO ALL
# =============================


def train_model_all():

    T = get_T()

    conn = get_connection()
    try:
        
        print('Query de entranamiento')
        query = f"""
        SELECT
            id_celda,
            fecha,
            conteo,

            LAG(conteo,1) OVER (PARTITION BY id_celda ORDER BY fecha) AS lag1,

            SUM(conteo) OVER (
                PARTITION BY id_celda
                ORDER BY fecha
                ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
            ) AS sum7,

            EXTRACT(DOW FROM fecha) AS dow,
            EXTRACT(MONTH FROM fecha) AS month

        FROM {SCHEMA}.celda_dia_200m_all
        WHERE fecha >= (%s::date - interval '{TRAIN_YEARS_BACK} years')
          AND fecha < %s;
        """

        df = pd.read_sql(query, conn, params=[T, T])

    finally:
        conn.close()


# =============================
# CARGAR MODELO
# =============================
def load_model():

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(f"""
            SELECT modelo
            FROM {SCHEMA}.modelos_ml
            WHERE nombre_modelo = 'rf_ALL';
        """)
        row = cur.fetchone()

        if not row:
            raise FileNotFoundError("Modelo ALL no entrenado.")

        buffer = io.BytesIO(row[0])
        model = joblib.load(buffer)

    finally:
        conn.close()

    return model

def clasificar_riesgo(p):
    if p >= 70:
        return "Alto", "#d73027"
    elif p >= 50:
        return "Medio", "#fc8d59"
    elif p >= 30:
        return "Bajo", "#fee08b"
    else:
        return "Muy bajo", "#d9d9d900"

# =============================
# PREDICCION
# =============================
def predict_next_days_geojson(horizonte=3):

    horizonte = int(horizonte)
    if horizonte < 1: horizonte = 1
    if horizonte > 7: horizonte = 7

    T = get_T()
    model = load_model()

    conn = get_connection()
    try:
        query = f"""
            WITH base_fecha AS (
            SELECT %s::date AS f
        )
        SELECT
            g.id_celda,
            ST_Transform(g.geom,4326) AS geom,

            COALESCE((
                SELECT e.conteo
                FROM analisis.celda_dia_200m_all e, base_fecha
                WHERE e.id_celda = g.id_celda
                AND e.fecha = (f - interval '1 day')::date
            ),0) AS lag1,

            COALESCE((
                SELECT SUM(e.conteo)
                FROM analisis.celda_dia_200m_all e, base_fecha
                WHERE e.id_celda = g.id_celda
                AND e.fecha BETWEEN (f - interval '7 day')::date
                                AND (f - interval '1 day')::date
            ),0) AS sum7,

            EXTRACT(DOW FROM f) AS dow,
            EXTRACT(MONTH FROM f) AS month

        FROM analisis.grilla_200 g, base_fecha
        ORDER BY id_celda;
        """

        fecha_pred = T + timedelta(days=1)
        print("T:", T)
        print("fecha_pred:", fecha_pred)
        

        df = gpd.read_postgis(
            query,
            conn,
            params=[fecha_pred],
            geom_col="geom"
        )

    finally:
        conn.close()

    df = df.fillna(0)
    X = df[FEATURES]

    probs = model.predict_proba(X)[:,1]

    if horizonte > 1:
        probs = 1 - (1 - probs)**horizonte

    df["riesgo_pct"] = probs * 100
    
    
        # =============================
    # GUARDAR HISTORICO
    # =============================

    fecha_pred_inicio = T + timedelta(days=1)
    fecha_pred_fin = T + timedelta(days=horizonte)

    conn = get_connection()
    try:
        cur = conn.cursor()

        insert_sql = f"""
            INSERT INTO {SCHEMA}.predicciones_historicas
            (nombre_modelo, fecha_base, fecha_pred_inicio, fecha_pred_fin,
            horizonte, id_celda, riesgo_pct, categoria)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (nombre_modelo, fecha_base, horizonte, id_celda)
            DO NOTHING;
        """

        records = []

        for _, row in df.iterrows():
            riesgo = round(row["riesgo_pct"], 2)
            categoria, _ = clasificar_riesgo(riesgo)

            records.append((
                "rf_ALL",
                T,
                fecha_pred_inicio,
                fecha_pred_fin,
                horizonte,
                int(row["id_celda"]),
                riesgo,
                categoria
            ))

        psycopg2.extras.execute_batch(cur, insert_sql, records, page_size=1000)
        conn.commit()

    finally:
        conn.close()

    features = []

    for _, row in df.iterrows():

        riesgo = round(row["riesgo_pct"], 2)
        categoria, color = clasificar_riesgo(riesgo)

        features.append({
            "type": "Feature",
            "geometry": mapping(row.geom),
            "properties": {
                "id_celda": int(row["id_celda"]),
                "riesgo_pct": riesgo,
                "categoria": categoria,
                "color": color
            }
        })

    return {
        "type":"FeatureCollection",
        "fecha_base": str(T),
        "horizonte": horizonte,
        "features": features
    }
    

