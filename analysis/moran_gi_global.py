import yaml
import psycopg2
import psycopg2.extras
import geopandas as gpd
import pandas as pd
import numpy as np
from libpysal.weights import Queen
from esda.moran import Moran
from esda.getisord import G_Local

import os
print(">>> moran_gi_global.py ejecutándose desde:", os.path.abspath(__file__))
print(">>> cwd:", os.getcwd())


# =========================
# Cargar configuración
# =========================
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

db = config["database"]
schema = db["schema"]


def get_connection():
    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["dbname"],
        user=db["user"],
        password=db["password"]
    )


# =========================
# Calcular Moran + Gi*
# =========================
def calcular_moran_gi(periodo):

    conn = get_connection()

    # Traer dataset del mes
    query = f"""
        SELECT id_celda, conteo_delitos, geom
        FROM {schema}.delitos_celda_mes
        WHERE periodo = %s
    """

    gdf = gpd.read_postgis(query, conn, params=(periodo,), geom_col="geom")

    if gdf.empty:
        print("No hay datos para ese período")
        return

    # =========================
    # Matriz espacial (Queen)
    # =========================
    w = Queen.from_dataframe(gdf)

    # eliminar islas
    w = w.subset([i for i in w.id_order if len(w.neighbors[i]) > 0])
    w.transform = "r"

    # =========================
    # Moran Global
    # =========================
    moran = Moran(gdf.loc[w.id_order, "conteo_delitos"], w)

    print("Moran I:", moran.I)
    print("p-value:", moran.p_sim)
    print("z-score:", moran.z_sim)

    # Guardar Moran
    cursor = conn.cursor()

    cursor.execute(f"""
        INSERT INTO {schema}.moran_global
        (periodo, moran_i, z_score, p_value)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (periodo)
        DO UPDATE SET
            moran_i = EXCLUDED.moran_i,
            z_score = EXCLUDED.z_score,
            p_value = EXCLUDED.p_value;
    """, (periodo, moran.I, moran.z_sim, moran.p_sim))

    # =========================
    # Getis-Ord Gi*
    # =========================
    gi = G_Local(gdf.loc[w.id_order, "conteo_delitos"], w)

    gdf_subset = gdf.loc[w.id_order].copy()
    gdf_subset["gi_zscore"] = gi.Zs
    gdf_subset["gi_pvalue"] = gi.p_sim

    # Clasificación
    def classify(row):
        if row["gi_pvalue"] < 0.01:
            return "Hotspot 99%" if row["gi_zscore"] > 0 else "Coldspot 99%"
        elif row["gi_pvalue"] < 0.05:
            return "Hotspot 95%" if row["gi_zscore"] > 0 else "Coldspot 95%"
        else:
            return "No significativo"

    gdf_subset["categoria"] = gdf_subset.apply(classify, axis=1)

    # Limpiar NaN
    gdf_subset = gdf_subset.replace([np.inf, -np.inf], np.nan)
    gdf_subset = gdf_subset.dropna(subset=["gi_zscore"])

    # Guardar Gi*
    records = [
        (
            periodo,
            row.id_celda,
            float(row.gi_zscore),
            float(row.gi_pvalue),
            row.categoria
        )
        for row in gdf_subset.itertuples()
    ]

    psycopg2.extras.execute_values(
        cursor,
        f"""
        INSERT INTO {schema}.gi_star
        (periodo, id_celda, gi_zscore, gi_pvalue, categoria)
        VALUES %s
        ON CONFLICT (periodo, id_celda)
        DO UPDATE SET
            gi_zscore = EXCLUDED.gi_zscore,
            gi_pvalue = EXCLUDED.gi_pvalue,
            categoria = EXCLUDED.categoria;
        """,
        records
    )

    conn.commit()
    cursor.close()
    conn.close()

    print("Cálculo terminado para:", periodo)


# =========================
# Ejecutar manual
# =========================
if __name__ == "__main__":
    periodo_test = "2024-12-01"
    calcular_moran_gi(periodo_test)