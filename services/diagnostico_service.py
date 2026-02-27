import yaml
import psycopg2
import geopandas as gpd
import numpy as np
from libpysal.weights import Queen
from esda.moran import Moran
from esda.getisord import G_Local
from shapely.geometry import mapping



def load_config(path="config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_connection():
    cfg = load_config()
    db = cfg["database"]
    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        dbname=db["dbname"],
        user=db["user"],
        password=db["password"],
    )


# -------------------------
# Helpers filtros
# -------------------------
def _norm_all(x):
    if x is None:
        return "ALL"
    x = str(x).strip()
    return "ALL" if x == "" else x


def _validate_dates(fecha_inicio, fecha_fin):
    if not fecha_inicio or not fecha_fin:
        raise ValueError("Faltan fecha_inicio o fecha_fin (YYYY-MM-DD).")
    # No parseamos a date acá; Postgres lo castea en params.
    return fecha_inicio, fecha_fin


# -------------------------
# Query base (conteo por celda) para Moran/Gi
# -------------------------
AGG_QUERY = """
SELECT
    g.id_celda,
    g.geom,
    COUNT(d.*) AS total
FROM analisis.grilla_200 g
LEFT JOIN analisis.delitos_raw_enriquecido d
    ON ST_Contains(g.geom, d.geom)
    AND d.fecha BETWEEN %s AND %s
    AND (%s = 'ALL' OR d.delito = %s)
    AND (%s = 'ALL' OR d.modalidad = %s)
GROUP BY g.id_celda, g.geom
ORDER BY g.id_celda;
"""


def fetch_celda_counts(fecha_inicio, fecha_fin, delito="ALL", modalidad="ALL"):
    fecha_inicio, fecha_fin = _validate_dates(fecha_inicio, fecha_fin)
    delito = _norm_all(delito)
    modalidad = _norm_all(modalidad)

    conn = get_connection()
    try:
        gdf = gpd.read_postgis(
            AGG_QUERY,
            conn,
            params=(fecha_inicio, fecha_fin, delito, delito, modalidad, modalidad),
            geom_col="geom",
        )
    finally:
        conn.close()

    # Asegurar dtype
    gdf["total"] = gdf["total"].astype(int)
    return gdf


# -------------------------
# HEATMAP: devolvemos puntos agregados (sin "limit")
# Agregamos por snap grid (50m) para que el payload sea razonable.
# -------------------------
HEATMAP_QUERY = """
WITH pts AS (
    SELECT ST_Transform(d.geom, 3857) AS g
    FROM analisis.delitos_raw_enriquecido d
    WHERE d.fecha BETWEEN %s AND %s
      AND (%s = 'ALL' OR d.delito = %s)
      AND (%s = 'ALL' OR d.modalidad = %s)
),
snapped AS (
    SELECT ST_SnapToGrid(g, 50) AS sg
    FROM pts
)
SELECT
    ST_Y(ST_Transform(sg, 4326)) AS lat,
    ST_X(ST_Transform(sg, 4326)) AS lon,
    COUNT(*)::int AS w
FROM snapped
GROUP BY sg;
"""


def heatmap_points(fecha_inicio, fecha_fin, delito="ALL", modalidad="ALL"):
    fecha_inicio, fecha_fin = _validate_dates(fecha_inicio, fecha_fin)
    delito = _norm_all(delito)
    modalidad = _norm_all(modalidad)

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                HEATMAP_QUERY,
                (fecha_inicio, fecha_fin, delito, delito, modalidad, modalidad),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    # Leaflet.heat: [[lat, lon, intensity], ...]
    return [[float(lat), float(lon), int(w)] for (lat, lon, w) in rows]


# -------------------------
# MORAN GLOBAL: devolver estado categórico (sin valores)
# -------------------------
def moran_estado(fecha_inicio, fecha_fin, delito="ALL", modalidad="ALL"):
    gdf = fetch_celda_counts(fecha_inicio, fecha_fin, delito, modalidad)

    y = gdf["total"].values

    # Si todo es cero -> no hay señal
    if np.all(y == 0):
        return {
            "estado": "Sin eventos",
            "nivel": "neutral",
            "color": "#9e9e9e",
            "detalle": "No hay delitos en el rango seleccionado.",
        }

    # Pesos Queen (incluye islas; es normal tener warnings)
    w = Queen.from_dataframe(gdf, ids=gdf["id_celda"].tolist())
    w.transform = "r"

    moran = Moran(y, w, permutations=199)

    # Clasificación por significancia (sin mostrar números)
    # p_sim bajo => autocorrelación espacial
    if moran.p_sim <= 0.01 and moran.I > 0:
        nivel = "muy_alto"
        estado = "Clustering fuerte"
        color = "#d32f2f"
    elif moran.p_sim <= 0.05 and moran.I > 0:
        nivel = "alto"
        estado = "Clustering"
        color = "#f57c00"
    elif moran.p_sim <= 0.05 and moran.I < 0:
        nivel = "alto"
        estado = "Dispersión"
        color = "#1976d2"
    else:
        nivel = "bajo"
        estado = "Sin patrón significativo"
        color = "#616161"

    return {
        "estado": estado,
        "nivel": nivel,
        "color": color,
        "detalle": "Diagnóstico global (Moran) para el rango/filtros seleccionados.",
    }


# -------------------------
# GETIS-ORD Gi*: devolver GeoJSON de grilla con categoría y color
# -------------------------
def gi_geojson(fecha_inicio, fecha_fin, delito="ALL", modalidad="ALL"):
    gdf = fetch_celda_counts(fecha_inicio, fecha_fin, delito, modalidad)
    
    # 🔥 REPROYECTAR A WGS84
    gdf = gdf.to_crs(epsg=4326)

    y = gdf["total"].values

    if np.all(y == 0):
        # Devolver grilla con todo "No significativo" para que el front igual pinte
        features = []
        for row in gdf.itertuples():
            features.append({
                "type": "Feature",
                "geometry": mapping(row.geom),
                "properties": {
                    "id_celda": int(row.id_celda),
                    "categoria": "Sin eventos",
                    "color": "#9e9e9e",
                },
            })
        return {"type": "FeatureCollection", "features": features, "leyenda": gi_leyenda()}

    w = Queen.from_dataframe(gdf, ids=gdf["id_celda"].tolist())
    w.transform = "r"

    gi = G_Local(y, w, permutations=199)

    # Z-scores y p-values simulados
    z = np.array(gi.Zs, dtype=float)
    p = np.array(gi.p_sim, dtype=float)

    def classify(zv, pv):
        # Si viene NaN (por islas), lo marcamos neutral
        if not np.isfinite(zv) or not np.isfinite(pv):
            return ("No significativo", "#bdbdbd")

        if pv <= 0.01 and zv > 0:
            return ("Hotspot 99%", "#b71c1c")
        if pv <= 0.05 and zv > 0:
            return ("Hotspot 95%", "#ef5350")
        if pv <= 0.01 and zv < 0:
            return ("Coldspot 99%", "#0d47a1")
        if pv <= 0.05 and zv < 0:
            return ("Coldspot 95%", "#64b5f6")
        return ("No significativo", "#bdbdbd")

    features = []
    for i, row in enumerate(gdf.itertuples()):
        cat, col = classify(z[i], p[i])
        features.append({
            "type": "Feature",
            "geometry": mapping(row.geom),
            "properties": {
                "id_celda": int(row.id_celda),
                "categoria": cat,
                "color": col,
            },
        })

    return {"type": "FeatureCollection", "features": features, "leyenda": gi_leyenda()}


def gi_leyenda():
    # Leyenda fija, el front puede dibujarla tal cual
    return [
        {"label": "Hotspot 99%", "color": "#b71c1c"},
        {"label": "Hotspot 95%", "color": "#ef5350"},
        {"label": "Coldspot 99%", "color": "#0d47a1"},
        {"label": "Coldspot 95%", "color": "#64b5f6"},
        {"label": "No significativo", "color": "#bdbdbd"},
        {"label": "Sin eventos", "color": "#9e9e9e"},
    ]
    
    
    
def puntos_delito(fecha_inicio, fecha_fin, delito="ALL", modalidad="ALL"):

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
    SELECT 
        ST_Y(ST_Transform(geom, 4326)) AS lat,
        ST_X(ST_Transform(geom, 4326)) AS lon
    FROM analisis.delitos_raw_enriquecido
    WHERE fecha BETWEEN %s AND %s
      AND (%s = 'ALL' OR delito = %s)
      AND (%s = 'ALL' OR modalidad = %s);
""", (fecha_inicio, fecha_fin, delito, delito, modalidad, modalidad))

            rows = cur.fetchall()

    finally:
        conn.close()

    puntos = [[float(r[0]), float(r[1])] for r in rows]

    return {
        "total": len(puntos),
        "points": puntos
    }
    
    
    
def get_catalogos():

    conn = get_connection()
    try:
        with conn.cursor() as cur:

            cur.execute("""
                SELECT DISTINCT delito
                FROM analisis.delitos_raw_enriquecido
                WHERE delito IS NOT NULL
                ORDER BY delito;
            """)
            delitos = [r[0] for r in cur.fetchall()]

            cur.execute("""
                SELECT DISTINCT modalidad
                FROM analisis.delitos_raw_enriquecido
                WHERE modalidad IS NOT NULL
                ORDER BY modalidad;
            """)
            modalidades = [r[0] for r in cur.fetchall()]

    finally:
        conn.close()

    return {
        "delitos": delitos,
        "modalidades": modalidades
    }