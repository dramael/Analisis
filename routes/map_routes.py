from flask import Blueprint, jsonify, request
from services.db import get_connection

map_bp = Blueprint("map_bp", __name__)

@map_bp.route("/api/gi")
def get_gi():
    periodo = request.args.get("periodo")

    conn = get_connection()
    cursor = conn.cursor()

    query = """
    SELECT 
        jsonb_build_object(
            'type', 'FeatureCollection',
            'features', COALESCE(jsonb_agg(
                jsonb_build_object(
                    'type', 'Feature',
                    'geometry', ST_AsGeoJSON(ST_Transform(g.geom, 4326))::jsonb,
                    'properties', jsonb_build_object(
                        'id_celda', gi.id_celda,
                        'categoria', gi.categoria,
                        'zscore', gi.gi_zscore
                    )
                )
            ), '[]'::jsonb)
        )
    FROM analisis.gi_star gi
    JOIN analisis.grilla_300m g
        ON gi.id_celda = g.id_celda
    WHERE gi.periodo = %s;
    """

    cursor.execute(query, (periodo,))
    result = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    return jsonify(result)


@map_bp.route("/api/puntos")
def get_puntos():
    fecha_inicio = request.args.get("fecha_inicio")  # YYYY-MM-DD
    fecha_fin    = request.args.get("fecha_fin")     # YYYY-MM-DD
    delito       = request.args.get("delito", "ALL")
    modalidad    = request.args.get("modalidad", "ALL")
    limit_       = int(request.args.get("limit", 5000))

    if not fecha_inicio or not fecha_fin:
        return jsonify({"type": "FeatureCollection", "features": [], "error": "Faltan fechas"}), 400

    where = ["fecha >= %s", "fecha <= %s"]
    params = [fecha_inicio, fecha_fin]

    if delito and delito != "ALL":
        where.append("delito = %s")
        params.append(delito)

    if modalidad and modalidad != "ALL":
        where.append("modalidad = %s")
        params.append(modalidad)

    where_sql = " AND ".join(where)

    query = f"""
    SELECT jsonb_build_object(
        'type','FeatureCollection',
        'features', COALESCE(jsonb_agg(
            jsonb_build_object(
                'type','Feature',
                'geometry', ST_AsGeoJSON(ST_Transform(geom, 4326))::jsonb,
                'properties', jsonb_build_object(
                    'id', id,
                    'fecha', fecha::text,
                    'delito', delito,
                    'modalidad', modalidad,
                    'barrio', barrio,
                    'comuna', comuna
                )
            )
        ), '[]'::jsonb)
    )
    FROM (
        SELECT id, fecha, delito, modalidad, barrio, comuna, geom
        FROM analisis.delitos_enriquecidos
        WHERE {where_sql}
        LIMIT %s
    ) t;
    """

    params.append(limit_)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(query, tuple(params))
    result = cur.fetchone()[0]
    cur.close()
    conn.close()

    return jsonify(result)


@map_bp.route("/api/riesgo_7d")
def get_riesgo_7d():
    fecha_base = request.args.get("fecha_base")

    conn = get_connection()
    cur = conn.cursor()

    # si no mandan fecha, usar la última
    if not fecha_base:
        cur.execute("SELECT MAX(fecha_base) FROM analisis.riesgo_7d;")
        fecha_base = cur.fetchone()[0]
        if fecha_base is None:
            cur.close(); conn.close()
            return jsonify({"type":"FeatureCollection","features":[]})

    query = """
    SELECT jsonb_build_object(
      'type','FeatureCollection',
      'features', COALESCE(jsonb_agg(
        jsonb_build_object(
          'type','Feature',
          'geometry', ST_AsGeoJSON(ST_Transform(g.geom, 4326))::jsonb,
          'properties', jsonb_build_object(
            'id_celda', r.id_celda,
            'prob', r.prob_riesgo
          )
        )
      ), '[]'::jsonb)
    )
    FROM analisis.riesgo_7d r
    JOIN analisis.grilla_300m g ON g.id_celda = r.id_celda
    WHERE r.fecha_base = %s;
    """

    cur.execute(query, (fecha_base,))
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    return jsonify(result)