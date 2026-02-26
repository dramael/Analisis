# routes/map_routes.py
from flask import Blueprint, jsonify, request
from services.db import get_connection

map_bp = Blueprint("map_bp", __name__)

@map_bp.route("/api/puntos")
def puntos():
    fecha_inicio = request.args.get("fecha_inicio")  # YYYY-MM-DD
    fecha_fin    = request.args.get("fecha_fin")     # YYYY-MM-DD
    delito       = request.args.get("delito")        # opcional
    modalidad    = request.args.get("modalidad")     # opcional
    limit_       = int(request.args.get("limit", 5000))

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