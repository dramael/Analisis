from flask import Blueprint, jsonify, request
from services.db import get_connection

stats_bp = Blueprint("stats_bp", __name__)

@stats_bp.route("/api/moran")
def get_moran():

    periodo = request.args.get("periodo")

    conn = get_connection()
    cursor = conn.cursor()

    query = """
    SELECT moran_i, z_score, p_value
    FROM analisis.moran_global
    WHERE periodo = %s;
    """

    cursor.execute(query, (periodo,))
    result = cursor.fetchone()

    cursor.close()
    conn.close()

    return jsonify({
        "moran_i": result[0],
        "z_score": result[1],
        "p_value": result[2]
    })
    
    
from flask import Blueprint, jsonify
from services.db import get_connection

stats_bp = Blueprint("stats_bp", __name__)

@stats_bp.route("/api/catalogos")
def catalogos():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT delito
        FROM analisis.delitos_enriquecidos
        WHERE delito IS NOT NULL AND delito <> ''
        ORDER BY 1
    """)
    delitos = [r[0] for r in cur.fetchall()]

    cur.execute("""
        SELECT DISTINCT modalidad
        FROM analisis.delitos_enriquecidos
        WHERE modalidad IS NOT NULL AND modalidad <> ''
        ORDER BY 1
    """)
    modalidades = [r[0] for r in cur.fetchall()]

    cur.close()
    conn.close()

    return jsonify({"delitos": delitos, "modalidades": modalidades})