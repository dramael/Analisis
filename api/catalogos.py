# routes/stats_routes.py (o un catalog_routes.py)
from flask import Blueprint, jsonify
from services.db import get_connection

stats_bp = Blueprint("stats_bp", __name__)

@stats_bp.route("/api/catalogos")
def catalogos():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT delito FROM analisis.delitos_enriquecidos WHERE delito IS NOT NULL ORDER BY 1;")
    delitos = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT DISTINCT modalidad FROM analisis.delitos_enriquecidos WHERE modalidad IS NOT NULL ORDER BY 1;")
    modalidades = [r[0] for r in cur.fetchall()]

    cur.close()
    conn.close()

    return jsonify({"delitos": delitos, "modalidades": modalidades})