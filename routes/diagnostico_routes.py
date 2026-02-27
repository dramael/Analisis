from flask import Blueprint, jsonify, request
from services.diagnostico_service import heatmap_points, moran_estado, gi_geojson


diagnostico_bp = Blueprint("diagnostico_bp", __name__)


def _args():
    return {
        "fecha_inicio": request.args.get("fecha_inicio"),
        "fecha_fin": request.args.get("fecha_fin"),
        "delito": request.args.get("delito", "ALL"),
        "modalidad": request.args.get("modalidad", "ALL"),
    }


@diagnostico_bp.route("/api/diagnostico/heatmap")
def api_heatmap():
    a = _args()
    pts = heatmap_points(a["fecha_inicio"], a["fecha_fin"], a["delito"], a["modalidad"])
    # Leaflet.heat espera array; devolvemos también metadata para la UI
    return jsonify({
        "points": pts,
        "leyenda": {
            "tipo": "intensidad",
            "nota": "Heatmap agregado por snap-grid 50m (peso = cantidad de eventos)."
        }
    })


@diagnostico_bp.route("/api/diagnostico/moran")
def api_moran():
    a = _args()
    out = moran_estado(a["fecha_inicio"], a["fecha_fin"], a["delito"], a["modalidad"])
    # Solo “estado” y color (sin números)
    return jsonify(out)


@diagnostico_bp.route("/api/diagnostico/gi")
def api_gi():
    a = _args()
    out = gi_geojson(a["fecha_inicio"], a["fecha_fin"], a["delito"], a["modalidad"])
    return jsonify(out)


from services.diagnostico_service import puntos_delito
@diagnostico_bp.route("/api/diagnostico/puntos")
def api_puntos():
    fecha_inicio = request.args.get("fecha_inicio")
    fecha_fin = request.args.get("fecha_fin")
    delito = request.args.get("delito", "ALL")
    modalidad = request.args.get("modalidad", "ALL")

    return jsonify(puntos_delito(fecha_inicio, fecha_fin, delito, modalidad))

from services.diagnostico_service import get_catalogos

@diagnostico_bp.route("/api/catalogos")
def api_catalogos():
    return jsonify(get_catalogos())