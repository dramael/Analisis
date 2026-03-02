from flask import Blueprint, jsonify, request
from services.prediccion_service import (
    train_model_all,
    predict_next_days_geojson,
    get_T,
    load_model
)
import os

prediccion_bp = Blueprint("prediccion_bp", __name__)


@prediccion_bp.route("/api/prediccion/status")
def api_status():
    try:
        T = get_T()

        # Verificar si modelo existe en DB
        try:
            load_model()
            entrenado = True
        except:
            entrenado = False

        return jsonify({
            "fecha_base": str(T),
            "modelo_entrenado": entrenado
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@prediccion_bp.route("/api/prediccion/train")
def api_train():
    meta = train_model_all()
    return jsonify({"status": "Modelo entrenado", "meta": meta})


@prediccion_bp.route("/api/prediccion")
def api_prediccion():
    horizonte = int(request.args.get("horizonte", 3))
    gj = predict_next_days_geojson(horizonte)
    return jsonify(gj)



from flask import send_file
import io
import geopandas as gpd

@prediccion_bp.route("/api/prediccion/export")
def api_prediccion_export():

    horizonte = int(request.args.get("horizonte", 7))
    formato = request.args.get("formato", "geojson")

    gj = predict_next_days_geojson(horizonte)

    gdf = gpd.GeoDataFrame.from_features(gj["features"], crs="EPSG:4326")

    if formato == "geojson":
        buffer = io.BytesIO()
        gdf.to_file(buffer, driver="GeoJSON")
        buffer.seek(0)
        return send_file(
            buffer,
            mimetype="application/geo+json",
            as_attachment=True,
            download_name=f"prediccion_{horizonte}d.geojson"
        )

    elif formato == "kml":
        buffer = io.BytesIO()
        gdf.to_file(buffer, driver="KML")
        buffer.seek(0)
        return send_file(
            buffer,
            mimetype="application/vnd.google-earth.kml+xml",
            as_attachment=True,
            download_name=f"prediccion_{horizonte}d.kml"
        )

    return {"error": "Formato no soportado"}, 400