from flask import Blueprint, jsonify, request
from services.prediccion_service import train_model_all, predict_next_days_geojson

prediccion_bp = Blueprint("prediccion_bp", __name__)


@prediccion_bp.route("/api/prediccion/train")
def api_train():
    meta = train_model_all()
    return jsonify({"status":"Modelo entrenado","meta":meta})


@prediccion_bp.route("/api/prediccion")
def api_prediccion():
    horizonte = int(request.args.get("horizonte",3))
    gj = predict_next_days_geojson(horizonte)
    return jsonify(gj)