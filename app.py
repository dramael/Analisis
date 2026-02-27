from flask import Flask, render_template
from routes.diagnostico_routes import diagnostico_bp
import yaml


# =========================
# Cargar configuración
# =========================
def load_config():
    with open("config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


config = load_config()

# =========================
# Crear app
# =========================
app = Flask(__name__)

# Registrar blueprints
app.register_blueprint(diagnostico_bp)

# =========================
# Ruta principal
# =========================
@app.route("/")
def home():
    return render_template("index.html")


# =========================
# Run
# =========================
if __name__ == "__main__":
    app.run(
        host=config["server"]["host"],
        port=config["server"]["port"],
        debug=config["server"]["debug"]
    )