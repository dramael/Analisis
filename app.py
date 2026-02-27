from flask import Flask, render_template
import yaml

from routes.diagnostico_routes import diagnostico_bp
from routes.prediccion_routes import prediccion_bp  # <-- NUEVO

with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

app = Flask(__name__)

app.register_blueprint(diagnostico_bp)
app.register_blueprint(prediccion_bp)  # <-- NUEVO

@app.route("/")
def home():
    return render_template("index.html")

if __name__ == "__main__":
    app.run(
        host=config["server"]["host"],
        port=config["server"]["port"],
        debug=config["server"]["debug"]
    )