from flask import Flask
import yaml
from routes.map_routes import map_bp
from routes.stats_routes import stats_bp

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

app = Flask(__name__)

app.register_blueprint(map_bp)
app.register_blueprint(stats_bp)

from flask import render_template

@app.route("/")
def home():
    return render_template("index.html")
if __name__ == "__main__":
    app.run(
        host=config["server"]["host"],
        port=config["server"]["port"],
        debug=config["server"]["debug"]
    )