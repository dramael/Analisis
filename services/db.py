import yaml
import psycopg2

with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

db_config = config["database"]

def get_connection():
    conn = psycopg2.connect(
        host=db_config["host"],
        port=db_config["port"],
        dbname=db_config["dbname"],
        user=db_config["user"],
        password=db_config["password"]
    )
    return conn