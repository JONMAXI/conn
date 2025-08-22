from flask import Flask, jsonify
from db_connection import get_connection, close_connection
from db_connection_google import get_connection_google, close_connection_google
import os
import traceback

app = Flask(__name__)

# Endpoint raíz
@app.route("/")
def index():
    return jsonify({"status": "ok", "message": "🚀 Servicio en Cloud Run funcionando!"})

# Endpoint AWS
@app.route("/pingdb")
def pingdb():
    try:
        conn = get_connection()
        if not conn:
            return jsonify({"status": "error", "message": "No se pudo conectar a la base de datos"}), 500
        cursor = conn.cursor()
        cursor.execute("SELECT NOW();")
        result = cursor.fetchone()
        cursor.close()
        close_connection(conn)
        return jsonify({"status": "ok", "db_time": str(result[0])})
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"status": "error", "message": str(e)}), 500

# Endpoint Google Cloud SQL
@app.route("/pingdb_google")
def pingdb_google():
    try:
        conn = get_connection_google()
        if not conn:
            return jsonify({"status": "error", "message": "No se pudo conectar a Google Cloud SQL"}), 500
        cursor = conn.cursor()
        cursor.execute("SELECT NOW();")
        result = cursor.fetchone()
        cursor.close()
        close_connection_google(conn)
        return jsonify({"status": "ok", "db_time": str(result[0])})
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"status": "error", "message": str(e)}), 500

# Endpoint merge por bloques (import dinámico)
@app.route("/merge-data")
def merge_data_endpoint():
    try:
        # Import dentro del endpoint
        from merge_aws_google import merge_aws_google_batch
        df = merge_aws_google_batch()
        return df.to_json(orient="records")
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"status": "error", "message": str(e)}), 500

# Main
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
