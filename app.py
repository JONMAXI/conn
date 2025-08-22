from flask import Flask, jsonify
from db_connection import get_connection, close_connection
import os
import traceback

app = Flask(__name__)

# Endpoint raíz para probar que el servicio está vivo
@app.route("/")
def index():
    return jsonify({"status": "ok", "message": "🚀 Servicio en Cloud Run funcionando!"})

# Endpoint para probar la conexión a la base de datos
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
        # Muestra el traceback completo en los logs de Cloud Run
        error_msg = traceback.format_exc()
        print("❌ ERROR en /pingdb:", error_msg)
        return jsonify({"status": "error", "message": str(e)}), 500

# Main para correr localmente (Cloud Run ignora esto, usa gunicorn)
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
