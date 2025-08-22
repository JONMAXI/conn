from flask import Flask, jsonify
from db_connection import get_connection, close_connection

app = Flask(__name__)

@app.route("/")
def index():
    return jsonify({"status": "ok", "message": "🚀 Servicio en Cloud Run funcionando!"})

@app.route("/pingdb")
def pingdb():
    """Endpoint de prueba para verificar conexión a MySQL"""
    conn = get_connection()
    if not conn:
        return jsonify({"status": "error", "message": "No se pudo conectar a la base de datos"}), 500

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT NOW();")
        result = cursor.fetchone()
        cursor.close()
        close_connection(conn)
        return jsonify({"status": "ok", "db_time": str(result[0])})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    # Cloud Run asigna el puerto dinámicamente en $PORT
    import os
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
