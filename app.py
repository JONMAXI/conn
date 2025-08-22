# app.py
from flask import Flask, jsonify, render_template, send_file
from db_connection import get_connection, close_connection
from db_connection_google import get_connection_google, close_connection_google
from merge_aws_google import merge_aws_google_batch
import pandas as pd
from io import BytesIO
import os
import traceback
from datetime import datetime

app = Flask(__name__, template_folder="templates")

# ----------------------
# Endpoint raíz: resumen
# ----------------------
@app.route("/")
def index():
    try:
        conn = get_connection()
        if not conn:
            return jsonify({"status": "error", "message": "No se pudo conectar a la base de datos"}), 500

        cursor = conn.cursor()
        # Consulta último corte
        cursor.execute("""
            SELECT MAX(
                CASE
                    -- Domingo
                    WHEN Dias_mora_Domingo_23_50 IS NOT NULL THEN 'Dias_mora_Domingo_23_50'
                    WHEN Dias_mora_Domingo_20_30 IS NOT NULL THEN 'Dias_mora_Domingo_20_30'
                    WHEN Dias_mora_Domingo_18_30 IS NOT NULL THEN 'Dias_mora_Domingo_18_30'
                    -- ... (el resto de tu CASE completo)
                    ELSE NULL
                END
            ) AS ultima_columna_llena
            FROM tbl_segundometro_semana;
        """)
        ultima_columna = cursor.fetchone()[0]
        cursor.close()
        close_connection(conn)

        return render_template(
            "index.html",
            ultima_columna=ultima_columna,
            hora_consulta=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            status="OK"
        )

    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"status": "error", "message": str(e)}), 500


# ----------------------------------
# Endpoint de descarga de Excel
# ----------------------------------
@app.route("/download")
def download_excel():
    try:
        # Ejecuta merge entre Google y AWS
        df = merge_aws_google_batch()

        # Genera Excel en memoria
        output = BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)

        filename = f"reporte_segundometro_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        return send_file(
            output,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            download_name=filename,
            as_attachment=True
        )

    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"status": "error", "message": str(e)}), 500


# ----------------------
# Main local
# ----------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
