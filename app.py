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
        conn = get_connection_google()
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
        WHEN Dias_mora_Domingo_16_30 IS NOT NULL THEN 'Dias_mora_Domingo_16_30'
        WHEN Dias_mora_Domingo_14_30 IS NOT NULL THEN 'Dias_mora_Domingo_14_30'
        WHEN Dias_mora_Domingo_13_30 IS NOT NULL THEN 'Dias_mora_Domingo_13_30'
        WHEN Dias_mora_Domingo_11_30 IS NOT NULL THEN 'Dias_mora_Domingo_11_30'
        WHEN Dias_mora_Domingo_09_30 IS NOT NULL THEN 'Dias_mora_Domingo_09_30'
        WHEN Dias_mora_Domingo_07_30 IS NOT NULL THEN 'Dias_mora_Domingo_07_30'

        -- Sábado
        WHEN Dias_mora_Sabado_20_30 IS NOT NULL THEN 'Dias_mora_Sabado_20_30'
        WHEN Dias_mora_Sabado_18_30 IS NOT NULL THEN 'Dias_mora_Sabado_18_30'
        WHEN Dias_mora_Sabado_16_30 IS NOT NULL THEN 'Dias_mora_Sabado_16_30'
        WHEN Dias_mora_Sabado_14_30 IS NOT NULL THEN 'Dias_mora_Sabado_14_30'
        WHEN Dias_mora_Sabado_13_30 IS NOT NULL THEN 'Dias_mora_Sabado_13_30'
        WHEN Dias_mora_Sabado_11_30 IS NOT NULL THEN 'Dias_mora_Sabado_11_30'
        WHEN Dias_mora_Sabado_09_30 IS NOT NULL THEN 'Dias_mora_Sabado_09_30'
        WHEN Dias_mora_Sabado_07_30 IS NOT NULL THEN 'Dias_mora_Sabado_07_30'

        -- Viernes
        WHEN Dias_mora_Viernes_20_30 IS NOT NULL THEN 'Dias_mora_Viernes_20_30'
        WHEN Dias_mora_Viernes_18_30 IS NOT NULL THEN 'Dias_mora_Viernes_18_30'
        WHEN Dias_mora_Viernes_16_30 IS NOT NULL THEN 'Dias_mora_Viernes_16_30'
        WHEN Dias_mora_Viernes_14_30 IS NOT NULL THEN 'Dias_mora_Viernes_14_30'
        WHEN Dias_mora_Viernes_13_30 IS NOT NULL THEN 'Dias_mora_Viernes_13_30'
        WHEN Dias_mora_Viernes_11_30 IS NOT NULL THEN 'Dias_mora_Viernes_11_30'
        WHEN Dias_mora_Viernes_09_30 IS NOT NULL THEN 'Dias_mora_Viernes_09_30'
        WHEN Dias_mora_Viernes_07_30 IS NOT NULL THEN 'Dias_mora_Viernes_07_30'

        -- Jueves
        WHEN Dias_mora_Jueves_20_30 IS NOT NULL THEN 'Dias_mora_Jueves_20_30'
        WHEN Dias_mora_Jueves_18_30 IS NOT NULL THEN 'Dias_mora_Jueves_18_30'
        WHEN Dias_mora_Jueves_16_30 IS NOT NULL THEN 'Dias_mora_Jueves_16_30'
        WHEN Dias_mora_Jueves_14_30 IS NOT NULL THEN 'Dias_mora_Jueves_14_30'
        WHEN Dias_mora_Jueves_13_30 IS NOT NULL THEN 'Dias_mora_Jueves_13_30'
        WHEN Dias_mora_Jueves_11_30 IS NOT NULL THEN 'Dias_mora_Jueves_11_30'
        WHEN Dias_mora_Jueves_09_30 IS NOT NULL THEN 'Dias_mora_Jueves_09_30'
        WHEN Dias_mora_Jueves_07_30 IS NOT NULL THEN 'Dias_mora_Jueves_07_30'

        -- Miércoles
        WHEN Dias_mora_Miercoles_20_30 IS NOT NULL THEN 'Dias_mora_Miercoles_20_30'
        WHEN Dias_mora_Miercoles_18_30 IS NOT NULL THEN 'Dias_mora_Miercoles_18_30'
        WHEN Dias_mora_Miercoles_16_30 IS NOT NULL THEN 'Dias_mora_Miercoles_16_30'
        WHEN Dias_mora_Miercoles_14_30 IS NOT NULL THEN 'Dias_mora_Miercoles_14_30'
        WHEN Dias_mora_Miercoles_13_30 IS NOT NULL THEN 'Dias_mora_Miercoles_13_30'
        WHEN Dias_mora_Miercoles_11_30 IS NOT NULL THEN 'Dias_mora_Miercoles_11_30'
        WHEN Dias_mora_Miercoles_09_30 IS NOT NULL THEN 'Dias_mora_Miercoles_09_30'
        WHEN Dias_mora_Miercoles_07_30 IS NOT NULL THEN 'Dias_mora_Miercoles_07_30'

        -- Martes
        WHEN Dias_mora_Martes_20_30 IS NOT NULL THEN 'Dias_mora_Martes_20_30'
        WHEN Dias_mora_Martes_18_30 IS NOT NULL THEN 'Dias_mora_Martes_18_30'
        WHEN Dias_mora_Martes_16_30 IS NOT NULL THEN 'Dias_mora_Martes_16_30'
        WHEN Dias_mora_Martes_14_30 IS NOT NULL THEN 'Dias_mora_Martes_14_30'
        WHEN Dias_mora_Martes_13_30 IS NOT NULL THEN 'Dias_mora_Martes_13_30'
        WHEN Dias_mora_Martes_11_30 IS NOT NULL THEN 'Dias_mora_Martes_11_30'
        WHEN Dias_mora_Martes_09_30 IS NOT NULL THEN 'Dias_mora_Martes_09_30'
        WHEN Dias_mora_Martes_07_30 IS NOT NULL THEN 'Dias_mora_Martes_07_30'

        -- Lunes
        WHEN Dias_mora_Lunes_20_30 IS NOT NULL THEN 'Dias_mora_Lunes_20_30'
        WHEN Dias_mora_Lunes_18_30 IS NOT NULL THEN 'Dias_mora_Lunes_18_30'
        WHEN Dias_mora_Lunes_16_30 IS NOT NULL THEN 'Dias_mora_Lunes_16_30'
        WHEN Dias_mora_Lunes_14_30 IS NOT NULL THEN 'Dias_mora_Lunes_14_30'
        WHEN Dias_mora_Lunes_13_30 IS NOT NULL THEN 'Dias_mora_Lunes_13_30'
        WHEN Dias_mora_Lunes_11_30 IS NOT NULL THEN 'Dias_mora_Lunes_11_30'
        WHEN Dias_mora_Lunes_09_30 IS NOT NULL THEN 'Dias_mora_Lunes_09_30'
        WHEN Dias_mora_Lunes_07_30 IS NOT NULL THEN 'Dias_mora_Lunes_07_30'

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
