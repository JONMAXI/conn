from flask import Flask, render_template, request, redirect, url_for, jsonify, send_file
from db_connection import get_connection, close_connection
from db_connection_google import get_connection_google, close_connection_google
from merge_aws_google import merge_aws_google_batch
import pandas as pd
import os, io, datetime

app = Flask(__name__)

# ---------- LOGIN ----------
@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form['username']
        password = request.form['password']

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, nombre_completo FROM usuarios WHERE username=%s AND password=%s",
            (username, password)
        )
        user = cursor.fetchone()
        cursor.close()
        close_connection(conn)

        if user:
            return redirect(url_for("dashboard"))
        else:
            error = "Usuario o contraseña incorrectos"

    return render_template("login.html", error=error)

# ---------- DASHBOARD ----------
@app.route("/dashboard")
def dashboard():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MAX(
            CASE
                WHEN Dias_mora_Domingo_23_50 IS NOT NULL THEN 'Dias_mora_Domingo_23_50'
                WHEN Dias_mora_Viernes_11_30 IS NOT NULL THEN 'Dias_mora_Viernes_11_30'
                -- Agrega aquí todas las demás columnas según tu consulta original
                ELSE NULL
            END
        ) AS ultima_columna
        FROM tbl_segundometro_semana
    """)
    result = cursor.fetchone()
    ultima_columna = result[0] if result else "Sin datos"
    cursor.close()
    close_connection(conn)

    hora_consulta = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = "OK" if ultima_columna != "Sin datos" else "Sin información"

    return render_template("dashboard.html",
                           ultima_columna=ultima_columna,
                           hora_consulta=hora_consulta,
                           status=status)

# ---------- DESCARGA EXCEL ----------
@app.route("/download")
def download():
    df = merge_aws_google_batch()
    output = io.BytesIO()
    df.to_excel(output, index=False, engine='openpyxl')
    output.seek(0)

    filename = f"reporte_segundometro_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return send_file(output,
                     attachment_filename=filename,
                     as_attachment=True,
                     mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

# ---------- PRUEBAS DE CONEXIÓN ----------
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
        return jsonify({"status": "error", "message": str(e)}), 500

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
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
