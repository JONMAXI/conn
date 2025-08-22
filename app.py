from flask import Flask, render_template, request, redirect, url_for, session, send_file
import mysql.connector
import pandas as pd
from io import BytesIO
from merge_aws_google import merge_aws_google_batch  # tu función de merge

app = Flask(__name__)
app.secret_key = "supersecretkey123"  # necesario para sesiones

db_config = {
    'host': '34.9.147.5',
    'user': 'jonathan',
    'password': ')1>SbilQ,$VKr=hO',
    'database': 'db-mega-reporte',
    'port': 3306
}

def get_connection():
    try:
        conn = mysql.connector.connect(**db_config)
        return conn
    except:
        return None

# ---------------- LOGIN ----------------
@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = request.form.get("username")
        password = request.form.get("password")

        conn = get_connection()
        if not conn:
            error = "No se pudo conectar a la base de datos"
            return render_template("login.html", error=error)

        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT * FROM usuarios WHERE username = %s AND password = %s",
            (username, password)
        )
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        if user:
            session["user"] = {
                "id": user["id"],
                "username": user["username"],
                "nombre_completo": user["nombre_completo"]
            }
            return redirect(url_for("dashboard"))
        else:
            error = "Usuario o contraseña incorrectos"

    return render_template("login.html", error=error)

# ---------------- DASHBOARD ----------------
@app.route("/dashboard")
def dashboard():
    if "user" not in session:
        return redirect(url_for("login"))

    user = session["user"]

    # Consultar última fecha de corte
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT MAX(
            CASE
                WHEN Dias_mora_Viernes_11_30 IS NOT NULL THEN 'Dias_mora_Viernes_11_30'
                WHEN Dias_mora_Jueves_11_30 IS NOT NULL THEN 'Dias_mora_Jueves_11_30'
                ELSE 'Sin datos'
            END
        ) AS ultima_columna
        FROM tbl_segundometro_semana;
    """)
    result = cursor.fetchone()
    ultima_columna = result[0] if result else "Sin datos"
    cursor.close()
    conn.close()

    return render_template("index.html",
                           user=user,
                           ultima_columna=ultima_columna,
                           hora_consulta=pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"),
                           status="OK")

# ---------------- DESCARGAR EXCEL ----------------
@app.route("/download")
def download():
    if "user" not in session:
        return redirect(url_for("login"))

    # Ejecutar merge
    df = merge_aws_google_batch()

    # Crear archivo Excel en memoria
    output = BytesIO()
    df.to_excel(output, index=False, engine='openpyxl')
    output.seek(0)

    return send_file(output,
                     attachment_filename="reporte_segundometro.xlsx",
                     as_attachment=True,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

# ---------------- LOGOUT ----------------
@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

# ---------------- RUN ----------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
