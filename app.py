# app.py
from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from db_connection_google import get_connection_google, close_connection_google
import os
from datetime import datetime

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "supersecretkey123")  # Para sesiones

# ---------------------------
# LOGIN
# ---------------------------
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form["username"]
        password = request.form["password"]

        conn = get_connection_google()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, username, password, nombre_completo FROM usuarios WHERE username=%s AND password=%s",
            (username, password)
        )
        user = cursor.fetchone()
        cursor.close()
        close_connection_google(conn)

        if user:
            # Guardamos info en sesión
            session["user_id"] = user[0]
            session["username"] = user[1]
            session["nombre_completo"] = user[3]
            return redirect(url_for("index"))
        else:
            return render_template("login.html", error="Usuario o contraseña incorrectos")
    else:
        return render_template("login.html", error=None)

# ---------------------------
# INDEX (resumen último corte)
# ---------------------------
@app.route("/index")
def index():
    if "user_id" not in session:
        return redirect(url_for("login"))

    conn = get_connection_google()
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
    close_connection_google(conn)

    hora_consulta = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status = "Activo"

    return render_template(
        "index.html",
        ultima_columna=ultima_columna,
        hora_consulta=hora_consulta,
        status=status
    )
# ---------------------------
# DOWNLOAD
# ---------------------------
@app.route("/download")
def download_excel():
    try:
        # Ejecuta tu merge y obtiene DataFrame
        df = merge_aws_google_batch()

        # Convierte a Excel en memoria
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Reporte')
        output.seek(0)

        # Envía archivo al cliente
        return send_file(
            output,
            as_attachment=True,
            download_name=f"reporte_segundometro_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        return jsonify({"message": f"Error al generar el archivo: {str(e)}"}), 500

# ---------------------------
# LOGOUT
# ---------------------------
@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))



# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
