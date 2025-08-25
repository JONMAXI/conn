# app.py
from flask import Flask, render_template, request, redirect, url_for, session, jsonify, send_file
from db_connection_google import get_connection_google, close_connection_google
import os
from datetime import datetime
from merge_aws_google import merge_aws_google_batch
from io import BytesIO
import pandas as pd
from datetime import datetime
import pytz

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

 # 👇 Validación especial para guillermo
    if session.get("username") == "guillermo":
        return render_template("bonos_1_7.html", nombre=session.get("nombre_completo"))

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
    ultima_columna_raw = cursor.fetchone()[0]
    cursor.close()
    close_connection_google(conn)

    if ultima_columna_raw:
        # Extraemos solo 'Viernes_14_30' y le agregamos el prefijo
        ultima_columna = f"Reporte_Corte_{'_'.join(ultima_columna_raw.split('_')[2:])}"
    else:
        ultima_columna = "Reporte_Corte_Desconocido"

    # Zona horaria Ciudad de México
    cdmx_tz = pytz.timezone("America/Mexico_City")
    hora_consulta = datetime.now(cdmx_tz).strftime("%Y-%m-%d %H:%M:%S")
    status = "Activo"

    # Guardamos el nombre dinámico en sesión para la descarga
    session["ultima_columna"] = ultima_columna

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
        batch_size = 5000
        page = 1
        output = BytesIO()

        # Recuperamos el nombre dinámico de sesión
        nombre_archivo = session.get("ultima_columna", f"reporte_segundometro_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}")

        # Creamos el ExcelWriter usando openpyxl
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            first_page = True
            while True:
                df = merge_aws_google_batch(batch_size=batch_size, page=page)
                if df.empty:
                    break  # terminamos cuando no hay más registros

                startrow = (page - 1) * batch_size
                df.to_excel(
                    writer,
                    index=False,
                    sheet_name='Reporte',
                    startrow=startrow if not first_page else 0,
                    header=first_page
                )

                first_page = False
                page += 1

        output.seek(0)
        return send_file(
            output,
            as_attachment=True,
            download_name=f"{nombre_archivo}.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({"message": f"Error al generar el archivo: {str(e)}"}), 500# ---------------------------
# LOGOUT
# ---------------------------
@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ------------------------------
# EJECUCIÓN DE BONOS
# ------------------------------
@app.route("/ejecutar_bonos", methods=["POST"])
def ejecutar_bonos():
    from db_connection_google import get_connection_google, close_connection_google
    from mysql.connector import Error
    from datetime import datetime, timedelta
    import pytz

    logs = []  # 🔹 Inicializamos logs
    tz = pytz.timezone('America/Mexico_City')
    hoy = datetime.now(tz)
    dia_semana = 6 #hoy.weekday()  # 0=Lunes ... 6=Domingo

    TABLA_ORIGEN = 'tbl_segundometro_semana'
    TABLA_DESTINO = 'tbl_eficiencia_bonos_1_7'

    # ------------------------------
    # Determinar día de ejecución
    # ------------------------------
    if dia_semana == 5:  # Sábado
        DIA_EJECUCION = 'SABADO'
        TBL_CIERRE_DIA = 'Dias_mora_Sabado_09_30'
    elif dia_semana == 6:  # Domingo
        DIA_EJECUCION = 'DOMINGO'
        TBL_CIERRE_DIA = 'Dias_mora_Domingo_09_30'
    elif dia_semana == 0:  # Lunes
        DIA_EJECUCION = 'LUNES'
        TBL_CIERRE_DIA = 'Dias_mora_Lunes_09_30'
    else:
        return jsonify({"status": "error", "logs": ["❌ Este script solo debe ejecutarse Sábado, Domingo o Lunes."]})

    # ------------------------------
    # Calcular semana
    # ------------------------------
    fecha_semana = hoy
    if DIA_EJECUCION == 'LUNES':
        fecha_semana -= timedelta(days=1)
    numero_semana = fecha_semana.isocalendar()[1]
    anio = fecha_semana.year
    SEMANA = f"Semana {numero_semana}-{anio}"

    conn = None
    cursor = None

    try:
        conn = get_connection_google()
        cursor = conn.cursor()
        logs.append("✅ Conexión exitosa a la base de datos")

        # ------------------------------
        # Verificar si ya se ejecutó
        # -----------------------------
        cursor.execute(f"SELECT COUNT(*) FROM {TABLA_DESTINO} WHERE SEMANA='{SEMANA}';")
        count_destino = cursor.fetchone()[0]

        if DIA_EJECUCION == 'SABADO' and count_destino > 2:
            logs.append(f"❌ Ya se ejecutó el cálculo de bonos para {SEMANA} (sábado). Espere al domingo o contacte al administrador.")
            return jsonify({"status": "error", "logs": logs})

        # ------------------------------
        # BLOQUE SÁBADO
        # ------------------------------
        if DIA_EJECUCION == 'SABADO':
            logs.append(f"🔹 Sábado: Insertando datos base en {TABLA_DESTINO}...")
            cursor.execute(f"""
                INSERT INTO {TABLA_DESTINO} (Nombre_RH, Territorial, Gestor_Asignado, Cobranza, Semana)
                SELECT Gestor_Asignado, Territorial, Gestor_Asignado, SUM(Saldo_vencido_actualizado), '{SEMANA}'
                FROM {TABLA_ORIGEN}
                WHERE SEMANA = '{SEMANA}'
                GROUP BY Gestor_Asignado, Territorial;
            """)
            conn.commit()
            logs.append("✅ Datos base insertados")

            # Asignación y Cura
            logs.append("🔹 Calculando Asignacion y Cura...")
            cursor.execute(f"""
                UPDATE {TABLA_DESTINO} gc
                JOIN (
                    SELECT Gestor_Asignado,
                           COUNT(*) AS Asignacion,
                           COUNT(CASE WHEN {TBL_CIERRE_DIA}=0 THEN 1 ELSE NULL END) AS Cura
                    FROM {TABLA_ORIGEN}
                    WHERE SEMANA = '{SEMANA}'
                    GROUP BY Gestor_Asignado
                ) t ON gc.Gestor_Asignado = t.Gestor_Asignado
                SET gc.Asignacion = t.Asignacion,
                    gc.Cura = t.Cura;
            """)
            conn.commit()
            logs.append("✅ Asignacion y Cura actualizados")

            # Eficiencia
            logs.append("🔹 Calculando Eficiencia...")
            cursor.execute(f"UPDATE {TABLA_DESTINO} SET Eficiencia = IF(Asignacion>0,(Cura/Asignacion)*100,0);")
            conn.commit()
            logs.append("✅ Eficiencia calculada")

            # Pendientes
            logs.append("🔹 Calculando Pendientes...")
            cursor.execute(f"UPDATE {TABLA_DESTINO} SET Pendientes = (Asignacion - Cura);")
            conn.commit()
            logs.append("✅ Pendientes calculados")

            # Tipo de Gestor
            logs.append("🔹 Asignando Tipo de Gestor...")
            cursor.execute(f"""
                UPDATE {TABLA_DESTINO}
                SET Tipo_de_Gestor = CASE
                    WHEN Asignacion<=19 THEN 'ARCADIO'
                    WHEN Asignacion BETWEEN 20 AND 29 THEN 'GLADIADOR'
                    ELSE 'CENTURION'
                END;
            """)
            conn.commit()
            logs.append("✅ Tipo de Gestor asignado")

            # Cierre Viernes
            logs.append("🔹 Calculando Cierre VIERNES...")
            cursor.execute(f"""
                UPDATE {TABLA_DESTINO}
                SET Cierre='Cierre Viernes'
                WHERE (Eficiencia>=97) AND (Cierre IS NULL OR Cierre='');
            """)
            conn.commit()
            logs.append("✅ Cierre VIERNES calculado")

            # Cobranza total
            logs.append("🔹 Calculando Cobranza total por gestor...")
            cursor.execute(f"""
                UPDATE {TABLA_DESTINO} gc
                JOIN (
                    SELECT Gestor_Asignado,SUM(Saldo_vencido_actualizado) AS Total_Cobranza
                    FROM {TABLA_ORIGEN} WHERE SEMANA='{SEMANA}' GROUP BY Gestor_Asignado
                ) t ON gc.Gestor_Asignado=t.Gestor_Asignado
                SET gc.Cobranza=t.Total_Cobranza;
            """)
            conn.commit()
            logs.append("✅ Cobranza total actualizada")

        # ------------------------------
        # BLOQUE DOMINGO
        # ------------------------------
        elif DIA_EJECUCION=='DOMINGO':
            logs.append("🔹 Domingo: calculando cierre SÁBADO...")
            cursor.execute(f"""
                UPDATE {TABLA_DESTINO}
                SET Cierre='Cierre Sábado'
                WHERE Cierre IS NULL OR Cierre='';
            """)
            conn.commit()
            logs.append("✅ Cierre SABADO calculado")

        # ------------------------------
        # BLOQUE LUNES
        # ------------------------------
        elif DIA_EJECUCION=='LUNES':
            logs.append("🔹 Lunes: calculando cierre DOMINGO...")
            cursor.execute(f"""
                UPDATE {TABLA_DESTINO}
                SET Cierre='Cierre Domingo'
                WHERE Cierre IS NULL OR Cierre='';
            """)
            conn.commit()
            logs.append("✅ Cierre DOMINGO calculado")

        # ------------------------------
        # BONOS
        # ------------------------------
        if DIA_EJECUCION=='SABADO':
            cierre_objetivo='Cierre Viernes'
            bonos={'ARCADIO':'Cura*150','GLADIADOR':'3000','CENTURION':'Cobranza*0.18'}
        elif DIA_EJECUCION=='DOMINGO':
            cierre_objetivo='Cierre Sábado'
            bonos={'ARCADIO':'Cura*90','GLADIADOR':'1800','CENTURION':'Cobranza*0.16'}
        else:
            cierre_objetivo='Cierre Domingo'
            bonos={'ARCADIO':'Cura*70','GLADIADOR':'1600','CENTURION':'Cobranza*0.14'}

        logs.append(f"🔹 Calculando bono para {cierre_objetivo}...")
        cursor.execute(f"""
            UPDATE {TABLA_DESTINO}
            SET Bono = CASE
                WHEN Tipo_de_Gestor='ARCADIO' THEN {bonos['ARCADIO']}
                WHEN Tipo_de_Gestor='GLADIADOR' THEN {bonos['GLADIADOR']}
                WHEN Tipo_de_Gestor='CENTURION' THEN {bonos['CENTURION']}
            END
            WHERE Cierre='{cierre_objetivo}';
        """)
        conn.commit()
        logs.append(f"✅ Bono actualizado para {cierre_objetivo}")
        logs.append("🎉 Proceso finalizado correctamente.")

    except Error as e:
        logs.append(f"❌ Error en ejecución: {str(e)}")

    finally:
        if cursor: cursor.close()
        if conn:
            close_connection_google(conn)
            logs.append("🔒 Conexión cerrada")

    return jsonify({"status":"ok","logs":logs})
  


# ---------------------------
# Main
# ---------------------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
