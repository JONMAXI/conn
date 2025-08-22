import os
import mysql.connector
from mysql.connector import Error

# ------------------------------
# CONFIGURACIÓN DE BASE DE DATOS GOOGLE CLOUD SQL
# ------------------------------
DB_USER = os.getenv("DB_USER_GOOGLE", "jonathan")
DB_PASSWORD = os.getenv("DB_PASSWORD_GOOGLE", ")1>SbilQ,$VKr=hO")
DB_NAME = os.getenv("DB_NAME_GOOGLE", "db-mega-reporte")
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME", "mxkcobranza:us-central1:maxikash-db")
DB_PORT = int(os.getenv("DB_PORT_GOOGLE", 3306))

def get_connection_google():
    """Crea y devuelve una conexión a Google Cloud SQL MySQL."""
    try:
        # Cloud Run se conecta vía socket Unix
        connection = mysql.connector.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            unix_socket=f"/cloudsql/{INSTANCE_CONNECTION_NAME}",
            database=DB_NAME,
            port=DB_PORT
        )
        if connection.is_connected():
            print("✅ Conexión exitosa a Google Cloud SQL")
            return connection
    except Error as e:
        print(f"❌ Error al conectar a Google Cloud SQL: {e}")
        return None

def close_connection_google(conn):
    """Cierra una conexión existente a Google Cloud SQL."""
    if conn and conn.is_connected():
        conn.close()
        print("🔒 Conexión cerrada Google Cloud SQL")
