import os
import mysql.connector
from mysql.connector import Error

# ------------------------------
# CONFIGURACIÓN DE BASE DE DATOS
# ------------------------------
DB_HOST = os.getenv("DB_HOST", "maxi-base.cluster-csa4gsaishoe.us-east-1.rds.amazonaws.com")
DB_USER = os.getenv("DB_USER", "jesus.ruvalcaba")
DB_PASSWORD = os.getenv("DB_PASSWORD", "tu_password")  # ⚠️ Configura en Cloud Run
DB_NAME = os.getenv("DB_NAME", "maxi-prod")
DB_PORT = int(os.getenv("DB_PORT", 3306))

def get_connection():
    """Crea y devuelve una conexión a la base de datos MySQL."""
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME,
            port=DB_PORT
        )
        if connection.is_connected():
            print("✅ Conexión exitosa a la base de datos")
            return connection
    except Error as e:
        print(f"❌ Error al conectar a MySQL: {e}")
        return None

def close_connection(conn):
    """Cierra una conexión existente a MySQL."""
    if conn and conn.is_connected():
        conn.close()
        print("🔒 Conexión cerrada")
