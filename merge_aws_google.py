import pandas as pd
from db_connection import get_connection, close_connection          # AWS
from db_connection_google import get_connection_google, close_connection_google  # Google

def merge_aws_google_batch():
    """
    Obtiene datos de Google Cloud SQL y AWS RDS, y realiza un merge seguro.
    Retorna un DataFrame de pandas con los datos combinados.
    """
    # --- DATOS GOOGLE (tbl_segundometro_semana) ---
    conn_google = get_connection_google()
    query_google = """
        SELECT KT AS id_original, Celular AS Telefono, Id_cliente AS mkm,
               Id_credito AS id_credit, nombre_cliente AS nombre, 1 AS pagos_vencidos,
               saldo_vencido_inicio AS monto_vencido,
               '' AS bucket, '' AS fecha_de_pago, '' AS telefono_1,
               'Transferencia' AS tipoo_de_pago, Referencia_stp AS clabe,
               'STP' AS banco, '' AS atributo_segmento
        FROM tbl_segundometro_semana
        ORDER BY KT
    """
    df_google = pd.read_sql(query_google, conn_google)
    close_connection_google(conn_google)

    # --- DATOS AWS (oferta + persona + persona_adicionales) ---
    conn_aws = get_connection()
    query_aws = """
        SELECT o.id_oferta, p.id_persona, 
               CONCAT(p.primer_nombre, ' ', p.apellido_paterno, ' ', p.apellido_materno) AS nombre_completo,
               CONCAT(p2.nombre_referencia1, ' ', p2.apellido_paterno_referencia1, ' ', p2.apellido_materno_referencia1) AS nombre_completo_referencia1,
               p2.telefono_referencia1,
               CONCAT(p2.nombre_referencia2, ' ', p2.apellido_paterno_referencia2, ' ', p2.apellido_materno_referencia2) AS nombre_completo_referencia2,
               p2.telefono_referencia2
        FROM oferta o
        INNER JOIN persona p ON o.fk_persona = p.id_persona
        LEFT JOIN persona_adicionales p2 ON p2.fk_persona = p.id_persona
    """
    df_aws = pd.read_sql(query_aws, conn_aws)
    close_connection(conn_aws)

    # --- Asegurar que los tipos de columna coincidan ---
    df_google['id_credit'] = df_google['id_credit'].astype(str)
    df_aws['id_oferta'] = df_aws['id_oferta'].astype(str)

    # --- Merge LEFT para mantener todos los registros de Google ---
    df_merged = pd.merge(df_google, df_aws,
                         left_on='id_credit',
                         right_on='id_oferta',
                         how='left')

    # --- Eliminar columnas duplicadas si es necesario ---
    if 'id_oferta' in df_merged.columns:
        df_merged.drop(columns=['id_oferta'], inplace=True)

    return df_merged
