import pandas as pd
from db_connection import get_connection, close_connection
from db_connection_google import get_connection_google, close_connection_google

def merge_aws_google_batch():
    """
    Obtiene datos de AWS y Google Cloud SQL, y realiza un merge seguro por tipos.
    Retorna un DataFrame de pandas con los datos combinados.
    """
    # --- DATOS GOOGLE (tbl_segundometro_semana) ---
    conn_google = get_connection()
    query_google = """
        SELECT KT AS id_original, Celular AS Telefono, Id_cliente AS mkm,
               Id_credito as id_credit, nombre_cliente AS nombre, 1 AS pagos_vencidos,
               saldo_vencido_inicio AS monto_vencido,
               '' AS bucket, '' AS fecha_de_pago, '' AS telefono_1,
               'Transferencia' AS tipoo_de_pago, Referencia_stp AS clabe,
               'STP' AS banco, '' AS atributo_segmento
        FROM tbl_segundometro_semana
        ORDER BY KT
    """
    df_google = pd.read_sql(query_google, conn_google)
    close_connection(conn_google)

    # --- DATOS AWS (oferta + persona + persona_adicionales) ---
    conn_aws = get_connection_google()
    query_aws = """
        SELECT p.id_persona, CONCAT(p.primer_nombre, ' ', p.apellido_paterno, ' ', p.apellido_materno) AS nombre_completo,
               CONCAT(p2.nombre_referencia1, ' ', p2.apellido_paterno_referencia1, ' ', p2.apellido_materno_referencia1) AS nombre_completo_referencia1,
               p2.telefono_referencia1,
               CONCAT(p2.nombre_referencia2, ' ', p2.apellido_paterno_referencia2, ' ', p2.apellido_materno_referencia2) AS nombre_completo_referencia2,
               p2.telefono_referencia2
        FROM oferta o
        INNER JOIN persona p ON o.fk_persona = p.id_persona
        LEFT JOIN persona_adicionales p2 ON p2.fk_persona = p.id_persona
    """
    df_aws = pd.read_sql(query_aws, conn_aws)
    close_connection_google(conn_aws)

    # --- Aseguramos que los tipos sean consistentes ---
    df_google['id_credit'] = df_google['id_credit'].astype(str)
    df_aws['id_persona'] = df_aws['id_persona'].astype(str)  # llave de AWS para merge
    df_google['id_original'] = df_google['id_original'].astype(str)

    # --- Merge LEFT para mantener todos los registros de Google ---
    df_merged = pd.merge(df_google, df_aws,
                         left_on='id_credit',  # id_credit de Google
                         right_on='id_persona',  # id_persona de AWS
                         how='left')

    # --- Opcional: eliminar columnas duplicadas ---
    if 'id_persona' in df_merged.columns:
        df_merged.drop(columns=['id_persona'], inplace=True)

    return df_merged
