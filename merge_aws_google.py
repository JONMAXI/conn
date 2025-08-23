import pandas as pd
from db_connection import get_connection, close_connection          # AWS
from db_connection_google import get_connection_google, close_connection_google  # Google

def merge_aws_google_batch(batch_size=5000, page=1):
    """
    Obtiene datos de Google Cloud SQL y AWS RDS en batches para evitar saturar Cloud Run.
    Retorna un DataFrame de pandas con los datos combinados de la página solicitada.
    """
    offset = (page - 1) * batch_size

    # --- Conexión Google Cloud SQL ---
    conn_google = get_connection_google()

    # 1️⃣ Obtener el nombre de la última columna no nula
    query_ultima_columna = """
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
               -- resto de días...
               ELSE NULL
           END
       ) AS ultima_columna_llena
       FROM tbl_segundometro_semana;
    """
    ultima_columna = pd.read_sql(query_ultima_columna, conn_google).iloc[0, 0]

    # 2️⃣ Obtener los datos de Google filtrados por la última columna
    query_google = f"""
        SELECT KT AS id_original, Celular AS Telefono, 'Transferencia' as fideicomiso, Id_cliente AS mkm,
               Id_credito AS id_credit, nombre_cliente AS nombre, 1 AS pagos_vencidos,
               saldo_vencido_inicio AS monto_vencido,
               '' AS bucket, '' AS fecha_de_pago, '' AS telefono_1,
               'Transferencia' AS tipoo_de_pago, Referencia_stp AS clabe,
               'STP' AS banco, '' AS atributo_segmento
        FROM tbl_segundometro_semana
        WHERE {ultima_columna} BETWEEN 1 AND 7
        ORDER BY KT
        LIMIT {batch_size} OFFSET {offset}
    """
    df_google = pd.read_sql(query_google, conn_google)
    close_connection_google(conn_google)

    if df_google.empty:
        return pd.DataFrame()  # página fuera de rango

    # --- Conexión AWS RDS ---
    conn_aws = get_connection()
    ids_chunk = tuple(df_google['id_credit'].astype(str).tolist())
    if len(ids_chunk) == 1:
        ids_chunk = f"('{ids_chunk[0]}')"

    query_aws = f"""
        SELECT o.id_oferta, 
               CONCAT(p.primer_nombre, ' ', p.apellido_paterno, ' ', p.apellido_materno) AS nombre_completo,
               CONCAT(p2.nombre_referencia1, ' ', p2.apellido_paterno_referencia1, ' ', p2.apellido_materno_referencia1) AS nombre_completo_referencia1,
               p2.telefono_referencia1,
               CONCAT(p2.nombre_referencia2, ' ', p2.apellido_paterno_referencia2, ' ', p2.apellido_materno_referencia2) AS nombre_completo_referencia2,
               p2.telefono_referencia2, '' as nombre_referencia_3, '' as telefono_referencia_3, 0 as Motivo_de_no_Pago, 0 as cuando_le_pagan, 0 as Giro_de_Trabajo, 0 as hora_de_pago

        FROM oferta o
        INNER JOIN persona p ON o.fk_persona = p.id_persona
        LEFT JOIN persona_adicionales p2 ON p2.fk_persona = p.id_persona
        WHERE o.id_oferta IN {ids_chunk}
    """
    df_aws = pd.read_sql(query_aws, conn_aws)
    close_connection(conn_aws)

    # --- Asegurar tipos para merge ---
    df_google['id_credit'] = df_google['id_credit'].astype(str)
    df_aws['id_oferta'] = df_aws['id_oferta'].astype(str)

    # --- Merge LEFT ---
    df_merged = pd.merge(df_google, df_aws,
                         left_on='id_credit',
                         right_on='id_oferta',
                         how='left')

    if 'id_oferta' in df_merged.columns:
        df_merged.drop(columns=['id_oferta'], inplace=True)

    return df_merged


# ------------------------------
# Alias de compatibilidad
# ------------------------------
def merge_aws_google():
    return merge_aws_google_batch(batch_size=5000, page=1)
