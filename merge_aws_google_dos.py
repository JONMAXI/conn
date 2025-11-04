import pandas as pd
from db_connection import get_connection, close_connection          # AWS
from db_connection_google import get_connection_google, close_connection_google  # Google
import numpy as np

def merge_aws_google_batch_dos(batch_size=5000, page=1):
    """
    Obtiene datos de Google Cloud SQL y AWS RDS en batches usando Python en lugar de SQL.
    Retorna un DataFrame de pandas con los datos combinados del batch solicitado.
    """
    # --- Conexión Google Cloud SQL ---
    conn_google = get_connection_google()

    # 1️⃣ Obtener el nombre de la última columna no nula
    query_ultima_columna = """
       SELECT nombre_columna, valor
        FROM (
            SELECT 'Dias_mora_Lunes_07_30' AS nombre_columna, Dias_mora_Lunes_07_30 AS valor,
                   STR_TO_DATE(CONCAT(CURDATE(), ' 07:30'), '%Y-%m-%d %H:%i') AS fecha_real
            FROM tbl_segundometro_semana
            UNION ALL
            SELECT 'Dias_mora_Lunes_09_30', Dias_mora_Lunes_09_30,
                   STR_TO_DATE(CONCAT(CURDATE(), ' 09:30'), '%Y-%m-%d %H:%i')
            FROM tbl_segundometro_semana
            UNION ALL
            SELECT 'Dias_mora_Lunes_11_30', Dias_mora_Lunes_11_30,
                   STR_TO_DATE(CONCAT(CURDATE(), ' 11:30'), '%Y-%m-%d %H:%i')
            FROM tbl_segundometro_semana
            UNION ALL
            SELECT 'Dias_mora_Lunes_13_30', Dias_mora_Lunes_13_30,
                   STR_TO_DATE(CONCAT(CURDATE(), ' 13:30'), '%Y-%m-%d %H:%i')
            FROM tbl_segundometro_semana
            UNION ALL
            SELECT 'Dias_mora_Lunes_14_30', Dias_mora_Lunes_14_30,
                   STR_TO_DATE(CONCAT(CURDATE(), ' 14:30'), '%Y-%m-%d %H:%i')
            FROM tbl_segundometro_semana
            UNION ALL
            SELECT 'Dias_mora_Lunes_16_30', Dias_mora_Lunes_16_30,
                   STR_TO_DATE(CONCAT(CURDATE(), ' 16:30'), '%Y-%m-%d %H:%i')
            FROM tbl_segundometro_semana
            UNION ALL
            SELECT 'Dias_mora_Lunes_18_30', Dias_mora_Lunes_18_30,
                   STR_TO_DATE(CONCAT(CURDATE(), ' 18:30'), '%Y-%m-%d %H:%i')
            FROM tbl_segundometro_semana
            UNION ALL
            SELECT 'Dias_mora_Lunes_20_30', Dias_mora_Lunes_20_30,
                   STR_TO_DATE(CONCAT(CURDATE(), ' 20:30'), '%Y-%m-%d %H:%i')
            FROM tbl_segundometro_semana
        ) t
        WHERE valor IS NOT NULL
        ORDER BY ABS(TIMESTAMPDIFF(MINUTE, fecha_real, NOW())) ASC
        LIMIT 1;
    """
    ultima_columna = pd.read_sql(query_ultima_columna, conn_google).iloc[0, 0]

    # 2️⃣ Obtener todos los datos de Google (sin LIMIT/OFFSET)
    query_google = f"""
       SELECT 
        CONCAT(Id_credito, '_', Id_cliente) AS id_original, 
        Celular AS Telefono, 
        'Transferencia' AS fideicomiso, 
        Id_cliente AS mkm,
        Id_credito AS id_credit, 
        nombre_cliente AS nombre, 
        1 AS pagos_vencidos,
        saldo_vencido_inicio AS monto_vencido,
        '' AS bucket, 
        '' AS fecha_de_pago, 
        '' AS telefono_1,
        'Transferencia' AS tipoo_de_pago, 
        Referencia_stp AS clabe,
        'STP' AS banco, 
        '' AS atributo_segmento
    FROM tbl_segundometro_semana
    WHERE 
        ({ultima_columna} = 0
        OR (Saldo_Vencido_actualizado) <= 50)
        AND (Fecha_ultimo_pago_efectivo > CURDATE() OR Fecha_ultimo_pago_efectivo IS NULL)
    ORDER BY KT;
    """
    df_google_full = pd.read_sql(query_google, conn_google)
    close_connection_google(conn_google)

    if df_google_full.empty:
        return pd.DataFrame()  # página fuera de rango

    # --- Dividir en batches usando pandas ---
    start_idx = (page - 1) * batch_size
    end_idx = start_idx + batch_size
    df_google = df_google_full.iloc[start_idx:end_idx]

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
               p2.telefono_referencia2, '' as nombre_referencia_3, '' as telefono_referencia_3, 
               0 as Motivo_de_no_Pago, 0 as cuando_le_pagan, 0 as Giro_de_Trabajo, 0 as hora_de_pago
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


# 🔁 Función para recorrer todas las páginas automáticamente
def merge_aws_google_full(batch_size=5000):
    page = 1
    all_data = []

    while True:
        print(f"Descargando lote {page}...")
        df_batch = merge_aws_google_batch_dos(batch_size=batch_size, page=page)
        if df_batch.empty:
            break
        all_data.append(df_batch)
        page += 1

    if not all_data:
        print("No se obtuvieron datos.")
        return pd.DataFrame()

    df_final = pd.concat(all_data, ignore_index=True)
    print(f"✅ Descarga completa: {len(df_final)} registros en total.")
    return df_final


# ------------------------------
# Alias de compatibilidad
# ------------------------------
def merge_aws_google():
    return merge_aws_google_full(batch_size=5000)
