import pandas as pd
from db_connection import get_connection, close_connection          # AWS
from db_connection_google import get_connection_google, close_connection_google  # Google

def merge_aws_google_batch(batch_size=5000, page=1):
    """
    Obtiene datos de Google Cloud SQL y AWS RDS en batches para evitar saturar Cloud Run.
    Retorna un DataFrame de pandas con los datos combinados de la página solicitada.

    Args:
        batch_size: cantidad de registros por batch (default 5000)
        page: número de página (default 1)
    """

    # --- Conexión Google Cloud SQL ---
    conn_google = get_connection_google()
    offset = (page - 1) * batch_size
    query_google = f"""
        SELECT KT AS id_original, Celular AS Telefono, Id_cliente AS mkm,
               Id_credito AS id_credit, nombre_cliente AS nombre, 1 AS pagos_vencidos,
               saldo_vencido_inicio AS monto_vencido,
               '' AS bucket, '' AS fecha_de_pago, '' AS telefono_1,
               'Transferencia' AS tipoo_de_pago, Referencia_stp AS clabe,
               'STP' AS banco, '' AS atributo_segmento
        FROM tbl_segundometro_semana
        ORDER BY KT
        LIMIT {batch_size} OFFSET {offset}
    """
    df_google = pd.read_sql(query_google, conn_google)
    close_connection_google(conn_google)

    if df_google.empty:
        return pd.DataFrame()  # página fuera de rango

    # --- Conexión AWS RDS ---
    conn_aws = get_connection()
    # Traer solo los registros de AWS que coinciden con los id_credit del batch
    ids_chunk = tuple(df_google['id_credit'].astype(str).tolist())
    if len(ids_chunk) == 1:
        ids_chunk = f"('{ids_chunk[0]}')"  # SQL necesita paréntesis si solo 1
    query_aws = f"""
        SELECT o.id_oferta, p.id_persona, 
               CONCAT(p.primer_nombre, ' ', p.apellido_paterno, ' ', p.apellido_materno) AS nombre_completo,
               CONCAT(p2.nombre_referencia1, ' ', p2.apellido_paterno_referencia1, ' ', p2.apellido_materno_referencia1) AS nombre_completo_referencia1,
               p2.telefono_referencia1,
               CONCAT(p2.nombre_referencia2, ' ', p2.apellido_paterno_referencia2, ' ', p2.apellido_materno_referencia2) AS nombre_completo_referencia2,
               p2.telefono_referencia2
        FROM oferta o
        INNER JOIN persona p ON o.fk_persona = p.id_persona
        LEFT JOIN persona_adicionales p2 ON p2.fk_persona = p.id_persona
        WHERE o.id_oferta IN {ids_chunk}
    """
    df_aws = pd.read_sql(query_aws, conn_aws)
    close_connection(conn_aws)

    # --- Asegurar tipos de merge ---
    df_google['id_credit'] = df_google['id_credit'].astype(str)
    df_aws['id_oferta'] = df_aws['id_oferta'].astype(str)

    # --- Merge LEFT para mantener todos los registros de Google ---
    df_merged = pd.merge(df_google, df_aws,
                         left_on='id_credit',
                         right_on='id_oferta',
                         how='left')

    # --- Eliminar columnas duplicadas ---
    if 'id_oferta' in df_merged.columns:
        df_merged.drop(columns=['id_oferta'], inplace=True)

    return df_merged
