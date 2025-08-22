import pandas as pd
from db_connection import get_connection, close_connection
from db_connection_google import get_connection_google, close_connection_google

def merge_aws_google_batch():
    # -----------------------------
    # 1️⃣ Traer datos de Google
    # -----------------------------
    conn_google = get_connection_google()
    query_google = """
        SELECT 
            KT AS id_original,
            Celular AS Telefono,
            id_cliente AS mkm,
            id_credit AS id_credito,
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
    """
    df_google = pd.read_sql(query_google, conn_google)
    close_connection_google(conn_google)

    # -----------------------------
    # 2️⃣ Traer todos los datos de AWS en 1 query
    # -----------------------------
    conn_aws = get_connection()

    # Generar lista de id_credito para filtrar
    id_creditos = df_google['id_credito'].tolist()
    if not id_creditos:
        return df_google  # No hay registros

    # Convertir lista a string para IN clause
    id_creditos_str = ",".join(str(x) for x in id_creditos)

    query_aws = f"""
        SELECT 
            o.id_oferta AS id_credito,
            p.id_persona,
            CONCAT(p.primer_nombre, ' ', p.apellido_paterno, ' ', p.apellido_materno) AS nombre_completo,
            CONCAT(p2.nombre_referencia1, ' ', p2.apellido_paterno_referencia1, ' ', p2.apellido_materno_referencia1) AS nombre_completo_referencia1,
            p2.telefono_referencia1,
            CONCAT(p2.nombre_referencia2, ' ', p2.apellido_paterno_referencia2, ' ', p2.apellido_materno_referencia2) AS nombre_completo_referencia2,
            p2.telefono_referencia2
        FROM oferta o
        INNER JOIN persona p ON o.fk_persona = p.id_persona
        LEFT JOIN persona_adicionales p2 ON p2.fk_persona = p.id_persona
        WHERE o.id_oferta IN ({id_creditos_str})
    """
    df_aws = pd.read_sql(query_aws, conn_aws)
    close_connection(conn_aws)

    # -----------------------------
    # 3️⃣ Combinar Google + AWS usando merge (LEFT JOIN)
    # -----------------------------
    df_final = df_google.merge(
        df_aws,
        how='left',  # Mantener todos los registros de Google
        left_on='id_credito',
        right_on='id_credito'
    )

    return df_final
