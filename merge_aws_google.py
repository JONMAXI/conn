import pandas as pd
from db_connection import get_connection, close_connection
from db_connection_google import get_connection_google, close_connection_google

BATCH_SIZE = 10000  # Ajustable

def merge_aws_google_batch():
    all_chunks = []
    conn_google = get_connection_google()
    try:
        offset = 0
        while True:
            query = f"""
                SELECT KT AS id_original, Celular AS Telefono, id_cliente AS mkm,
                       id_credit, nombre_cliente AS nombre, 1 AS pagos_vencidos,
                       saldo_vencido_inicio AS monto_vencido,
                       '' AS bucket, '' AS fecha_de_pago, '' AS telefono_1,
                       'Transferencia' AS tipoo_de_pago, Referencia_stp AS clabe,
                       'STP' AS banco, '' AS atributo_segmento
                FROM tbl_segundometro_semana
                ORDER BY KT
                LIMIT {BATCH_SIZE} OFFSET {offset};
            """
            chunk = pd.read_sql(query, conn_google)
            if chunk.empty:
                break

            # AWS
            conn_aws = get_connection()
            try:
                id_creditos = tuple(chunk['id_credit'].tolist())
                if not id_creditos:
                    continue

                aws_query = f"""
                    SELECT o.id_oferta, p.id_persona,
                           CONCAT(p.primer_nombre,' ',p.apellido_paterno,' ',p.apellido_materno) AS nombre_completo,
                           CONCAT(p2.nombre_referencia1,' ',p2.apellido_paterno_referencia1,' ',p2.apellido_materno_referencia1) AS nombre_completo_referencia1,
                           p2.telefono_referencia1,
                           CONCAT(p2.nombre_referencia2,' ',p2.apellido_paterno_referencia2,' ',p2.apellido_materno_referencia2) AS nombre_completo_referencia2,
                           p2.telefono_referencia2
                    FROM oferta o
                    INNER JOIN persona p ON o.fk_persona = p.id_persona
                    LEFT JOIN persona_adicionales p2 ON p2.fk_persona = p.id_persona
                    WHERE o.id_oferta IN {id_creditos};
                """
                df_aws = pd.read_sql(aws_query, conn_aws)
                merged_chunk = pd.merge(chunk, df_aws,
                                        left_on='id_credit',
                                        right_on='id_oferta',
                                        how='left')
                all_chunks.append(merged_chunk)
            finally:
                close_connection(conn_aws)

            offset += BATCH_SIZE
    finally:
        close_connection_google(conn_google)

    if all_chunks:
        return pd.concat(all_chunks, ignore_index=True)
    else:
        return pd.DataFrame()
