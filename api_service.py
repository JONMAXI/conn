# api_service.py
from flask import Blueprint, jsonify, request
import pandas as pd

# Conexiones
from db_connection import get_connection, close_connection
from db_connection_google import get_connection_google, close_connection_google

# Blueprint
api = Blueprint("api_service", __name__)

# ---------------------------
# API: REPORTE SEGUNDOMETRO
# ---------------------------
@api.route("/api/reporte_segundometro", methods=["GET"])
def api_reporte_segundometro():
    try:
        batch_size = int(request.args.get("batch_size", 5000))
        page = int(request.args.get("page", 1))
        offset = (page - 1) * batch_size

        # --- Conexión Google ---
        conn_google = get_connection_google()

        query_google = f"""
            SELECT 
                CONCAT(Id_credito, '_', Id_cliente) AS id_original, 
                Celular AS Telefono, 
                'Transferencia' AS fideicomiso, 
                Id_cliente AS mkm,
                Id_credito AS id_credit, 
                nombre_cliente AS nombre, 
                Cuotas_vencidas AS pagos_vencidos, 
                saldo_vencido_inicio AS monto_vencido,
                Cuota,
                Monto_Otorgado,
                Bucket_Morosidad_Real AS bucket_nacimiento,
                Fecha_Ultimo_Pago_Efectivo,
                'Pendiente' AS tipo_de_pago,
                Referencia_stp AS clabe, 
                CASE 
                    WHEN LEFT(Referencia_stp, 4) = '6461' THEN 'STP' 
                    ELSE 'VE POR MAS' 
                END AS tipo_clabe,
                Sucursal,
                Fecha_ultimo_pago_efectivo as ultimo_abono_efectivo,
                fecha_ultimo_pago_efectivo as fecha_ultimo_abono_efectivo,
                '' as calificacion_contacto,
                '' as segmentacion_campanas,
                '' as probabilidad_de_pago,
                '' as dia_de_pago_probable,
                CASE 
                    WHEN Bucket_Morosidad_Final = 'j) First Payment Default' THEN 'SI' 
                    ELSE 'NO' 
                END AS primer_pago,
                '' as promesa_de_pago, 
                '' as equipo,
                '' as campo, 
                '' as accion,
                '' as lider_campo,
                '' as territorio, 
                '' as corte_1130, 
                '' as pago1,
                '' as pago2,
                Dias_mora as respuesta,
                Saldo_total_capital,
                Monto_otorgado_2 as rango_capital,
                '' as dias_pago,
                Avance_pago_plazo as avance,
                Avance_pago_plazo as rango_avance,
                '' as cierre_gestor_3semanas, 
                '' as variable1,
                '' as variable2,
                '' as variable3,
                '' as variable4,
                '' as variable5,
                '' as variable6,
                '' as variable7,
                '' as variable8,
                '' as variable9,
                '' as variable10
            FROM tbl_segundometro_semana
            LIMIT {batch_size} OFFSET {offset};
        """

        df_google = pd.read_sql(query_google, conn_google)
        close_connection_google(conn_google)

        if df_google.empty:
            return jsonify({
                "status": "success",
                "page": page,
                "batch_size": batch_size,
                "total_registros": 0,
                "data": []
            })

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

        # --- Merge LEFT ---
        df_google['id_credit'] = df_google['id_credit'].astype(str)
        df_aws['id_oferta'] = df_aws['id_oferta'].astype(str)

        df_merged = pd.merge(df_google, df_aws,
                             left_on='id_credit',
                             right_on='id_oferta',
                             how='left')
        if 'id_oferta' in df_merged.columns:
            df_merged.drop(columns=['id_oferta'], inplace=True)

        # --- Convertir a JSON ---
        data_json = df_merged.fillna("").to_dict(orient="records")

        return jsonify({
            "status": "success",
            "page": page,
            "batch_size": batch_size,
            "total_registros": len(data_json),
            "data": data_json
        })

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return jsonify({"status": "error", "message": str(e)}), 500
