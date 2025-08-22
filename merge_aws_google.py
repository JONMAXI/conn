import pandas as pd

def merge_aws_google_batch():
    # Crear un DataFrame de ejemplo para no depender de la base de datos
    df_final = pd.DataFrame({
        'id_credito': [1, 2, 3],
        'nombre': ['Hola', 'Hola', 'Hola'],
        'Telefono': ['0000000000', '1111111111', '2222222222'],
        'pagos_vencidos': [1, 1, 1],
        'monto_vencido': [100, 200, 300],
        'tipoo_de_pago': ['Transferencia', 'Transferencia', 'Transferencia'],
        'clabe': ['0000', '1111', '2222'],
        'banco': ['STP', 'STP', 'STP'],
        'nombre_completo': ['Hola', 'Hola', 'Hola'],
        'nombre_completo_referencia1': ['Hola', 'Hola', 'Hola'],
        'telefono_referencia1': ['000', '111', '222'],
        'nombre_completo_referencia2': ['Hola', 'Hola', 'Hola'],
        'telefono_referencia2': ['000', '111', '222']
    })

    return df_final