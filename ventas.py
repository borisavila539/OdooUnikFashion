from datetime import datetime
import pytz
import xmlrpc.client
import pandas as pd
import pyodbc
import json
from prefect import flow, task 
from pathlib import Path



prod_connection_string = "DRIVER={ODBC Driver 17 for SQL Server};Server=CUBO-INTERMODA;Database=IMClientesIV;UID=iditm;PWD=Int3r-M0d@.Id@;Trusted_Connection=no;"
url = "https://unikfashiongt.odoo.com"
db = "rocketgithub-unikfashiongt-odoo-sh-main-25251833"
username = "rmartinez@intermoda.com.hn"
password = "Intermod@2026/?"


def generate_flow_run_name():
    date = datetime.now(pytz.timezone('America/Tegucigalpa'))
    return f"{date:%Y-%m-%d %H:%M:%S}"

#obtener detalle del código de barra
def obtener_CodigoDeBarraInfo(codigo_barra):
    with pyodbc.connect(prod_connection_string) as conn:
        cursor = conn.cursor()
        query = "EXEC dbo.SP_GetCodigosDeBarraInfo ?;"
        json_data = '[{"CodigoBarra":' + codigo_barra + '}]'
        cursor.execute(query, json_data )
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns)
        return df

#Enviar datos a la base de datos de Intermoda
def enviar_IntermodaUnikFashion(json_data):
    """
    Recibe json_data como string JSON o como lista de diccionarios.
    Inserta o actualiza ventas diarias en [IMClientesIV].[dbo].[Ventas].
    """

    if isinstance(json_data, str):
        ventas = json.loads(json_data)
    else:
        ventas = json_data

    if isinstance(ventas, dict):
        ventas = [ventas]

    with pyodbc.connect(prod_connection_string) as conn:
        cursor = conn.cursor()

        for venta in ventas:
            update_query = """
                UPDATE [IMClientesIV].[dbo].[Ventas]
                SET
                    Cantidad =  ?,
                    Ganancia = ?,
                    ImporteTotal =  ?,
                    GananciaTotal = ?,
                    CostoTotal =  ?,
                    FechaModificacion = GETDATE()
                WHERE Fecha = ?
                  AND Tienda = ?
                  AND CodigoBarra = ?;
            """

            cursor.execute(update_query, (
                venta["Cantidad"],
                venta["Ganancia"],
                venta["ImporteTotal"],
                venta["GananciaTotal"],
                venta["CostoTotal"],
                venta["Fecha"],
                venta["Tienda"],
                venta["CodigoBarra"]
            ))

            if cursor.rowcount == 0:
                insert_query = """
                    INSERT INTO [IMClientesIV].[dbo].[Ventas] (
                        CodigoCliente,
                        Cliente,
                        CodigoTienda,
                        Tienda,
                        FechaCreacion,
                        FechaModificacion,
                        Fecha,
                        Año,
                        Semestre,
                        Trimestre,
                        NoMes,
                        Mes,
                        Semana,
                        DiaSemana,
                        Dia,
                        CodigoBarra,
                        CodigoArticulo,
                        Descripcion,
                        CodigoColor,
                        Color,
                        Talla,
                        Linea,
                        Sublinea,
                        Categoria,
                        Base,
                        Genero,
                        Clasificacion,
                        LoteOrigen,
                        Costo,
                        Precio,
                        Cantidad,
                        Ganancia,
                        ImporteTotal,
                        GananciaTotal,
                        CostoTotal
                    )
                    VALUES (
                        ?, ?, NULL, ?,
                        GETDATE(), GETDATE(),
                        ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?,?
                    );
                """

                cursor.execute(insert_query, (
                    venta["CodigoCliente"],
                    venta["Cliente"],
                    #venta["CodigoTienda"],
                    venta["Tienda"],
                    venta["Fecha"],
                    venta["Año"],
                    venta["Semestre"],
                    venta["Trimestre"],
                    venta["NoMes"],
                    venta["Mes"],
                    venta["Semana"],
                    venta["DiaSemana"],
                    venta["Dia"],
                    venta["CodigoBarra"],
                    venta["CodigoArticulo"],
                    venta["Descripcion"],
                    venta["CodigoColor"],
                    venta["Color"],
                    venta["Talla"],
                    venta["Linea"],
                    venta["Sublinea"],
                    venta["Categoria"],
                    venta["Base"],
                    venta["Genero"],
                    venta["Clasificacion"],
                    venta["LoteOrigen"],
                    venta["Costo"],
                    venta["Precio"],
                    venta["Cantidad"],
                    venta["Ganancia"],
                    venta["ImporteTotal"],
                    venta["GananciaTotal"],
                    venta["CostoTotal"]
                ))

        conn.commit()



@flow(name='Ventas UnikFashion', flow_run_name=generate_flow_run_name, retries=0, retry_delay_seconds=60)
def ventas():

    common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
    uid = common.authenticate(db, username, password, {})

    #obtener las facturas de venta
    models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
    fecha_inicio = datetime.now().date()- pd.Timedelta(days=1)
    fecha_inicio = fecha_inicio.strftime('%Y-%m-%d')
    invoice = pd.DataFrame(models.execute_kw(db, uid, password, 'pos.order', 'search_read',[[['date_order','>',fecha_inicio]]]))
    print(f"Facturas encontradas desde {fecha_inicio}: {len(invoice)}")

    #Obtener el cliente 
    with pyodbc.connect(prod_connection_string) as conn:
        cursor = conn.cursor()
        cursor.execute("EXEC dbo.SP_ObtenerClientes")

        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()

        df = pd.DataFrame.from_records(rows, columns=columns)

        conn.commit()
    Clientes = df[df['CodigoCliente'] == "IMGT-000001134"]

    #iterar sobre las facturas y obtener los productos vendidos para cada factura
    registros = []
    cantidad_lineas = 0

    for index, row in invoice.iterrows():
        order_id = row['id']
        products = pd.DataFrame(models.execute_kw(
            db, uid, password,
            'pos.order.line',
            'search_read',
            [[['order_id', '=', order_id]]]
        ))

        tienda = row['config_id'][1] if row['config_id'] else 'Desconocida'

        print(f"Procesando factura: {row['pos_reference']}: {len(products)} productos")

        for index, product in products.iterrows():
            product_odoo = pd.DataFrame(models.execute_kw(
                db, uid, password,
                'product.product',
                'search_read',
                [[['id', '=', product['product_id'][0]]]],
                {'limit': 1}
            )).rename(columns={'barcode': 'CodigoBarra'})

            codigo_barra = (
                product_odoo['CodigoBarra'].values[0]
                if 'CodigoBarra' in product_odoo.columns and len(product_odoo['CodigoBarra']) > 0
                else None
            )

            if pd.isna(codigo_barra) or codigo_barra in (False, '', None):
                continue

            try:
                results = obtener_CodigoDeBarraInfo(codigo_barra)
            except Exception as e:
                print(f"Error al obtener información para el código de barra {codigo_barra}: {e}")
                continue

            if results.empty:
                print(f"No se encontró información para el código de barra {codigo_barra}")
                continue

            fecha_venta_dt = (
                pd.to_datetime(row['date_order']).date()
                if 'date_order' in row and pd.notna(row['date_order'])
                else None
            )

            fecha_venta = fecha_venta_dt.strftime('%Y-%m-%d') if fecha_venta_dt else None

            json_data = {
                "CodigoCliente": Clientes['CodigoCliente'].values[0],
                "Cliente": Clientes['Cliente'].values[0],
                #"CodigoTienda": "",
                "Tienda": tienda,
                "Fecha": fecha_venta,
                "Año": fecha_venta_dt.year if fecha_venta_dt else None,
                "Semestre": "S1" if fecha_venta_dt and fecha_venta_dt.month <= 6 else "S2" if fecha_venta_dt else None,
                "Trimestre": (
                    "Q1" if fecha_venta_dt and fecha_venta_dt.month <= 3 else
                    "Q2" if fecha_venta_dt and fecha_venta_dt.month <= 6 else
                    "Q3" if fecha_venta_dt and fecha_venta_dt.month <= 9 else
                    "Q4" if fecha_venta_dt else None
                ),
                "NoMes": fecha_venta_dt.month if fecha_venta_dt else None,
                "Mes": fecha_venta_dt.strftime('%B') if fecha_venta_dt else None,
                "Semana": fecha_venta_dt.isocalendar()[1] if fecha_venta_dt else None,
                "DiaSemana": fecha_venta_dt.strftime('%A') if fecha_venta_dt else None,
                "Dia": fecha_venta_dt.day if fecha_venta_dt else None,
                "CodigoBarra": codigo_barra,
                "CodigoArticulo": results['CodigoArticulo'].values[0] if 'CodigoArticulo' in results.columns else None,
                "Descripcion": results['Descripcion'].values[0] if 'Descripcion' in results.columns else '',
                "CodigoColor": results['CodigoColor'].values[0] if 'CodigoColor' in results.columns else '',
                "Color": results['Color'].values[0] if 'Color' in results.columns else '',
                "Talla": results['Talla'].values[0] if 'Talla' in results.columns else '',
                "Linea": results['Linea'].values[0] if 'Linea' in results.columns else '',
                "Sublinea": results['Sublinea'].values[0] if 'Sublinea' in results.columns else '',
                "Categoria": results['Categoria'].values[0] if 'Categoria' in results.columns else '',
                "Base": results['Base'].values[0] if 'Base' in results.columns else '',
                "Genero": results['Genero'].values[0] if 'Genero' in results.columns else '',
                "Clasificacion": results['Clasificacion'].values[0] if 'Clasificacion' in results.columns else '',
                "LoteOrigen": results['LoteOrigen'].values[0] if 'LoteOrigen' in results.columns else '',
                "Costo": 0,
                "Precio": product['price_unit'],
                "Cantidad": int(product['qty']),
                "Ganancia": 0,
                "ImporteTotal": (
                    product['price_unit'] * product['qty']
                    if pd.notna(product['price_unit']) and pd.notna(product['qty'])
                    else 0
                ),
                "GananciaTotal": 0,
                "CostoTotal": 0
            }

            registros.append(json_data)
            cantidad_lineas += 1
    df = pd.DataFrame(registros)

    columnas_sumar = ["Cantidad", "ImporteTotal", "GananciaTotal", "CostoTotal"]

    columnas_agrupar = [
        col for col in df.columns
        if col not in columnas_sumar
    ]

    df_agrupado = (
        df.groupby(columnas_agrupar, dropna=False, as_index=False)[columnas_sumar]
        .sum()
    )

    for _, row in df_agrupado.iterrows():
        json_data = json.dumps(row.to_dict(), ensure_ascii=False)
        print(json_data)
        enviar_IntermodaUnikFashion(json_data)

    print("Proceso completado")

            