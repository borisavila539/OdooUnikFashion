prod_connection_string = "DRIVER={ODBC Driver 17 for SQL Server};Server=CUBO-INTERMODA;Database=IMClientesIV;UID=iditm;PWD=Int3r-M0d@.Id@;Trusted_Connection=no;"
url = "https://unikfashiongt.odoo.com"
db = "rocketgithub-unikfashiongt-odoo-sh-main-25251833"
username = "rmartinez@intermoda.com.hn"
password = "Intermod@2026/?"

#Autenticación con Odoo
from datetime import datetime
import xmlrpc.client
import pandas as pd
import pyodbc
import json


common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
uid = common.authenticate(db, username, password, {})

#Obtener el producto por su código de barras
models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
products = pd.DataFrame(models.execute_kw(db, uid, password, 'stock.move.line', 'search_read',[[['quantity','>',0]]]))

#obtener los clientes
with pyodbc.connect(prod_connection_string) as conn:
    cursor = conn.cursor()
    cursor.execute("EXEC dbo.SP_ObtenerClientes")

    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()

    df = pd.DataFrame.from_records(rows, columns=columns)

    conn.commit()
Clientes = df[df['CodigoCliente'] == "IMGT-000001134"]

#obtener la información del producto por su código de barras
locations = products['location_dest_id'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None).unique()
with pyodbc.connect(prod_connection_string) as conn:
    cursor = conn.cursor()
    for location in locations:
        id_ubicacion = int(location)
        ubicacion = pd.DataFrame(models.execute_kw(db, uid, password, 'stock.location', 'search_read',[[['id','=',id_ubicacion]]],{'limit': 1}))        
        id_Almacen = ubicacion['location_id'][0]       
        almacen = pd.DataFrame(models.execute_kw(db, uid, password, 'stock.warehouse', 'search_read',[[['code','=',id_Almacen[1]]]],{'limit': 1}))
        if almacen.empty:
            print(f"Almacén no encontrado para ubicación: {location}")
            continue
        tienda =almacen['name'][0]     

        json_data_todo = '['
        #obtener productos por location_dest_id
        products_location = products[products['location_dest_id'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None) == location]
        print(f"Procesando Tienda: {tienda}: {len(products_location)} productos")
        
        for index, row in products_location.iterrows():
            product = pd.DataFrame(models.execute_kw(db, uid, password, 'product.product', 'search_read',[[['id','=',row['product_id'][0]]]],{'limit': 1})).rename(columns={'barcode': 'CodigoBarra'})
            codigo_barra = product['CodigoBarra'].iloc[0]
            if pd.isna(codigo_barra) or codigo_barra in (False, '', None):
                continue

            
            query = "EXEC dbo.SP_GetCodigosDeBarraInfo ?;"
            json_data = '[{"CodigoBarra":' + codigo_barra + '}]'
            #print(json_data)
            cursor.execute(query, json_data)
            columns = [column[0]  for column in cursor.description]
            rows = cursor.fetchall()
            results = pd.DataFrame.from_records(rows, columns=columns)

            create_date = datetime.strptime(product["create_date"].values[0], "%Y-%m-%d %H:%M:%S")

            
            #Crear json para enviar a la base de datos de Intermoda
            json_data = {
                "CodigoCliente": Clientes['CodigoCliente'].values[0],
                "Cliente": Clientes['Cliente'].values[0],
                "CodigoTienda": None,
                "Tienda": tienda,
                "FechaCreacion": create_date.strftime("%Y-%m-%d"),
                "Año": create_date.year,
                "NoMes": create_date.month,
                "Mes": create_date.strftime("%B"),
                "Dia": create_date.day,
                "CodigoBarra": product['CodigoBarra'].values[0],
                "CodigoArticulo": results['CodigoArticulo'].values[0],
                "Descripcion": results['Descripcion'].values[0],
                "CodigoColor": results['CodigoColor'].values[0],
                "Color": results['Color'].values[0],
                "Talla": results['Talla'].values[0],
                "Linea": results['Linea'].values[0],
                "Sublinea": results['Sublinea'].values[0],
                "Categoria": results['Categoria'].values[0],
                "Base": results['Base'].values[0],
                "Genero": results['Genero'].values[0],
                "Clasificacion": results['ClasificacionAX'].values[0],
                "LoteOrigen": results['LoteOrigen'].values[0],
                "PedidoVenta": None,
                "FechaFactura": None,
                "Costo": None,
                "Precio": float(product['list_price'].values[0]),
                "Cantidad": int(row['quantity']),
                "CostoTotal": None,
            }        
            json_data = json.dumps(json_data,ensure_ascii=False)
            json_data_todo += json_data + ','        

        #Enviar json a la base de datos de Intermoda
        json_data_todo = json_data_todo[:-1] + ']'
        print('json listo para enviar a la base de datos de Intermoda, enviando...')
        query = "EXEC dbo.SP_InsertarInventarioUnikFashion ?,?,?;"

        cursor.execute(query, json_data_todo,Clientes['CodigoCliente'].values[0],tienda)
        
        print(f'Datos insertados correctamente en {tienda}')
print('Proceso completado')

