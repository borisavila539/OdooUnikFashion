
#.venv 
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

common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
uid = common.authenticate(db, username, password, {})

#Obtener el producto por su código de barras
models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
products = pd.DataFrame(models.execute_kw(db, uid, password, 'stock.move.line', 'search_read',[[['quantity','>',0]]]))
print(len(products))
#products

    
#obtener los clientes
with pyodbc.connect(prod_connection_string) as conn:
    cursor = conn.cursor()
    cursor.execute("EXEC dbo.SP_ObtenerClientes")

    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()

    df = pd.DataFrame.from_records(rows, columns=columns)

    conn.commit()
Clientes = df[df['CodigoCliente'] == "IMGT-000001134"]
#print(Clientes)


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

def enviar_IntermodaUnikFashion(sql, *params):
    with pyodbc.connect(prod_connection_string) as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        conn.commit()

def borrar_Invetario_Tienda(codigo_tienda):
    with pyodbc.connect(prod_connection_string) as conn:
        cursor = conn.cursor()
        query = f"delete  Inventario where Tienda = '{codigo_tienda}';"
        cursor.execute(query)
        conn.commit()

#obtener la información del producto por su código de barras
locations = products['location_dest_id'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None).unique()

for location in locations:
    id_ubicacion = int(location)
    ubicacion = pd.DataFrame(models.execute_kw(db, uid, password, 'stock.location', 'search_read',[[['id','=',id_ubicacion]]],{'limit': 1}))        
    id_Almacen = ubicacion['location_id'][0]       
    almacen = pd.DataFrame(models.execute_kw(db, uid, password, 'stock.warehouse', 'search_read',[[['code','=',id_Almacen[1]]]],{'limit': 1}))
    if almacen.empty:
        print(f"Almacén no encontrado para ubicación: {location}")
        continue
    tienda =almacen['name'][0]     
    borrar_Invetario_Tienda(tienda) #Borrar el inventario de la tienda antes de insertar los nuevos datos

    json_data_todo = '['
    #obtener productos por location_dest_id
    products_location = products[products['location_dest_id'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None) == location]
    print(f"Procesando Tienda: {tienda}: {len(products_location)} productos")
    
    for index, row in products_location.iterrows():
        product = pd.DataFrame(models.execute_kw(db, uid, password, 'product.product', 'search_read',[[['id','=',row['product_id'][0]]]],{'limit': 1})).rename(columns={'barcode': 'CodigoBarra'})
        codigo_barra = product['CodigoBarra'].iloc[0]
        if pd.isna(codigo_barra) or codigo_barra in (False, '', None):
            continue 

        try:
            results = obtener_CodigoDeBarraInfo(codigo_barra) #pd.DataFrame.from_records(rows, columns=columns)
        except Exception as e:
            print(f"Error al obtener información para el código de barra {codigo_barra}: {e}")
            continue
        if results.empty:
            print(f"No se encontró información para el código de barra {codigo_barra}")
            continue

        create_date = datetime.strptime(product["create_date"].values[0], "%Y-%m-%d %H:%M:%S")
        insert_sql = """
        INSERT INTO Inventario (
            CodigoCliente, Cliente, CodigoTienda, Tienda, FechaCreacion,
            Año, NoMes, Mes, Dia, CodigoBarra, CodigoArticulo, Descripcion,
            CodigoColor, Color, Talla, Linea, Sublinea, Categoria, Base,
            Genero, Clasificacion, LoteOrigen, PedidoVenta, FechaFactura,
            Costo, Precio, Cantidad, CostoTotal
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """

        enviar_IntermodaUnikFashion(
            insert_sql,
            Clientes['CodigoCliente'].values[0],
            Clientes['Cliente'].values[0],
            None,
            tienda,
            create_date.strftime("%Y-%m-%d"),
            create_date.year,
            create_date.month,
            create_date.strftime("%B"),
            create_date.day,
            product['CodigoBarra'].values[0],
            results['CodigoArticulo'].values[0],
            results['Descripcion'].values[0],
            results['CodigoColor'].values[0],
            results['Color'].values[0],
            results['Talla'].values[0],
            results['Linea'].values[0],
            results['Sublinea'].values[0],
            results['Categoria'].values[0],
            results['Base'].values[0],
            results['Genero'].values[0],
            results['ClasificacionAX'].values[0],
            results['LoteOrigen'].values[0],
            None,
            None,
            None,
            float(product['list_price'].values[0]),
            int(row['quantity']),
            None
        )    
    print(f'Datos insertados correctamente en {tienda}')
print('Proceso completado')
   




