# %%
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

# %%
#Obtener el producto por su código de barras
models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
#products = pd.DataFrame(models.execute_kw(db, uid, password, 'product.product', 'search_read', [[['barcode', '=', '7423353733006']]] ,{'limit': 1})).rename(columns={'barcode': 'CodigoBarra'})
#products = pd.DataFrame(models.execute_kw(db, uid, password, 'product.product', 'search_read',[]))#,['location_id','=',180]]]))
products = pd.DataFrame(models.execute_kw(db, uid, password, 'stock.quant', 'search_read',[[['quantity','>',0]]],{'fields': ['product_id', 'location_id', 'quantity']}))#,['location_id','=',180]]]))
print(len(products))
#products

    


# %%
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


# %%
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

# %%
def obtenerCodigosDeBarra():
    with pyodbc.connect(prod_connection_string) as conn:
        cursor = conn.cursor()        
        query = "SELECT distinct CodigoBarra, CodigoArticulo,Descripcion,CodigoColor,Color,Talla,Linea,Sublinea,Categoria,Base,Genero,LoteOrigen FROM [IMClientesIV].[dbo].[DimCodigosBarra] "
        
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame.from_records(rows, columns=columns)
        return df

# %%
def enviar_IntermodaUnikFashion(sql, *params):
    with pyodbc.connect(prod_connection_string) as conn:
        cursor = conn.cursor()
        cursor.execute(sql, params)
        conn.commit()

# %%
def borrar_Invetario_Tienda(codigo_tienda):
    with pyodbc.connect(prod_connection_string) as conn:
        cursor = conn.cursor()
        query = f"delete  Inventario where Tienda = '{codigo_tienda}';"
        cursor.execute(query)
        conn.commit()

# %%
# 1. Extraer ids únicos
products['location_id_num'] = products['location_id'].apply(
    lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None
)

products['product_id_num'] = products['product_id'].apply(
    lambda x: x[0] if isinstance(x, list) and len(x) > 0 else None
)

location_ids = products['location_id_num'].dropna().astype(int).unique().tolist()
product_ids = products['product_id_num'].dropna().astype(int).unique().tolist()

ubicaciones = pd.DataFrame(models.execute_kw(
    db, uid, password,
    'stock.location',
    'search_read',
    [[['id', 'in', location_ids]]],
    {'fields': ['id', 'location_id']}
))

productos_odoo = pd.DataFrame(models.execute_kw(
    db, uid, password,
    'product.product',
    'search_read',
    [[['id', 'in', product_ids]]],
    {
        'fields': ['id', 'barcode', 'create_date', 'list_price'],
    }
)).rename(columns={'barcode': 'CodigoBarra'})

ubicaciones['codigo_almacen'] = ubicaciones['location_id'].apply(
    lambda x: x[1] if isinstance(x, list) and len(x) > 1 else None
)

codigos_almacen = ubicaciones['codigo_almacen'].dropna().unique().tolist()

almacenes = pd.DataFrame(models.execute_kw(
    db, uid, password,
    'stock.warehouse',
    'search_read',
    [[['code', 'in', codigos_almacen]]],
    {'fields': ['code', 'name']}
))

map_ubicacion_almacen = dict(zip(ubicaciones['id'], ubicaciones['codigo_almacen']))
map_almacen_tienda = dict(zip(almacenes['code'], almacenes['name']))
map_productos = productos_odoo.set_index('id').to_dict('index')

#obtener los codigos de barra para evitar consultas repetidas
codigos_barra = obtenerCodigosDeBarra().set_index('CodigoBarra').to_dict('index')

insert_sql = """
INSERT INTO Inventario (
    CodigoCliente, Cliente, CodigoTienda, Tienda, FechaCreacion,
    Año, NoMes, Mes, Dia, CodigoBarra, CodigoArticulo, Descripcion,
    CodigoColor, Color, Talla, Linea, Sublinea, Categoria, Base,
    Genero, LoteOrigen, PedidoVenta, FechaFactura,
    Costo, Precio, Cantidad, CostoTotal
)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""

with pyodbc.connect(prod_connection_string) as conn:
    cursor = conn.cursor()
    cursor.fast_executemany = True

    for location in location_ids:
        codigo_almacen = map_ubicacion_almacen.get(location)
        tienda = map_almacen_tienda.get(codigo_almacen)

        if not tienda:
            print(f"Almacén no encontrado para ubicación: {location}")
            continue

        borrar_Invetario_Tienda(tienda)

        products_location = products[products['location_id_num'] == location]

        print(f"Procesando Tienda: {tienda}: {len(products_location)} productos")

        registros = []

        for _, row in products_location.iterrows():
            #print(f"Procesando producto: {_} ")
            product = map_productos.get(row['product_id_num'])

            if not product:
                continue

            codigo_barra = product.get('CodigoBarra')

            if pd.isna(codigo_barra) or codigo_barra in (False, '', None):
                continue

            try:
                #validar si el código de barra ya existe en el diccionario para evitar consultas repetidas
                if codigo_barra in codigos_barra:
                    results = pd.DataFrame([codigos_barra[codigo_barra]])
                else:
                    results = obtener_CodigoDeBarraInfo(codigo_barra)
            except Exception as e:
                print(f"Error al obtener información para el código de barra {codigo_barra}: {e}")
                continue

            if results.empty:
                print(f"No se encontró información para el código de barra {codigo_barra}")
                continue

            create_date = datetime.strptime(product["create_date"], "%Y-%m-%d %H:%M:%S")

            registros.append((
                Clientes['CodigoCliente'].values[0],
                Clientes['Cliente'].values[0],
                None,
                tienda,
                create_date.strftime("%Y-%m-%d"),
                create_date.year,
                create_date.month,
                create_date.strftime("%B"),
                create_date.day,
                codigo_barra,
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
                #results['ClasificacionAX'].values[0],
                results['LoteOrigen'].values[0],
                None,
                None,
                None,
                float(product['list_price']),
                int(row['quantity']),
                None
            ))

        if registros:
            cursor.executemany(insert_sql, registros)
            conn.commit()

        print(f"Datos insertados correctamente en {tienda}")

print("Proceso completado")




