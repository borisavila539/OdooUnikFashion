# %%
# .venv
prod_connection_string = "DRIVER={ODBC Driver 17 for SQL Server};Server=CUBO-INTERMODA;Database=IMClientesIV;UID=iditm;PWD=Int3r-M0d@.Id@;Trusted_Connection=no;"
url = "https://unikfashiongt.odoo.com"
db = "rocketgithub-unikfashiongt-odoo-sh-main-25251833"
username = "rmartinez@intermoda.com.hn"
password = "Intermod@2026/?"

# Autenticación con Odoo
from datetime import datetime
import xmlrpc.client
import pandas as pd
import pyodbc
import json

common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
uid = common.authenticate(db, username, password, {})
models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")

# %%
# ---------------------------------------------------------------------------
# 1) Facturas del rango de fechas
#    - Se piden solo los campos que realmente se usan (menos payload por XML-RPC)
# ---------------------------------------------------------------------------
fecha_inicio = (datetime.today() - pd.Timedelta(days=1)).replace(
    hour=6,
    minute=0,
    second=0,
    microsecond=0
)
fecha_fin = datetime.today().replace(
    hour=6,
    minute=0,
    second=0,
    microsecond=0
)
print(f"Rango de fechas: {fecha_inicio} a {fecha_fin}")

invoice = pd.DataFrame(
    models.execute_kw(
        db, uid, password,
        'pos.order', 'search_read',
        [[
            ['date_order', '>=', fecha_inicio],
            ['date_order', '<=', fecha_fin],
            ['state', 'in', [ 'invoiced']],#'paid', 'done',
            #['pos_reference', '=', 'Orden 00508-001-0001']
        ]],
        {'fields': ['id', 'pos_reference', 'config_id', 'date_order']}
    )
)
print(f"Facturas encontradas: {len(invoice)}")
#print(invoice[['id', 'pos_reference', 'config_id', 'date_order']])

if invoice.empty:
    raise SystemExit("No hay facturas para el rango de fechas indicado.")

invoice = invoice.rename(columns={'id': 'order_id'})
invoice['tienda'] = invoice['config_id'].apply(lambda x: x[1] if x else 'Desconocida')
# Ajuste de zona horaria (-6h), una sola vez para todas las facturas
invoice['date_order_local'] = pd.to_datetime(invoice['date_order']) - pd.Timedelta(hours=6)

# %%
# ---------------------------------------------------------------------------
# 2) Cliente fijo (igual que antes, una sola consulta)
# ---------------------------------------------------------------------------
with pyodbc.connect(prod_connection_string) as conn:
    cursor = conn.cursor()
    cursor.execute("EXEC dbo.SP_ObtenerClientes")
    columns = [c[0] for c in cursor.description]
    df_clientes = pd.DataFrame.from_records(cursor.fetchall(), columns=columns)

cliente_row = df_clientes[df_clientes['CodigoCliente'] == "IMGT-000001134"].iloc[0]

# %%
# ---------------------------------------------------------------------------
# 3) TODAS las líneas de venta en UNA SOLA llamada a Odoo
#    (antes: 1 llamada por factura)
# ---------------------------------------------------------------------------
order_ids = invoice['order_id'].tolist()

lineas = pd.DataFrame(
    models.execute_kw(
        db, uid, password,
        'pos.order.line', 'search_read',
        [[['order_id', 'in', order_ids]]],
        {'fields': ['order_id', 'product_id', 'price_unit', 'qty']}
    )
)
#print(f"Líneas de venta encontradas: {len(lineas)}")
#print(lineas)

if lineas.empty:
    raise SystemExit("Las facturas del rango no tienen líneas de venta.")

lineas['order_id_val'] = lineas['order_id'].apply(lambda x: x[0] if x else None)
lineas['product_id_val'] = lineas['product_id'].apply(lambda x: x[0] if x else None)

# Traemos tienda / fecha de la factura correspondiente (merge, no loop)
lineas = lineas.merge(
    invoice[['order_id', 'tienda', 'date_order_local', 'pos_reference']],
    left_on='order_id_val', right_on='order_id', how='left'
)

# %%
# ---------------------------------------------------------------------------
# 4) Info de producto (código de barra) para TODOS los product_id
#    en UNA SOLA llamada a Odoo (antes: 1 llamada por línea de venta)
# ---------------------------------------------------------------------------
product_ids = [int(p) for p in lineas['product_id_val'].dropna().unique().tolist()]

productos = pd.DataFrame(
    models.execute_kw(
        db, uid, password,
        'product.product', 'search_read',
        [[['id', 'in', product_ids]]],
        {'fields': ['id', 'barcode']}
    )
).rename(columns={'id': 'product_id_val', 'barcode': 'CodigoBarra'})

lineas = lineas.merge(productos, on='product_id_val', how='left')

# Descartar líneas sin código de barra válido (igual que el "continue" original)
lineas = lineas[
    lineas['CodigoBarra'].notna() & (lineas['CodigoBarra'] != False) & (lineas['CodigoBarra'] != '')
].copy()



# %%
# ---------------------------------------------------------------------------
# 5) Info de códigos de barra: UNA SOLA consulta SQL para TODOS los códigos
#    distintos (antes: 1 consulta por línea de venta -> el mayor cuello de botella)
# ---------------------------------------------------------------------------
def obtener_CodigosDeBarraInfo_bulk(codigos_barra):
    """Consulta en bloque la información de una lista de códigos de barra."""
    with pyodbc.connect(prod_connection_string) as conn:
        cursor = conn.cursor()
        query = "EXEC dbo.SP_GetCodigosDeBarraInfo ?;"
        # NOTA: se construye JSON válido con json.dumps (el original concatenaba
        # el código de barra sin comillas, lo que solo funciona si es numérico
        # y puede romperse con cualquier caracter no numérico).
        payload = json.dumps([{"CodigoBarra": cb} for cb in codigos_barra], ensure_ascii=False)
        cursor.execute(query, payload)
        columns = [c[0] for c in cursor.description]
        df = pd.DataFrame.from_records(cursor.fetchall(), columns=columns)
        return df

codigos_unicos = lineas['CodigoBarra'].unique().tolist()
info_barras = obtener_CodigosDeBarraInfo_bulk(codigos_unicos)

lineas = lineas.merge(info_barras, on='CodigoBarra', how='left')
# %%
# ---------------------------------------------------------------------------
# 6) Cálculo de campos de fecha, vectorizado (antes: fila por fila en el loop)
# ---------------------------------------------------------------------------
fecha_series = pd.to_datetime(lineas['date_order_local'])

lineas['Fecha'] = fecha_series.dt.strftime('%Y-%m-%d')
lineas['Año'] = fecha_series.dt.year
lineas['Semestre'] = fecha_series.dt.month.apply(lambda m: 'S1' if m <= 6 else 'S2')
lineas['Trimestre'] = fecha_series.dt.month.apply(
    lambda m: 'Q1' if m <= 3 else 'Q2' if m <= 6 else 'Q3' if m <= 9 else 'Q4'
)
lineas['NoMes'] = fecha_series.dt.month
lineas['Mes'] = fecha_series.dt.strftime('%B')
lineas['Semana'] = fecha_series.dt.isocalendar().week
lineas['DiaSemana'] = fecha_series.dt.strftime('%A')
lineas['Dia'] = fecha_series.dt.day

lineas['CodigoCliente'] = cliente_row['CodigoCliente']
lineas['Cliente'] = cliente_row['Cliente']
lineas['Tienda'] = lineas['tienda']
lineas['Precio'] = lineas['price_unit']
lineas['Cantidad'] = lineas['qty'].astype(int)
lineas['Costo'] = 0
lineas['Ganancia'] = 0
lineas['ImporteTotal'] = lineas['price_unit'] * lineas['qty']
lineas['GananciaTotal'] = 0
lineas['CostoTotal'] = 0

# Columnas de info_barras que podrían no venir si el SP no encontró el código
for col in ['CodigoArticulo', 'Descripcion', 'CodigoColor', 'Color', 'Talla',
            'Linea', 'Sublinea', 'Categoria', 'Base', 'Genero', 'Clasificacion', 'LoteOrigen']:
    if col not in lineas.columns:
        lineas[col] = ''
    else:
        lineas[col] = lineas[col].fillna('')

columnas_finales = [
    "CodigoCliente", "Cliente", "Tienda", "Fecha", "Año", "Semestre", "Trimestre",
    "NoMes", "Mes", "Semana", "DiaSemana", "Dia", "CodigoBarra", "CodigoArticulo",
    "Descripcion", "CodigoColor", "Color", "Talla", "Linea", "Sublinea", "Categoria",
    "Base", "Genero", "Clasificacion", "LoteOrigen", "Costo", "Precio", "Cantidad",
    "Ganancia", "ImporteTotal", "GananciaTotal", "CostoTotal"
]

df = lineas[columnas_finales].copy()
# %%
# ---------------------------------------------------------------------------
# 7) Agrupar (igual lógica que el script original)
# ---------------------------------------------------------------------------
columnas_sumar = ["Cantidad", "ImporteTotal", "GananciaTotal", "CostoTotal"]
columnas_agrupar = [col for col in df.columns if col not in columnas_sumar]

df_agrupado = (
    df.groupby(columnas_agrupar, dropna=False, as_index=False)[columnas_sumar]
    .sum()
)

# %%
# ---------------------------------------------------------------------------
# 8) Enviar a SQL Server con UNA SOLA CONEXIÓN reutilizada
#    (antes: abría/cerraba una conexión ODBC por cada registro, que es
#    la operación más cara después de las llamadas de red repetidas)
# ---------------------------------------------------------------------------
def enviar_IntermodaUnikFashion_bulk(df_ventas):
    update_query = """
        UPDATE [IMClientesIV].[dbo].[Ventas]
        SET
            Cantidad = ?,
            Ganancia = ?,
            ImporteTotal =  ?,
            GananciaTotal = ?,
            CostoTotal = ?,
            FechaModificacion = GETDATE()
        WHERE Fecha = ?
          AND Tienda = ?
          AND CodigoBarra = ?
          AND Precio = ?;
    """

    insert_query = """
        INSERT INTO [IMClientesIV].[dbo].[Ventas] (
            CodigoCliente, Cliente, CodigoTienda, Tienda,
            FechaCreacion, FechaModificacion,
            Fecha, Año, Semestre, Trimestre, NoMes, Mes, Semana, DiaSemana, Dia,
            CodigoBarra, CodigoArticulo, Descripcion, CodigoColor, Color, Talla,
            Linea, Sublinea, Categoria, Base, Genero, Clasificacion, LoteOrigen,
            Costo, Precio, Cantidad, Ganancia, ImporteTotal, GananciaTotal, CostoTotal
        )
        VALUES (
            ?, ?, NULL, ?,
            GETDATE(), GETDATE(),
            ?, ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?
        );
    """

    with pyodbc.connect(prod_connection_string) as conn:
        cursor = conn.cursor()
        for venta in df_ventas.to_dict('records'):
            cursor.execute(update_query, (
                venta["Cantidad"], venta["Ganancia"], venta["ImporteTotal"],
                venta["GananciaTotal"], venta["CostoTotal"],
                venta["Fecha"], venta["Tienda"], venta["CodigoBarra"],venta["Precio"]
            ))

            if cursor.rowcount == 0:
                cursor.execute(insert_query, (
                    venta["CodigoCliente"], venta["Cliente"], venta["Tienda"],
                    venta["Fecha"], venta["Año"], venta["Semestre"], venta["Trimestre"],
                    venta["NoMes"], venta["Mes"], venta["Semana"], venta["DiaSemana"], venta["Dia"],
                    venta["CodigoBarra"], venta["CodigoArticulo"], venta["Descripcion"],
                    venta["CodigoColor"], venta["Color"], venta["Talla"],
                    venta["Linea"], venta["Sublinea"], venta["Categoria"], venta["Base"],
                    venta["Genero"], venta["Clasificacion"], venta["LoteOrigen"],
                    venta["Costo"], venta["Precio"], venta["Cantidad"], venta["Ganancia"],
                    venta["ImporteTotal"], venta["GananciaTotal"], venta["CostoTotal"]
                ))
        conn.commit()
#mostrar el valor de Cantidad del primer registro del DataFrame df_agrupado
enviar_IntermodaUnikFashion_bulk(df_agrupado)

print("Proceso completado")