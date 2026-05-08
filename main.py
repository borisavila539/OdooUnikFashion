
# Configuración de conexión a Odoo
url = "https://unikfashiongt.odoo.com"
db = "rocketgithub-unikfashiongt-odoo-sh-main-25251833"
username = "rmartinez@intermoda.com.hn"
password = "Intermod@2026/?"

#Autenticación con Odoo
import xmlrpc.client
import pandas as pd
common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
uid = common.authenticate(db, username, password, {})

#Obtener el producto por su código de barras
models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")
products = pd.DataFrame(models.execute_kw(db, uid, password, 'product.product', 'search_read', [[['display_name', '=', "7423353729863 PepeCamiHoS/mPR903T-XS 55"]]] ,{'limit': 10})).rename(columns={'barcode': 'CodigoBarra'})

print(products[['CodigoBarra','qty_available']])

#validar si se encontró el producto


