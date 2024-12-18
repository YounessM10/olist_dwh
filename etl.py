import os
import pandas as pd
from sqlalchemy import create_engine

# Chemin vers le Data Lake
data_lake_path = "./DataLake"
# Connexion PostgreSQL
engine = create_engine('postgresql+psycopg2://postgres:postgresql@localhost/olist_dwh')

# Fonction pour charger les données dans PostgreSQL
def load_data_to_sql(table_name, dataframe):
    try:
        dataframe.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Table {table_name} chargée avec succès.")
    except Exception as e:
        print(f"Erreur lors du chargement de la table {table_name}: {e}")

# #custommer
columns_keep_custommers = ['customer_id', 'customer_city', 'customer_state']
customers = pd.read_csv(os.path.join(data_lake_path, "customers/olist_customers_dataset.csv"), usecols=columns_keep_custommers)
load_data_to_sql('dim_customers', customers)

# #sellers
columns_keep_sellers = ['seller_id', 'seller_city']
sellers = pd.read_csv(os.path.join(data_lake_path, "sellers/olist_sellers_dataset.csv"), usecols=columns_keep_sellers)
load_data_to_sql('dim_sellers', sellers)

# #products
columns_keep_products = ['product_id', 'product_category_name']
products = pd.read_csv(os.path.join(data_lake_path, "products/olist_products_dataset.csv"), usecols=columns_keep_products)
products.rename(columns={'product_category_name': 'category'}, inplace=True)
load_data_to_sql('dim_products', products)

# #dim_time
columns_keep_orders = ['order_id', 'order_purchase_timestamp']
orders = pd.read_csv(os.path.join(data_lake_path, "orders/olist_orders_dataset.csv"), usecols=columns_keep_orders)
orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
orders['order_month'] = orders['order_purchase_timestamp'].dt.month
orders['order_year'] = orders['order_purchase_timestamp'].dt.year
orders['order_quarter'] = orders['order_purchase_timestamp'].dt.quarter
orders = orders[["order_id", 'order_purchase_timestamp', "order_month", "order_year", "order_quarter"]]
load_data_to_sql('dim_time', orders)

#fact_sales
#payment
columns_keep_payments = ['order_id', 'payment_value']
payments = pd.read_csv(os.path.join(data_lake_path, "payments/olist_order_payments_dataset.csv"), usecols=columns_keep_payments)
#count
columns_keep_orders_items = ['order_id', 'order_item_id', 'seller_id', 'product_id']
order_items = pd.read_csv(os.path.join(data_lake_path, "orders/olist_order_items_dataset.csv"), usecols=columns_keep_orders_items)
order_item_count = order_items.groupby('order_id')['order_item_id'].count().reset_index()
order_item_count.rename(columns={'order_item_id': 'quantity'}, inplace=True)

columns_keep_orders = ['order_id', 'customer_id', 'order_purchase_timestamp']
orders = pd.read_csv(os.path.join(data_lake_path, "orders/olist_orders_dataset.csv"), usecols=columns_keep_orders)

fact_sales = pd.merge(payments, orders, on='order_id', how='inner')
fact_sales = fact_sales.merge(order_item_count, on='order_id', how='inner')
fact_sales = fact_sales.merge(order_items, on='order_id', how='inner')

# Sélectionner les colonnes finales
fact_sales = fact_sales[["order_id", "customer_id", "product_id", "seller_id", "payment_value", "quantity"]]
load_data_to_sql('fact_sales', fact_sales)
