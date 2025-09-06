import os
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.types import String, Integer, DateTime, Date, DECIMAL, Text
from dotenv import load_dotenv

# Configuracion basica de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Config
load_dotenv()
DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', 'ItcaTesis2025?')
DB_HOST = os.getenv('DB_HOST', '127.0.0.1')
DB_PORT = os.getenv('DB_PORT', '3306')
DB_NAME = os.getenv('DB_NAME', 'olist_db')

CSV_ORDERS = r'.\data\olist_orders_dataset.csv'
CSV_PAYMENTS = r'.\data\olist_order_payments_dataset.csv'
CSV_REVIEWS = r'.\data\olist_order_reviews_dataset.csv'

engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}', pool_pre_ping=True)

# Helpers

def parser_datetimes(df: pd.DataFrame, cols):
    """Convierte columnas a datetime (ignora vacios y errores)"""
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors='coerce', utc=False)
    return df

def replace_na_str(df: pd.DataFrame, cols):
    """Reemplaza NaN en columnas string por cadena vacia"""
    for c in cols:
        if c in df.columns:
            df[c] = df[c].where(pd.notnull(df[c]), None)
    return df

def load_df_csv(path, dtype=None):
    logging.info(f'Leyendo CSV: {path}')
    return pd.read_csv(path, dtype=dtype, keep_default_na=True, na_values=['', 'NULL', "null", 'NaN'])

def upsert_append(df, table_name, dtype_map):
    """Inserta por lotes (append)"""
    if df.empty:
        logging.warning(f'[{table_name}] DataFrame vacio, no se carga nada')
        return
    logging.info(f'Cargando {len(df):,} filas a {table_name}...')
    df.to_sql(
        table_name,
        engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=5000,
        dtype=dtype_map
    )
    logging.info(f'Ok: {table_name}')

# ------------
# ORDERS
# ------------
orders_dtype = {
    'order_id': str,
    'customer_id': str,
    'order_status': str,
}
orders = load_df_csv(CSV_ORDERS, dtype=orders_dtype)

# Verifica la información del DataFrame
logging.info("Orders DataFrame Info:")
logging.info(orders.info())

# Si quieres ver los primeros registros
logging.info("Orders DataFrame Sample:")
logging.info(orders.head())

# Verificar si hay duplicados en el DataFrame
logging.info(f"Número de duplicados antes de eliminar: {orders.duplicated(subset=['order_id']).sum()}")

orders = parser_datetimes(orders, [
    'order_purchase_timestamp',
    'order_approved_at',
    'order_delivered_carrier_date',
    'order_delivered_customer_date',
    'order_estimated_delivery_date'
])
orders = replace_na_str(orders, ['order_id', 'customer_id', 'order_status'])

# Dedup por PK
orders = orders.drop_duplicates(subset=['order_id'])

# Verifica las filas duplicadas con diferentes valores en otras columnas
duplicates = orders[orders.duplicated(subset=['order_id'], keep=False)]
logging.info(f"Duplicados encontrados:\n{duplicates}")

# Verificar que no hay duplicados después de limpiar
logging.info(f"Número de duplicados después de eliminar: {orders.duplicated(subset=['order_id']).sum()}")

#Dtype map para MySQL
orders_sqlalchemy_types = {
    'order_id': String(50),
    'customer_id': String(50),
    'order_status': String(20),
    'order_purchase_timestamp': DateTime(),
    'order_approved_at': DateTime(),
    'order_delivered_carrier_date': DateTime(),
    'order_delivered_customer_date': DateTime(),
    'order_estimated_delivery_date': DateTime(),
}

upsert_append(orders, 'olist_orders_dataset', orders_sqlalchemy_types)

# ------------
# PAYMENTS
# ------------
pay_dtype = {
    'order_id': str,
    'payment_sequential': 'Int64',
    'payment_type': str,
    'payment_installments': 'Int64',
    'payment_value': 'float',
}
payments = load_df_csv(CSV_PAYMENTS, dtype=pay_dtype)

# Verifica la información del DataFrame
logging.info("Payments DataFrame Info:")
logging.info(payments.info())

logging.info("Payments DataFrame Sample:")
logging.info(payments.head())

# Verificar si hay duplicados en el DataFrame
logging.info(f"Número de duplicados antes de eliminar: {payments.duplicated(subset=['order_id', 'payment_sequential']).sum()}")

payments = replace_na_str(payments, ['order_id', 'payment_type'])

# Limpieza basica
payments['payment_sequential'] = payments['payment_sequential'].fillna(0).astype('Int64')
payments['payment_installments'] = payments['payment_installments'].fillna(0).astype('Int64')
payments['payment_value'] = payments['payment_value'].fillna(0.0)

# Dedup por PK compuesta
payments = payments.drop_duplicates(subset=['order_id', 'payment_sequential'])

# Verificar que no hay duplicados después de limpiar
logging.info(f"Número de duplicados después de eliminar: {payments.duplicated(subset=['order_id', 'payment_sequential']).sum()}")

# Validacion FK: solo pagos con order_id existente
logging.info('Validando FKs de payments contra orders...')
with engine.connect() as cn:
    existing_orders = pd.read_sql(text('SELECT order_id FROM olist_orders_dataset'), cn)
ok_orders_ids = set(existing_orders['order_id'])
before = len(payments)
payments = payments[payments['order_id'].isin(ok_orders_ids)]
logging.info(f'Payments filtrados por FK: {before:,} -> {len(payments):,}')

payments_sqlalchemy_types = {
    'order_id': String(50),
    'payment_sequential': Integer(),
    'payment_type': String(20),
    'payment_installments': Integer(),
    'payment_value': DECIMAL(10, 2),
}
upsert_append(payments, 'olist_order_payments_dataset', payments_sqlalchemy_types)

# ------------
# REVIEWS
# ------------
rev_dtype = {
    'review_id': str,
    'order_id': str,
    'review_score': 'Int64',
    'review_comment_title': str,
    'review_comment_message': str,
    'review_creation_date': str,
    'review_answer_timestamp': str,
}
reviews = load_df_csv(CSV_REVIEWS, dtype=rev_dtype)

# Verifica la información del DataFrame
logging.info("Reviews DataFrame Info:")
logging.info(reviews.info())

logging.info("Reviews DataFrame Sample:")
logging.info(reviews.head())

reviews = parser_datetimes(reviews, ['review_answer_timestamp'])

# review_creation_date viene como fecha (YYYY-MM-DD), lo convertimos a date
if 'review_creation_date' in reviews.columns:
    reviews['review_creation_date'] = pd.to_datetime(
        reviews['review_creation_date'], errors = 'coerce'
    ).dt.date

reviews = replace_na_str(reviews, ['review_id', 'order_id', 'review_comment_title', 'review_comment_message'])
reviews['review_score'] = reviews['review_score'].fillna(0).astype('Int64')

# Verificar el número de registros que no tienen un `order_id` válido
invalid_reviews = reviews[~reviews['order_id'].isin(ok_orders_ids)]
logging.info(f"Número de reviews con order_id no válido: {len(invalid_reviews)}")

# Verificar duplicados con valores nulos
logging.info(f"Número de duplicados con valores nulos en review_id: {reviews[reviews['review_id'].isnull()].duplicated(subset=['review_id']).sum()}")

# Verifica duplicados en el CSV
logging.info(f"Número de duplicados en reviews antes de limpiar: {reviews.duplicated(subset=['review_id']).sum()}")

# Dedup por PK
reviews = reviews.drop_duplicates(subset=['review_id'])

logging.info(f"Número de duplicados después de limpiar: {reviews.duplicated(subset=['review_id']).sum()}")

# Validacion FK: solo reviews con order_id existente
logging.info('Validando FKs de reviews contra orders...')
before = len(reviews)
reviews = reviews[reviews['order_id'].isin(ok_orders_ids)]
logging.info(f'Reviews filtradas por FK: {before:,} -> {len(reviews):,}')

reviews_sqlalchemy_types = {
    'review_id': String(50),
    'order_id': String(50),
    'review_score': Integer(),
    'review_comment_title': String(255),
    'review_comment_message': Text(),
    'review_creation_date': Date(),
    'review_answer_timestamp': DateTime(),
}
upsert_append(reviews, 'olist_order_reviews_dataset', reviews_sqlalchemy_types)

#------------
# Validaciones Post-carga
#------------
with engine.begin() as cn:
    n_orders = cn.execute(text('SELECT COUNT(*) FROM olist_orders_dataset')).scalar()
    n_payments = cn.execute(text('SELECT COUNT(*) FROM olist_order_payments_dataset')).scalar()
    n_reviews = cn.execute(text('SELECT COUNT(*) FROM olist_order_reviews_dataset')).scalar()
    logging.info(f'Filas en olist_orders_dataset: {n_orders:,} | olist_order_payments_dataset: {n_payments:,} | olist_order_reviews_dataset: {n_reviews:,}')

logging.info("ETL finalizado con éxito")