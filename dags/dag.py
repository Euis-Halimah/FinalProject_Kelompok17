from datetime import datetime
import pandas as pd  
import glob  
import os  
import fastavro
import logging
from airflow import DAG  
from airflow.operators.python_operator import PythonOperator  
from airflow.hooks.postgres_hook import PostgresHook 
from sqlalchemy import create_engine 

# Fungsi untuk memproses data pelanggan
def ingest_customer_data():
    # Menggabungkan semua file CSV di direktori 'data' menjadi satu DataFrame dan menyimpannya ke /tmp/customer_data.csv
    pd.concat((pd.read_csv(file) for file in glob.glob(os.path.join("data", '*.csv'))), ignore_index=True).to_csv("/tmp/customer_data.csv", index=False)

    # Menginisialisasi koneksi PostgreSQL dan mesin SQLAlchemy
    hook = PostgresHook(postgres_conn_id="postgres_dw")  # Membuat koneksi PostgreSQL dengan ID koneksi "postgres_dw"
    engine = hook.get_sqlalchemy_engine() # Mendapatkan SQLAlchemy dari hook

    # Membaca data pelanggan dari /tmp/customer_data.csv dan mengganti tabel 'customers'
    pd.read_csv("/tmp/customer_data.csv").to_sql("customers", engine, if_exists="replace", index=False)

def ingest_login_attempt_data():
    # Menggabungkan semua file CSV di direktori 'data' menjadi satu DataFrame dan menyimpannya ke /tmp/customer_data.csv
    pd.concat((pd.read_json(file) for file in glob.glob(os.path.join("data", 'login*.json'))), ignore_index=True).to_json("/tmp/login_data.json", index=False)

    # Menginisialisasi koneksi PostgreSQL dan mesin SQLAlchemy
    hook = PostgresHook(postgres_conn_id="postgres_dw")  # Membuat koneksi PostgreSQL dengan ID koneksi "postgres_dw"
    engine = hook.get_sqlalchemy_engine() # Mendapatkan SQLAlchemy dari hook

    # Membaca data pelanggan dari /tmp/customer_data.csv dan mengganti tabel 'customers'
    pd.read_json("/tmp/login_data.json").to_sql("login_attempts", engine, if_exists="replace", index=False)

# Function to ingest data to PostgreSQL
def ingest_coupons_data():
    # Initialize the PostgreSQL hook and SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Read the data from CSV and insert into the table
    pd.read_json("data/coupons.json").to_sql("coupons", engine, if_exists="replace", index=False, method="multi")

# Function to ingest data to PostgreSQL
def ingest_product_data():
    # Initialize the PostgreSQL hook and SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Read the data from CSV and insert into the table
    pd.read_excel("data/product.xls").to_sql("product", engine, if_exists="replace", index=False, method="multi")

# Function to ingest data to PostgreSQL
def ingest_product_category_data():
    # Initialize the PostgreSQL hook and SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Read the data from CSV and insert into the table
    pd.read_excel("data/product_category.xls").to_sql("product_category", engine, if_exists="replace", index=False, method="multi")

# Function to ingest data to PostgreSQL
def ingest_supplier_data():
    # Initialize the PostgreSQL hook and SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Read the data from CSV and insert into the table
    pd.read_excel("data/supplier.xls").to_sql("supplier", engine, if_exists="replace", index=False, method="multi")

def ingest_order_data():
    # Initialize the PostgreSQL hook and SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Read the data from CSV and insert into the table
    pd.read_parquet("data/order.parquet", engine="fastparquet").to_sql("order", engine, if_exists="replace", index=False, method="multi")


# Define the ingest_data_avro_order_items function
def ingest_order_items_data(batch_size=1000):
    logging.info("Starting ingest_order_items_data")
    
    # Initialize the PostgreSQL hook and SQLAlchemy engine
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Path ke file Avro
    file_path = 'data/order_item.avro'

    # Membuka file Avro untuk membaca
    with open(file_path, 'rb') as f:
        reader = fastavro.reader(f)
        records = []
        for record in reader:
            records.append(record)
            if len(records) == batch_size:
                df = pd.DataFrame(records)
                df.to_sql("order_item", engine, if_exists="append", index=False, method="multi")
                logging.info(f"Ingested {len(records)} records")
                records = []

        # Ingest any remaining records
        if records:
            df = pd.DataFrame(records)
            df.to_sql("order_item", engine, if_exists="append", index=False, method="multi")
            logging.info(f"Ingested {len(records)} remaining records")

    logging.info("Finished ingest_order_items_data")

def transform_data():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    # Ambil data dari PostgreSQL menggunakan SQL Query
    transformed_customer_query = """
    SELECT 
        id AS cust_id, 
        first_name, 
        last_name, 
        gender, 
        address 
    FROM customers;
    """

    merged_product_query = """
    SELECT 
        p.id AS prod_id, 
        p.name AS prod_name, 
        p.price, 
        pc.name AS category, 
        s.name AS supplier, 
        s.country 
    FROM product p
    JOIN product_category pc ON p.category_id = pc.id
    JOIN supplier s ON p.supplier_id = s.id;
    """
    
    merged_order_query = """
    SELECT 
        o.id AS order_id, 
        o.customer_id, 
        oi.product_id, 
        oi.amount, 
        oi.coupon_id, 
        o.status, 
        o.created_at
    FROM "order" o
    JOIN order_item oi ON o.id = oi.order_id;
    """

    # Eksekusi query dan simpan hasilnya ke dalam dataframe
    customer_df = pd.read_sql(transformed_customer_query, engine)
    product_df = pd.read_sql(merged_product_query, engine)
    order_data = pd.read_sql(merged_order_query, engine)

    # Simpan data yang sudah ditransform ke dalam PostgreSQL menggunakan Pandas
    customer_df.to_sql('customer_df', engine, if_exists='replace', index=False)
    product_df.to_sql('product_df', engine, if_exists='replace', index=False)
    order_data.to_sql('order_data', engine, if_exists='replace', index=False)

def transform_data_order():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    # Ambil data dari PostgreSQL menggunakan SQL Query
    transformed_order = """
    SELECT 
        od.order_id, 
        od.customer_id, 
        od.product_id, 
        od.amount,  
        od.status, 
        od.created_at,
        COALESCE(c.discount_percent, 0) AS discount_percent
    FROM 
        order_data od
    LEFT JOIN 
        coupons c ON od.coupon_id = c.id;
    """
    order_df = pd.read_sql(transformed_order, engine)
    order_df.to_sql('order_df', engine, if_exists='replace', index=False)

# Menentukan argumen default untuk DAG
default_args = {
    "owner": "airflow",  
    "depends_on_past": False,  
    "email_on_failure": False,
    "email_on_retry": False,  
    "retries": 1,  
}

# Mendefinisikan DAG dengan atribut yang sudah ditentukan
dag = DAG(
    "ETL",                      
    default_args=default_args,              
    description="ETL Kelompok 17",  
    schedule_interval="@once",              
    start_date=datetime(2023, 1, 1),        
    catchup=False,                          
)

# Mendefinisikan tugas(task) untuk menjalankan fungsi customers_funnel
t1 = PythonOperator(
     task_id="ingest_customer_data",             
     python_callable=ingest_customer_data,      
     dag=dag,                               
)

t2 = PythonOperator(
     task_id="ingest_login_attempt_data",             
     python_callable=ingest_login_attempt_data,      
     dag=dag,                               
)

t3 = PythonOperator(
     task_id="ingest_coupons_data",             
     python_callable=ingest_coupons_data,      
     dag=dag,                               
)

t4 = PythonOperator(
     task_id="ingest_product_data",             
     python_callable=ingest_product_data,      
     dag=dag,                               
)

t5 = PythonOperator(
     task_id="ingest_product_category_data",             
     python_callable=ingest_product_category_data,      
     dag=dag,                               
)

t6 = PythonOperator(
     task_id="ingest_supplier_data",             
     python_callable=ingest_supplier_data,      
     dag=dag,                               
)

t7 = PythonOperator(
     task_id="ingest_order_data",             
     python_callable=ingest_order_data,      
     dag=dag,                               
)

t8 = PythonOperator(
     task_id="ingest_order_items_data",             
     python_callable=ingest_order_items_data,      
     dag=dag                              
)

t9 = PythonOperator(
     task_id="transform_data",             
     python_callable=transform_data,      
     dag=dag                              
)

t10 = PythonOperator(
     task_id="transform_data_order",             
     python_callable=transform_data_order,      
     dag=dag                              
)

# set dependencies
[t1, t3, t4, t5, t6, t7, t8, t2] >> t9 >> t10
