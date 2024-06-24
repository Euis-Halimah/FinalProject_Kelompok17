from datetime import datetime
import pandas as pd  
import glob  
import os  
import fastavro
import logging
from airflow import DAG  
from airflow.operators.python_operator import PythonOperator  
from airflow.hooks.postgres_hook import PostgresHook 
 

# Function untuk ingest data customer ke postgres
def ingest_customer_data():
 
    pd.concat((pd.read_csv(file) for file in glob.glob(os.path.join("data", '*.csv'))), ignore_index=True).to_csv("/tmp/customer_data.csv", index=False)

    # Menginisialisasi koneksi PostgreSQL dan mesin sqlalchemy
    hook = PostgresHook(postgres_conn_id="postgres_dw") 
    engine = hook.get_sqlalchemy_engine()

    # Membaca data customers dan menyimpannya ke postgres
    pd.read_csv("/tmp/customer_data.csv").to_sql("customers", engine, if_exists="replace", index=False)

# Function untuk ingest data login ke postgres
def ingest_login_attempt_data():
    # Menggabungkan semua file login
    pd.concat((pd.read_json(file) for file in glob.glob(os.path.join("data", 'login*.json'))), ignore_index=True).to_json("/tmp/login_data.json", index=False)

    # Menginisialisasi koneksi PostgreSQL dan mesin sqlalchemy 
    hook = PostgresHook(postgres_conn_id="postgres_dw")  
    engine = hook.get_sqlalchemy_engine() 

    # Membaca data login dan menyimpannya ke postgres
    pd.read_json("/tmp/login_data.json").to_sql("login_attempts", engine, if_exists="replace", index=False)

# Function untuk ingest data coupons ke postgres
def ingest_coupons_data():
    # Menginisialisasi koneksi PostgreSQL dan mesin sqlalchemy
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Membaca data coupons dan menyimpannya ke postgres
    pd.read_json("data/coupons.json").to_sql("coupons", engine, if_exists="replace", index=False, method="multi")

# Function untuk ingest data product ke postgres
def ingest_product_data():
    # Menginisialisasi koneksi PostgreSQL dan mesin sqlalchemy
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Membaca data product dan menyimpannya ke postgres
    pd.read_excel("data/product.xls").to_sql("product", engine, if_exists="replace", index=False, method="multi")

# Function untuk ingest data product category ke postgres
def ingest_product_category_data():
    # Menginisialisasi koneksi PostgreSQL dan mesin sqlalchemy
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Membaca data product category dan menyimpannya ke postgres
    pd.read_excel("data/product_category.xls").to_sql("product_category", engine, if_exists="replace", index=False, method="multi")

# Function untuk ingest data supplier ke postgres
def ingest_supplier_data():
    # Menginisialisasi koneksi PostgreSQL dan mesin sqlalchemy
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Membaca data supplier dan menyimpannya ke postgres
    pd.read_excel("data/supplier.xls").to_sql("supplier", engine, if_exists="replace", index=False, method="multi")

# Function untuk ingest data order ke postgres
def ingest_order_data():
    # Menginisialisasi koneksi PostgreSQL dan mesin sqlalchemy
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    # Membaca data order dan menyimpannya ke postgres
    pd.read_parquet("data/order.parquet", engine="fastparquet").to_sql("order", engine, if_exists="replace", index=False, method="multi")


# Function untuk ingest data order_item ke postgres
def ingest_order_items_data(batch_size=1000):
    logging.info("Starting ingest_order_items_data")
    
    # Menginisialisasi koneksi PostgreSQL dan mesin sqlalchemy
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

        # Ingest data ke postgres
        if records:
            df = pd.DataFrame(records)
            df.to_sql("order_item", engine, if_exists="append", index=False, method="multi")
            logging.info(f"Ingested {len(records)} remaining records")

    logging.info("Finished ingest_order_items_data")


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
    "Ingest_Data",                      
    default_args=default_args,              
    description="Ingest Data To Postgres",  
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


# set dependencies
[t1, t2, t3, t4, t5, t6, t7, t8] 
