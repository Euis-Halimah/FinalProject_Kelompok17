from datetime import datetime
import pandas as pd  
from airflow import DAG  
from airflow.operators.python_operator import PythonOperator  
from airflow.hooks.postgres_hook import PostgresHook 
 
def update_order_item():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()

    update_query = """
    UPDATE order_item
    SET coupon_id = CASE
        WHEN coupon_id IS NULL THEN '0'
        WHEN coupon_id = '1.0' THEN '1'
        WHEN coupon_id = '2.0' THEN '2'
        WHEN coupon_id = '3.0' THEN '3'
        WHEN coupon_id = '4.0' THEN '4'
        WHEN coupon_id = '5.0' THEN '5'
        WHEN coupon_id = '6.0' THEN '6'
        WHEN coupon_id = '7.0' THEN '7'
        WHEN coupon_id = '8.0' THEN '8'
        WHEN coupon_id = '9.0' THEN '9'
        WHEN coupon_id = '10.0' THEN '10'
        ELSE coupon_id
    END;
    """

    # Eksekusi query untuk melakukan update
    with engine.connect() as connection:
        result = connection.execute(update_query)

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
        CASE WHEN c.discount_percent IS NULL THEN 0 ELSE c.discount_percent END AS discount_percent, 
        o.status, 
        o.created_at
    FROM "order" o
    JOIN order_item oi ON o.id = oi.order_id
    LEFT JOIN coupons c ON oi.coupon_id::integer = c.id;
    """

    # Eksekusi query dan simpan hasilnya ke dalam dataframe
    customer_df = pd.read_sql(transformed_customer_query, engine)
    product_df = pd.read_sql(merged_product_query, engine)
    order_df = pd.read_sql(merged_order_query, engine)

    # Simpan data yang sudah ditransform ke dalam PostgreSQL menggunakan Pandas
    customer_df.to_sql('dw_customer', engine, if_exists='replace', index=False)
    product_df.to_sql('dw_product', engine, if_exists='replace', index=False)
    order_df.to_sql('dw_order', engine, if_exists='replace', index=False)

def data_mart():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    engine = hook.get_sqlalchemy_engine()
    # Ambil data dari PostgreSQL menggunakan SQL Query
    data_mart1 = """
    SELECT 
        cust_id, 
        first_name, 
        last_name,
        gender, 
        count(o.order_id) count_order
    FROM dw_customer c JOIN dw_order o
    ON c.cust_id = o.customer_id
    GROUP BY 1,2,3,4
    ORDER BY 4 DESC;
    """

    data_mart2 = """
    SELECT
        p.prod_id,
        p.prod_name,
        p.category,
        COUNT(o.order_id) AS total_purchases,
        p.price,
        SUM(o.amount) AS total_amount
    FROM
        dw_order o
    JOIN
        dw_product p ON o.product_id = p.prod_id
    GROUP BY 1,2,3,5
    ORDER BY 4 DESC;
    """
    
    data_mart3 = """
    SELECT
        p.category,
        COUNT(o.order_id) AS total_orders
    FROM
        dw_order o
    JOIN
        dw_product p ON o.product_id = p.prod_id
    GROUP BY 1
    ORDER BY 2 DESC;
    """

    data_mart4 = """
    SELECT
        o.order_id,
        o.status,
        o.created_at,
        SUM(o.amount * p.price * (1 - o.discount_percent/ 100.0)) AS total_revenue
    FROM
        dw_order o
    JOIN
        dw_product p ON o.product_id = p.prod_id
    GROUP BY 1,2,3;
    """

    # Eksekusi query dan simpan hasilnya ke dalam dataframe
    datamart1 = pd.read_sql(data_mart1, engine)
    datamart2 = pd.read_sql(data_mart2, engine)
    datamart3 = pd.read_sql(data_mart3, engine)
    datamart4 = pd.read_sql(data_mart4, engine)

    # Simpan data yang sudah ditransform ke dalam PostgreSQL menggunakan Pandas
    datamart1.to_sql('top_buyers', engine, if_exists='replace', index=False)
    datamart2.to_sql('top_product', engine, if_exists='replace', index=False)
    datamart3.to_sql('top_category', engine, if_exists='replace', index=False)
    datamart4.to_sql('total_revenue', engine, if_exists='replace', index=False)


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
    "Data_Modeling",                      
    default_args=default_args,              
    description="Data Modeling Kelompok 17",  
    schedule_interval="@once",              
    start_date=datetime(2023, 1, 1),        
    catchup=False,                          
)

# Mendefinisikan tugas(task) untuk menjalankan fungsi-fungsi
t1 = PythonOperator(
     task_id="update_order_item",             
     python_callable=update_order_item,      
     dag=dag,                               
)

t2 = PythonOperator(
     task_id="transform_data",             
     python_callable=transform_data,      
     dag=dag,                               
)

t3 = PythonOperator(
     task_id="data_mart",             
     python_callable=data_mart,      
     dag=dag,                               
)


# set dependencies
t1 >> t2 >> t3
