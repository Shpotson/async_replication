from random import random

from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

import psycopg2
import pyodbc

def get_ms_sql_conn():
    SERVER = 'orders-ms-sql-db'
    DATABASE = 'master'
    USERNAME = 'SA'
    PASSWORD = 'b90cf1ef-6535-4d32-81cd-278e306dd25953ACE3C0-5985-4E7A-B86B-3314B271FFE4#'

    connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes'

    conn = pyodbc.connect(connection_string)

    return conn

dag = DAG("create_mart", dagrun_timeout=timedelta(minutes=5), start_date=datetime(2024,12,29), schedule=timedelta(days=1), tags=["orders_system"])

def init_mart_tables(**kwargs):
    conn = get_ms_sql_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='dbo'")

    check_result = cursor.fetchall()

    client_mart_table_exist = False
    monthly_order_mart_table_exist = False

    for table in check_result:
        if table[0] == 'monthly_order_mart':
            monthly_order_mart_table_exist = True
            print("Table daily_order_mart already exist")
        if table[0] == 'client_mart':
            client_mart_table_exist = True
            print("Table client_mart already exist")

    if not client_mart_table_exist:
        cursor.execute("CREATE TABLE client_mart (id varchar(256), name varchar(256), order_sum decimal, order_count decimal);")
        conn.commit()

    if not monthly_order_mart_table_exist:
        cursor.execute(
            "CREATE TABLE monthly_order_mart(month DATETIME PRIMARY KEY, order_sum decimal, order_count decimal);")
        conn.commit()

    cursor.close()
    conn.close()

def clear_mart_tables(**kwargs):
    conn = get_ms_sql_conn()
    cursor = conn.cursor()

    cursor.execute(
        "DELETE FROM client_mart")
    conn.commit()

    cursor.execute(
        "DELETE FROM monthly_order_mart")
    conn.commit()

    cursor.close()
    conn.close()

def create_client_mart(**kwargs):
    conn = get_ms_sql_conn()
    cursor = conn.cursor()

    mart_sql = """
    INSERT INTO client_mart(id, name, order_sum, order_count)
    SELECT
        id,
        name,
        (SELECT sum(amount) from orders where client_id = clients.id) as order_sum,
        (SELECT count(*) from orders where client_id = clients.id) as order_count
    FROM clients
    """

    cursor.execute(mart_sql)
    conn.commit()

    cursor.close()
    conn.close()

def create_monthly_order_mart(**kwargs):
    conn = get_ms_sql_conn()
    cursor = conn.cursor()

    mart_sql = """
         INSERT INTO monthly_order_mart(month, order_sum, order_count)
         SELECT datetrunc(month, created_at) as month, sum(amount) as order_sum, count(*) as order_count
         FROM orders
         GROUP BY datetrunc(month, created_at)
         """

    cursor.execute(mart_sql)
    conn.commit()

    cursor.close()
    conn.close()


init_task = PythonOperator(
  task_id = 'init_mart_tables',
  python_callable=init_mart_tables,
  provide_context=True,
  dag=dag)

clear_mart_task = PythonOperator(
  task_id = 'clear_mart_tables',
  python_callable=clear_mart_tables,
  provide_context=True,
  dag=dag)

create_client_mart_task = PythonOperator(
  task_id = 'create_client_mart',
  python_callable=create_client_mart,
  provide_context=True,
  dag=dag)

create_monthly_order_mart_task = PythonOperator(
  task_id = 'create_monthly_order_mart',
  python_callable=create_monthly_order_mart,
  provide_context=True,
  dag=dag)

init_task >> clear_mart_task >> [create_client_mart_task, create_monthly_order_mart_task]