from random import random

from airflow import DAG
from datetime import timedelta
from airflow.operators.python import PythonOperator

import uuid
import psycopg2
from faker import Faker

dag = DAG("generate_data", dagrun_timeout=timedelta(minutes=5), tags=["orders_system"])

def get_postgres_connection():
    DBNAME = 'orders-db'
    USER = 'test'
    PASSWORD = 'test'
    PORT = '5432'
    HOST = 'orders-postgres-db'

    conn = psycopg2.connect(dbname=DBNAME, host=HOST, user=USER, password=PASSWORD, port=PORT)
    return conn

def init_tables(**kwargs):
    conn = get_postgres_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")

    check_result = cursor.fetchall()

    clients_table_exist = False
    orders_table_exist = False

    for table in check_result:
        if table[0] == 'orders':
            orders_table_exist = True
            print("Table orders already exist")
        if table[0] == 'clients':
            clients_table_exist = True
            print("Table clients already exist")

    if not clients_table_exist:
        cursor.execute("CREATE TABLE clients(id text PRIMARY KEY, name text);")
        conn.commit()

    if not orders_table_exist:
        cursor.execute(
            "CREATE TABLE orders(id text PRIMARY KEY, amount decimal, client_id text REFERENCES clients(id), created_at timestamp);")
        conn.commit()

    cursor.close()
    conn.close()

def datagen(**kwargs):
    fake_generator = Faker()

    conn = get_postgres_connection()
    cursor = conn.cursor()

    client_ids = []

    for i in range(500):
        name = fake_generator.name()
        id = uuid.uuid4()
        client_ids.append(str(id))

        clients_insert_query = f"""
            INSERT INTO clients(id, name) VALUES ('{str(id)}', '{name}');
        """

        cursor.execute(clients_insert_query)
        conn.commit()

    for i in range(5000):
        index = int(random() * 499)
        amount = random() * 2000 + 500
        id = uuid.uuid4()
        client_id = client_ids[index]
        created_at = fake_generator.date_time()

        orders_insert_query = f"""
                    INSERT INTO orders(id, amount, client_id, created_at) VALUES (
                    '{str(id)}',
                    {str(amount)},
                    '{str(client_id)}',
                    '{created_at.isoformat()}');
                """

        cursor.execute(orders_insert_query)
        conn.commit()

    cursor.close()
    conn.close()


init_tables_task = PythonOperator(
  task_id = 'init_tables',
  python_callable=init_tables,
  provide_context=True,
  dag=dag)

datagen_task = PythonOperator(
  task_id = 'datagen',
  python_callable=datagen,
  provide_context=True,
  dag=dag)

init_tables_task >> datagen_task