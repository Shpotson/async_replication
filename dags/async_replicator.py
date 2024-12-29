from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator

import psycopg2
import pyodbc

def get_postgres_connection():
    DBNAME = "orders-db"
    USER = "test"
    PASSWORD = "test"
    PORT = "5432"
    HOST = "orders-postgres-db"

    conn = psycopg2.connect(dbname=DBNAME, host=HOST, user=USER, password=PASSWORD, port=PORT)
    return conn

def get_ms_sql_conn():
    SERVER = 'orders-ms-sql-db'
    DATABASE = 'master'
    USERNAME = 'SA'
    PASSWORD = 'b90cf1ef-6535-4d32-81cd-278e306dd25953ACE3C0-5985-4E7A-B86B-3314B271FFE4#'

    connection_string = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes'

    conn = pyodbc.connect(connection_string)

    return conn

dag = DAG("async_replicator", dagrun_timeout=timedelta(minutes=5), start_date=datetime(2024,12,29), schedule=timedelta(minutes=15), tags=["orders_system"])

def init_tables(**kwargs):
    conn = get_ms_sql_conn()
    cursor = conn.cursor()

    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='dbo'")

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
        cursor.execute("CREATE TABLE clients(id varchar(256) PRIMARY KEY, name varchar(256));")
        conn.commit()

    if not orders_table_exist:
        cursor.execute(
            "CREATE TABLE orders(id varchar(256) PRIMARY KEY, amount decimal, client_id varchar(256) REFERENCES clients(id), created_at DATETIME);")
        conn.commit()

    cursor.close()
    conn.close()

def data_replication_clients(**kwargs):
    pg_conn = get_postgres_connection()
    ms_conn = get_ms_sql_conn()

    pg_cursor = pg_conn.cursor()
    ms_cursor = ms_conn.cursor()

    offset = 0
    while True:
        pg_cursor.execute(f"SELECT * FROM clients LIMIT 10 OFFSET {offset}")

        pg_data = pg_cursor.fetchall()

        for client in pg_data:
            insert_sql = f"""
                    UPDATE TOP (1) clients SET 
                            name = '{client[1]}'
                    WHERE id = '{client[0]}';
                    IF (@@ROWCOUNT = 0) BEGIN 
                            INSERT INTO clients(id, name) VALUES (
                                            '{client[0]}',
                                            '{client[1]}');
                            END
                    """

            ms_cursor.execute(insert_sql)
            ms_conn.commit()

        if len(pg_data) < 10:
            break

        offset += 10

    pg_cursor.close()
    ms_cursor.close()

def data_replication_orders(**kwargs):
    pg_conn = get_postgres_connection()
    ms_conn = get_ms_sql_conn()

    pg_cursor = pg_conn.cursor()
    ms_cursor = ms_conn.cursor()

    offset = 0
    while True:
        pg_cursor.execute(f"SELECT * FROM orders LIMIT 10 OFFSET {offset}")

        pg_data = pg_cursor.fetchall()

        for order in pg_data:
            ms_cursor.execute(f"SELECT * FROM orders WHERE id = '{order[0]}'")
            check_result = ms_cursor.fetchall()

            date = order[3].strftime("%Y-%m-%dT%H:%M:%S")

            if len(check_result) == 1:
                update_sql = f"""
                                    UPDATE orders SET 
                                            amount = {order[1]},
                                            client_id = '{order[2]}',
                                            created_at = convert(datetime, '{date}', 126 )
                                    WHERE id = '{order[0]}';"""
                ms_cursor.execute(update_sql)
                ms_conn.commit()
                continue

            insert_sql = f"""
                    INSERT INTO orders(id, amount, client_id, created_at) VALUES (
                                            '{order[0]}',
                                            {order[1]},
                                            '{order[2]}',
                                            convert(datetime, '{date}', 126 ));
                    """


            ms_cursor.execute(insert_sql)
            ms_conn.commit()

        if len(pg_data) < 10:
            break

        print(f"Replicated {len(pg_data)} orders")

        offset += 10

    pg_cursor.close()
    ms_cursor.close()




init_task = PythonOperator(
  task_id = 'init_tables',
  python_callable=init_tables,
  provide_context=True,
  dag=dag)

data_replication_clients_task = PythonOperator(
  task_id = 'data_replication_clients',
  python_callable=data_replication_clients,
  provide_context=True,
  dag=dag)

data_replication_orders_task = PythonOperator(
  task_id = 'data_replication_orders',
  python_callable=data_replication_orders,
  provide_context=True,
  dag=dag)

init_task >> data_replication_clients_task >> data_replication_orders_task