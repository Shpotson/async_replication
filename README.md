# async_replication
Airflow example for async data replication from Postgres to MSSQL + creating marts.

## How to run:
Install Docker (I used Docker with Docker desktop app)
1) `docker compose build`
2) `docker compose up airflow-init`
3) `docker compose up`

Airflow will be accessible by `http://localhost:8080`

In DAGs list you can find several DAGs I created (tag = 'orders_system').

## How to generate test data:

To generate Postgres test data run DAG `generate_data`

## How to replicate data:

Run DAG named `async_replicator`.

DAG `async_replicator` runs every 15 min 

> **Warning**
> For correct work check if MSSQL container `orders-ms-sql-db` runs. If it does not, re run it again from docker desktop app

## How to create marts:

Run DAG named `create_mart`.

DAG `create_mart` runs daily 

## Also:

### Postgres credentials:

* *DBNAME* = `orders-db`

* *USER* = `test`

* *PASSWORD* = `test`

* *PORT* = `5400`

* *HOST* = `localhost`

### MSSQL credentials:

* *SERVER* = `localhost`

* *PORT* = `1433`

* *DATABASE* = `master`

* *USERNAME* = `SA`

* *PASSWORD* = `b90cf1ef-6535-4d32-81cd-278e306dd25953ACE3C0-5985-4E7A-B86B-3314B271FFE4#`

