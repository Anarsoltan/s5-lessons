from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from lib import ConnectionBuilder 
import json

API_URL = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"
HEADERS = {
    "X-Nickname": "anarsoltan",
    "X-Cohort": "31",
    "X-API-KEY": "25c27781-8fde-4b30-a22e-524044a7580f"
}
LIMIT = 50

# PostgreSQL bağlantısı
dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")  # Bağlantının düzgün olduğuna əmin olun

def save_to_stg(table, data, **context):
    with dwh_pg_connect.connection() as conn:  # `with` blokunda açmaq lazımdır
        with conn.cursor() as cursor:
            for record in data:
                cursor.execute(f"""
                    INSERT INTO {table} (object_value)
                    VALUES (%s)
                """, (json.dumps(record),))
 

def fetch_and_store(endpoint, table, **context):
    offset = 0
    while True:
        try:
            response = requests.get(f"{API_URL}/{endpoint}?limit={LIMIT}&offset={offset}", headers=HEADERS)
            response.raise_for_status()
            data = response.json()
            if not data:
                break
            save_to_stg(table, data, **context)
            offset += LIMIT
        except requests.RequestException as e:
            print(f"API Error: {e}")
            break  # Səhv olduqda loop-u bitiririk

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="cur_api_to_stg_dag",
    default_args=default_args,
    description="DAG for fetching data from API and storing to STG",
    schedule_interval="0 10 * * *",
    catchup=False,
    tags=["api", "stg", "bonus_system"]
)

fetch_couriers = PythonOperator(
    task_id="fetch_couriers",
    python_callable=fetch_and_store,
    op_kwargs={"endpoint": "couriers", "table": "stg.bonussystem_couriers"},
    dag=dag
)

fetch_deliveries = PythonOperator(
    task_id="fetch_deliveries",
    python_callable=fetch_and_store,
    op_kwargs={"endpoint": "deliveries", "table": "stg.bonussystem_deliveries"},
    dag=dag
)

fetch_couriers >> fetch_deliveries  # Əgər sıralama lazımdırsa
