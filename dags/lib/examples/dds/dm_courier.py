from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from lib import ConnectionBuilder
import json

# PostgreSQL bağlantısı
dwh_pg_connect = ConnectionBuilder().pg_conn("PG_WAREHOUSE_CONNECTION")

def transfer_couriers(**context):
    query = """
        INSERT INTO dds.dm_courier (courier_id, courier_name)
        SELECT object_value::jsonb->>'_id', object_value::jsonb->>'name'
        FROM stg.bonussystem_couriers
        ON CONFLICT (courier_id) DO NOTHING;
    """
    with dwh_pg_connect.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)


def transfer_deliveries(**context):
    query = """
        INSERT INTO dds.dm_deliveries (order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum)
        SELECT 
            object_value::jsonb->>'order_id', 
            (object_value::jsonb->>'order_ts')::timestamp, 
            object_value::jsonb->>'delivery_id', 
            object_value::jsonb->>'courier_id', 
            object_value::jsonb->>'address', 
            (object_value::jsonb->>'delivery_ts')::timestamp, 
            (object_value::jsonb->>'rate')::int, 
            (object_value::jsonb->>'sum')::numeric, 
            (object_value::jsonb->>'tip_sum')::numeric
        FROM stg.bonussystem_deliveries
        ON CONFLICT (order_id) DO NOTHING;
    """
    with dwh_pg_connect.connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)

# DAG konfiqurasiyası
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="stg_to_dds_dag_courier",
    default_args=default_args,
    description="Transfer data from STG to DDS",
    schedule_interval="0 11 * * *",  # Hər gün saat 11:00-da icra olunur
    catchup=False,
    tags=["stg", "dds", "bonus_system"]
)

transfer_couriers_task = PythonOperator(
    task_id="transfer_couriers",
    python_callable=transfer_couriers,
    dag=dag
)

transfer_deliveries_task = PythonOperator(
    task_id="transfer_deliveries",
    python_callable=transfer_deliveries,
    dag=dag
)

transfer_couriers_task 
transfer_deliveries_task