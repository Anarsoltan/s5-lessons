import logging
import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from lib import PgConnect
from lib import ConnectionBuilder
from typing import List, Dict
from pendulum import DateTime

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0 0 * * *',  # Günlük işləyir
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'mart', 'courier_ledger'],
    is_paused_upon_creation=True
)
def sprint5_cdm_dm_courier_ledger_dag():
    # Connections
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="cdm_courier_dag")
    def extract_dds_data() -> List[Dict]:
        """
        DDS-dən kuryer hesabatı üçün lazımı məlumatları çıxarır.
        """
        with dwh_pg_connect.client().cursor() as cur:
            cur.execute(
                """
                SELECT
                    d.courier_id,
                    c.courier_name,
                    EXTRACT(YEAR FROM d.order_ts) AS settlement_year,
                    EXTRACT(MONTH FROM d.order_ts) AS settlement_month,
                    COUNT(d.order_id) AS orders_count,
                    SUM(d.sum) AS orders_total_sum,
                    ROUND(AVG(d.rate), 2) AS rate_avg,
                    SUM(d.tip_sum) AS courier_tips_sum
                FROM dds.dm_deliveries d
                JOIN dds.dm_courier c ON d.courier_id = c.courier_id
                GROUP BY d.courier_id, c.courier_name, settlement_year, settlement_month;
                """
            )
            results = cur.fetchall()
            if not results:
                log.info("DDS-də kuryer məlumatı tapılmadı.")
                return []
            return [
                {
                    "courier_id": row[0],
                    "courier_name": row[1],
                    "settlement_year": int(row[2]),
                    "settlement_month": int(row[3]),
                    "orders_count": row[4],
                    "orders_total_sum": float(row[5]),
                    "rate_avg": float(row[6]),
                    "courier_tips_sum": float(row[7])
                }
                for row in results
            ]

    @task(task_id="transform_and_calculate")
    def transform_and_calculate(dds_data: List[Dict]) -> List[Dict]:
        """
        Kuryer məlumatlarını transformasiya edir və ödənişləri hesablayır.
        """
        transformed_data = []
        for record in dds_data:
            orders_total_sum = record.get("orders_total_sum", 0)
            rate_avg = record.get("rate_avg", 0)
            courier_tips_sum = record.get("courier_tips_sum", 0)
            
            # Ödəniş məntiqi
            if rate_avg < 4:
                courier_order_sum = max(orders_total_sum * 0.05, 100)
            elif 4 <= rate_avg < 4.5:
                courier_order_sum = max(orders_total_sum * 0.07, 150)
            elif 4.5 <= rate_avg < 4.9:
                courier_order_sum = max(orders_total_sum * 0.08, 175)
            else:
                courier_order_sum = max(orders_total_sum * 0.10, 200)
            
            # Ümumi məbləğ
            courier_reward_sum = courier_order_sum + (courier_tips_sum * 0.95)
            
            transformed_record = {
                **record,
                "order_processing_fee": orders_total_sum * 0.25,
                "courier_order_sum": courier_order_sum,
                "courier_reward_sum": courier_reward_sum
            }
            transformed_data.append(transformed_record)
        return transformed_data

    @task(task_id="load_courier_ledger")
    def load_courier_ledger(transformed_data: List[Dict]) -> None:
        """
        Hesabatı cdm.dm_courier_ledger cədvəlinə yükləyir.
        """
        with dwh_pg_connect.client() as conn:
            with conn.cursor() as cur:
                for record in transformed_data:
                    try:
                        cur.execute(
                            """
                            INSERT INTO cdm.dm_courier_ledger (
                                courier_id, courier_name, settlement_year, settlement_month,
                                orders_count, orders_total_sum, rate_avg, order_processing_fee,
                                courier_order_sum, courier_tips_sum, courier_reward_sum
                            ) VALUES (
                                %(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s,
                                %(orders_count)s, %(orders_total_sum)s, %(rate_avg)s, %(order_processing_fee)s,
                                %(courier_order_sum)s, %(courier_tips_sum)s, %(courier_reward_sum)s
                            )
                            ON CONFLICT (courier_id, settlement_year, settlement_month) 
                            DO UPDATE SET
                                orders_count = EXCLUDED.orders_count,
                                orders_total_sum = EXCLUDED.orders_total_sum,
                                rate_avg = EXCLUDED.rate_avg,
                                order_processing_fee = EXCLUDED.order_processing_fee,
                                courier_order_sum = EXCLUDED.courier_order_sum,
                                courier_tips_sum = EXCLUDED.courier_tips_sum,
                                courier_reward_sum = EXCLUDED.courier_reward_sum;
                            """,
                            record
                        )
                    except Exception as e:
                        log.error(f"Səhv baş verdi: {e}, Məlumat: {record}")
                        continue
                conn.commit()

    # Task-lar arası asılılıqlar
    extracted_data = extract_dds_data()
    transformed_data = transform_and_calculate(extracted_data)
    load_task = load_courier_ledger(transformed_data)

    extracted_data >> transformed_data >> load_task

courier_ledger_dag = sprint5_cdm_dm_courier_ledger_dag()