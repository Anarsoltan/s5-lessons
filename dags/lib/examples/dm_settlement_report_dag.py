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
    schedule_interval='0 0 * * *',  # Загрузка каждый день
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'mart', 'settlement'],
    is_paused_upon_creation=True
)
def sprint5_example_cdm_dm_settlement_report_dag():
    # Connections
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="extract_dds_data")
    def extract_dds_data() -> List[Dict]:
        """
        Extracts required data from DDS for settlement report calculation.
        """
        with dwh_pg_connect.client().cursor() as cur:
            cur.execute(
                """
                SELECT
                    o.restaurant_id,
                    r.restaurant_name,
                    COUNT(o.order_key) AS orders_count,
                    SUM(f.total_sum) AS orders_total_sum,
                    SUM(f.bonus_payment) AS orders_bonus_payment_sum,
                    SUM(f.bonus_grant) AS orders_bonus_granted_sum
                FROM dds.dm_orders o
                JOIN dds.dm_restaurants r ON o.restaurant_id = r.id
                JOIN dds.fct_product_sales f ON o.id = f.order_id
                JOIN dds.dm_timestamps t ON o.timestamp_id = t.id
                WHERE o.order_status = 'CLOSED'
                GROUP BY o.restaurant_id, r.restaurant_name;
                """
            )
            results = cur.fetchall()
            if not results:
                log.info("No settlement data found in DDS.")
                return []
            return [
                {
                    "restaurant_id": row[0],
                    "restaurant_name": row[1],
                    "orders_count": row[2],
                    "orders_total_sum": float(row[3]),
                    "orders_bonus_payment_sum": float(row[4]),
                    "orders_bonus_granted_sum": float(row[5]),
                }
                for row in results
            ]

    @task(task_id="transform_and_enrich_data")
    def transform_and_enrich_data(dds_data: List[Dict], execution_date: DateTime = None) -> List[Dict]:
        """
        Transforms and enriches the data for the settlement report.
        """
        if execution_date is None:
            log.error("Execution date is missing.")
            raise AirflowException("Execution date is missing.")
        
        enriched_data = []
        for record in dds_data:
            orders_total_sum = record.get("orders_total_sum", 0)
            orders_bonus_payment_sum = record.get("orders_bonus_payment_sum", 0)
            order_processing_fee = orders_total_sum * 0.25
            restaurant_reward_sum = orders_total_sum - orders_bonus_payment_sum - order_processing_fee

            enriched_record = {
                "restaurant_id": record["restaurant_id"],
                "restaurant_name": record["restaurant_name"],
                "settlement_date": execution_date.subtract(days=1).isoformat(),
                "orders_count": record["orders_count"],
                "orders_total_sum": orders_total_sum,
                "orders_bonus_payment_sum": orders_bonus_payment_sum,
                "orders_bonus_granted_sum": record["orders_bonus_granted_sum"],
                "order_processing_fee": order_processing_fee,
                "restaurant_reward_sum": restaurant_reward_sum
            }
            enriched_data.append(enriched_record)
        return enriched_data

    @task(task_id="load_settlement_report")
    def load_settlement_report(settlement_data: List[Dict]) -> None:
        """
        Loads the settlement report data into the dm_settlement_report table.
        """
        with dwh_pg_connect.client() as conn:
            with conn.cursor() as cur:
                for record in settlement_data:
                    try:
                        cur.execute(
                            """
                            INSERT INTO cdm.dm_settlement_report (
                                restaurant_id, restaurant_name, settlement_date, orders_count,
                                orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum,
                                order_processing_fee, restaurant_reward_sum
                            ) VALUES (
                                %(restaurant_id)s, %(restaurant_name)s, %(settlement_date)s, %(orders_count)s,
                                %(orders_total_sum)s, %(orders_bonus_payment_sum)s, %(orders_bonus_granted_sum)s,
                                %(order_processing_fee)s, %(restaurant_reward_sum)s
                            )
                            ON CONFLICT (restaurant_id, settlement_date) DO UPDATE SET
                                orders_count = EXCLUDED.orders_count,
                                orders_total_sum = EXCLUDED.orders_total_sum,
                                orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                                orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                                order_processing_fee = EXCLUDED.order_processing_fee,
                                restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                            """,
                            record,
                        )
                    except Exception as e:
                        log.error(f"Error inserting/updating settlement report: {e}, {record}")
                        continue
                conn.commit()

    dds_data = extract_dds_data()
    enriched_data = transform_and_enrich_data(dds_data=dds_data)
    load_report_task = load_settlement_report(settlement_data=enriched_data)

    dds_data >> enriched_data >> load_report_task

dm_settlement_report_dag = sprint5_example_cdm_dm_settlement_report_dag()
