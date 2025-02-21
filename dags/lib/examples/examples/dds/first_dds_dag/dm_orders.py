from lib import PgConnect
from psycopg import Connection
from logging import Logger
from typing import List, Dict
from pydantic import BaseModel
from sqlalchemy import text
import json

class OrderObj(BaseModel):
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int

class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self) -> List[Dict]:
        with self._db.client().cursor() as cur:
            cur.execute("SELECT object_value FROM stg.ordersystem_orders;")
            orders = cur.fetchall()
            order_list = []

            for row in orders:
                order_data = json.loads(row[0])
                restaurant_external_id = order_data.get("restaurant")  # Ищем restaurant_id

                # Get the most recent timestamp from statuses
                most_recent_status = order_data["statuses"][-1]
                timestamp_id = most_recent_status["dttm"]  # Using the most recent "dttm" value

                order_dict = {
                    "order_key": order_data["_id"],
                    "order_status": most_recent_status["status"],  # Using the most recent status
                    "restaurant_id": restaurant_external_id,
                    "timestamp_id": timestamp_id,  # Using the timestamp value from the most recent status
                    "user_id": order_data["user"]["id"]  # Corrected the user_id access
                }
                order_list.append(order_dict)
            
            return order_list

class OrdersDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_orders(self, orders: List[Dict]) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                for order_dict in orders:
                    try:
                        # Ensure that restaurant_id is a string or integer, not a dictionary
                        if isinstance(order_dict["restaurant_id"], dict):
                            order_dict["restaurant_id"] = order_dict["restaurant_id"].get("id")

                        # Находим нужный restaurant_id
                        cur.execute(
                            """
                            SELECT id FROM dds.dm_restaurants
                            WHERE restaurant_id = %s;
                            """,
                            (order_dict["restaurant_id"],)
                        )
                        restaurant_result = cur.fetchone()
                        if not restaurant_result:
                            print(f"🚨 Restaurant ID {order_dict['restaurant_id']} не найден!")
                            continue
                        order_dict["restaurant_id"] = restaurant_result[0]
                        
                        # Находим timestamp_id
                        cur.execute(
                            """
                            SELECT id FROM dds.dm_timestamps
                            WHERE ts = %s;
                            """,
                            (order_dict["timestamp_id"],)
                        )
                        timestamp_result = cur.fetchone()
                        if not timestamp_result:
                            print(f"🚨 Timestamp {order_dict['timestamp_id']} не найден!")
                            continue
                        order_dict["timestamp_id"] = timestamp_result[0]
                        
                        # Находим user_id
                        cur.execute(
                            """
                            SELECT id FROM dds.dm_users
                            WHERE user_id = %s;
                            """,
                            (order_dict["user_id"],)
                        )
                        user_result = cur.fetchone()
                        if not user_result:
                            print(f"🚨 User ID {order_dict['user_id']} не найден!")
                            continue
                        order_dict["user_id"] = user_result[0]

                        # Вставляем заказ в dm_orders
                        cur.execute(
                            """
                            INSERT INTO dds.dm_orders (
                                order_key, order_status, restaurant_id, timestamp_id, user_id
                            ) VALUES (
                                %s, %s, %s, %s, %s
                            ) ON CONFLICT (order_key) DO NOTHING;
                            """,
                            (
                                order_dict["order_key"],
                                order_dict["order_status"],
                                order_dict["restaurant_id"],
                                order_dict["timestamp_id"],
                                order_dict["user_id"],
                            ),
                        )
                    except Exception as e:
                        print(f"🚨 Ошибка при вставке заказа: {e}, данные: {order_dict}")
                        continue
            conn.commit()


class OrdersLoader:
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = OrdersDestRepository(pg_dest)
        self.origin = OrdersOriginRepository(pg_origin)
        self.log = log

    def load_orders(self) -> None:
        order_list = self.origin.list_orders()
        self.stg.insert_orders(order_list)