from lib import PgConnect
from psycopg import Connection
from logging import Logger
from typing import List, Dict
from pydantic import BaseModel, ValidationError
import json


class ProductObj(BaseModel):
    product_id: str
    product_name: str
    product_price: float
    active_from: str
    active_to: str
    restaurant_id: int


class ProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self) -> List[Dict]:
        with self._db.client().cursor() as cur:
            cur.execute("SELECT or2.object_value FROM stg.ordersystem_restaurants or2;")
            orders = cur.fetchall()
            product_list = []

            for row in orders:
                try:
                    order_data = json.loads(row[0])
                    restaurant_external_id = order_data.get("_id")  # MongoDB `_id`

                    for product in order_data.get("menu", []):  # `menu` sahəsindən məhsulları oxuyuruq
                        product_data = {
                            "product_id": product["_id"],
                            "product_name": product["name"],
                            "product_price": product["price"],
                            "active_from": order_data.get("update_ts"),
                            "active_to": "2099-12-31",
                            "restaurant_id": restaurant_external_id,  # Burada string gəlir!
                        }
                        product_list.append(product_data)
                except (ValidationError, KeyError, json.JSONDecodeError) as e:
                    print(f"🚨 Məhsulun işlənməsi zamanı xəta: {e}, json: {row[0]}")
                    continue

            return product_list


class ProductsDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_products(self, products: List[Dict]) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                for product_dict in products:
                    try:
                        # 🔹 MongoDB-dən gələn `restaurant_id` (string formatdadır)
                        restaurant_ext_id = product_dict["restaurant_id"]

                        # 🔹 `dds.dm_restaurants` cədvəlindən integer ID-ni tapırıq
                        cur.execute(
                            """
                            SELECT id FROM dds.dm_restaurants
                            WHERE restaurant_id = %s;
                            """,
                            (restaurant_ext_id,)
                        )
                        restaurant_result = cur.fetchone()

                        if not restaurant_result:
                            print(f"🚨 Xəta: Restaurant ID {restaurant_ext_id} `dm_restaurants` cədvəlində tapılmadı.")
                            continue  # Nəticə tapılmasa, bu məhsulu keçirik

                        # 🔹 `restaurant_id` integer olaraq dəyişdirilir
                        product_dict["restaurant_id"] = restaurant_result[0]

                        # **DEBUG**: Əmin olmaq üçün print edək
                        print(f"✅ Debug: product_dict['restaurant_id'] = {product_dict['restaurant_id']} (Type: {type(product_dict['restaurant_id'])})")

                        # 🔹 `ProductObj` obyektini validasiya edirik (artıq `restaurant_id` integerdir)
                        product = ProductObj(**product_dict)

                        # 🔹 Məhsulu `dds.dm_products` cədvəlində insert edirik
                        cur.execute(
                            """
                            INSERT INTO dds.dm_products (
                                product_id, product_name, product_price, active_from, active_to, restaurant_id
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s
                            ) ON CONFLICT (product_id) DO NOTHING;
                            """,
                            (
                                product.product_id,
                                product.product_name,
                                product.product_price,
                                product.active_from,
                                product.active_to,
                                product.restaurant_id,  # Burada integer olmalıdır!
                            ),
                        )

                    except Exception as e:
                        print(f"🚨 Xəta məhsulun daxil edilməsində: {e}, data: {product_dict}")
                        continue

            conn.commit()


class ProductsLoader:
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = ProductsDestRepository(pg_dest)
        self.origin = ProductsOriginRepository(pg_origin)
        self.log = log

    def load_products(self) -> None:
        product_list = self.origin.list_products()
        self.stg.insert_products(product_list)
