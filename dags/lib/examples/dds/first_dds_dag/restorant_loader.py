from lib import PgConnect
from lib.dict_util import str2json
from psycopg import Connection
from logging import Logger
from typing import List, Dict
from pydantic import BaseModel, ValidationError
from examples.dds.dds_settings_repository import DdsEtlSettingsRepository


class RestaurantObj(BaseModel):
    restaurant_id: str
    restaurant_name: str
    active_from: str


class RestaurantsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self) -> List[Dict]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT id, object_value, update_ts
                FROM stg.ordersystem_restaurants;
                """
            )
            objs = cur.fetchall()

        restaurant_list = []
        for record in objs:
            try:
                restaurant_json = record[1]
                restaurant_dict = str2json(restaurant_json)

                transformed_restaurant = {
                    "restaurant_id": restaurant_dict["_id"],
                    "restaurant_name": restaurant_dict["name"],
                    "active_from": restaurant_dict["update_ts"],
                }
                restaurant_list.append(transformed_restaurant)
            except (ValidationError, KeyError) as e:
                print(f"Ошибка при обработке ресторана: {e}, json: {restaurant_json}")
                continue

        return restaurant_list


class RestaurantsDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_restaurants(self, restaurants: List[Dict]) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                for restaurant_dict in restaurants:
                    try:
                        restaurant = RestaurantObj(**restaurant_dict)
                        cur.execute(
    """
    INSERT INTO dds.dm_restaurants(
        restaurant_id,
        restaurant_name,
        active_from
    )
    VALUES (
        %(restaurant_id)s,
        %(restaurant_name)s,
        %(active_from)s
    )
    ON CONFLICT (restaurant_id) DO UPDATE SET
        restaurant_name = EXCLUDED.restaurant_name,
        active_from = EXCLUDED.active_from
    """,
    {
        "restaurant_id": restaurant.restaurant_id,
        "restaurant_name": restaurant.restaurant_name,
        "active_from": restaurant.active_from
    }
)
                    except Exception as e:
                        print(f"Ошибка при вставке ресторана: {e}, data: {restaurant_dict}")
                        continue
            conn.commit()


class RestaurantsLoader:
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = RestaurantsDestRepository(pg_dest)
        self.origin = RestaurantsOriginRepository(pg_origin)
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_restaurants(self) -> None:
        restaurant_list = self.origin.list_restaurants()
        self.stg.insert_restaurants(restaurant_list)