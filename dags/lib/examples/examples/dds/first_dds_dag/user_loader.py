from lib import PgConnect
from lib.dict_util import str2json
from psycopg import Connection
from logging import Logger
from typing import List, Dict

from lib import pg_connect
from examples.dds.dds_settings_repository import DdsEtlSettingsRepository

from psycopg.rows import class_row
from pydantic import BaseModel

class UserObj(BaseModel):
    user_id: str
    user_name: str
    user_login: str

class UsersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_users(self) -> List[Dict]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT id, object_value
                FROM stg.ordersystem_users;
                """
            )
            objs = cur.fetchall()
            user_list = []
            for record in objs:
                try:
                    user_json = record[1]
                    user_dict = str2json(user_json)

                    # Преобразование данных в нужный формат
                    transformed_user = {
                        "user_id": user_dict["_id"],   # Преобразуем _id в user_id
                        "user_name": user_dict["name"],  # Преобразуем name в user_name
                        "user_login": user_dict["login"],  # Преобразуем login в user_login
                    }
                    user_list.append(transformed_user)

                except (json.JSONDecodeError, ValidationError, KeyError) as e:
                    print(f"Ошибка при обработке пользователя: {e}, json: {user_json}")
                    continue
            return user_list
  


class UsersDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_users(self, users: List[Dict]) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                for user_dict in users:
                    try:
                        # Создаём объект UserObj, игнорируя id
                        user = UserObj(**user_dict)
                        cur.execute(
                            """
                            INSERT INTO dds.dm_users(user_id, user_name, user_login)
                            VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                            ON CONFLICT (user_id) DO NOTHING;
                            """,
                            {
                                "user_id": user.user_id,
                                "user_name": user.user_name,
                                "user_login": user.user_login,
                            },
                        )
                    except Exception as e:
                        print(f"Ошибка при вставке пользователя: {e}, data: {user_dict}")
                        continue
            conn.commit()
                
class UsersLoader:
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = UsersDestRepository(pg_dest)
        self.origin = UsersOriginRepository(pg_origin)
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log


    def load_users(self) -> None:
        users_list = self.origin.list_users()
        self.stg.insert_users(users_list)