from lib import PgConnect
from lib.dict_util import str2json
from psycopg import Connection
from logging import Logger
from typing import List, Dict
from pydantic import BaseModel, ValidationError
from examples.dds.dds_settings_repository import DdsEtlSettingsRepository

class TimestampObj(BaseModel):
    id: int
    ts: str
    year: int
    month: int
    day: int
    date: str
    time: str

class TimestampsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(self) -> List[Dict]:
        with self._db.client().cursor() as cur:
            cur.execute(
                """
                SELECT id, object_value
                FROM stg.ordersystem_orders;
                """
            )
            objs = cur.fetchall()
            timestamp_list = []

            for record in objs:
                try:
                    order_json = record[1]
                    order_dict = str2json(order_json)

                    if order_dict.get("final_status") in ["CLOSED", "CANCELLED"]:
                        ts = order_dict["date"]
                        transformed_timestamp = {
                            "id": record[0],
                            "ts": ts,
                            "year": int(ts[:4]),
                            "month": int(ts[5:7]),
                            "day": int(ts[8:10]),
                            "date": ts.split(" ")[0],
                            "time": ts.split(" ")[1],
                        }
                        timestamp_list.append(transformed_timestamp)
                except (ValidationError, KeyError) as e:
                    print(f"Ошибка при обработке временной метки: {e}, json: {order_json}")
                    continue

            return timestamp_list

class TimestampsDestRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def insert_timestamps(self, timestamps: List[Dict]) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                for timestamp_dict in timestamps:
                    try:
                        timestamp = TimestampObj(**timestamp_dict)

                        cur.execute(
                            """
                            INSERT INTO dds.dm_timestamps(
                                id, ts, year, month, day, date, time
                            )
                            VALUES (
                                %(id)s, %(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s
                            )
                            ON CONFLICT DO NOTHING;
                            """,
                            {
                                "id": timestamp.id,
                                "ts": timestamp.ts,
                                "year": timestamp.year,
                                "month": timestamp.month,
                                "day": timestamp.day,
                                "date": timestamp.date,
                                "time": timestamp.time,
                            },
                        )
                    except Exception as e:
                        print(f"Ошибка при вставке временной метки: {e}, data: {timestamp_dict}")
                        continue
            conn.commit()

class TimestampsLoader:
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = TimestampsDestRepository(pg_dest)
        self.origin = TimestampsOriginRepository(pg_origin)
        self.log = log

    def load_timestamps(self) -> None:
        timestamp_list = self.origin.list_timestamps()
        self.stg.insert_timestamps(timestamp_list)