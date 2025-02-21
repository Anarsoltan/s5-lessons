from logging import Logger
from typing import List
from datetime import datetime
from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


# Модель данных для событий
class EventObj(BaseModel):
    id: int
    event_ts: datetime  
    event_type: str
    event_value: str


# Репозиторий для работы с таблицей outbox
class EventsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_events(self, last_loaded_id: int, limit: int) -> List[EventObj]:
        with self._db.client().cursor(row_factory=class_row(EventObj)) as cur:
            cur.execute(
                """
                SELECT 
                    id, 
                    event_ts, 
                    event_type, 
                    event_value
                FROM public.outbox
                WHERE event_ts::date BETWEEN (now() at time zone 'utc')::date - 2 AND (now() at time zone 'utc')::date - 1
                ORDER BY id ASC
                ;
                """
            )
            events = cur.fetchall()
        return events


# Репозиторий для работы с таблицей stg.bonussystem_events
class EventsDestRepository:
    def insert_event(self, conn: Connection, event: EventObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO stg.bonussystem_events(id, event_ts, event_type, event_value)
                VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s);
                """,
                {
                    "id": event.id,
                    "event_ts": event.event_ts,
                    "event_type": event.event_type,
                    "event_value": event.event_value
                },
            )


# Загрузчик данных событий
class EventsLoader:
    WF_KEY = "events_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100  # Размер пачки для обработки событий.

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = EventsOriginRepository(pg_origin)
        self.stg = EventsDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_events(self):
        with self.pg_dest.connection() as conn:
            # Получаем состояние ETL
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Загружаем новую пачку событий
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_events(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} events to load.")
            if not load_queue:
                self.log.info("No new events to load. Exiting.")
                return

            # Сохраняем события в stg.bonussystem_events
            for event in load_queue:
                self.stg.insert_event(conn, event)

            # Обновляем состояние ETL
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([e.id for e in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
