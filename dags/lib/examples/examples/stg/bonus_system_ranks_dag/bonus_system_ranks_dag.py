import logging
import pendulum
from airflow.decorators import dag, task
from examples.stg.bonus_system_ranks_dag.ranks_loader import RankLoader
from examples.stg.bonus_system_ranks_dag.users_loader import UserLoader  
from examples.stg.bonus_system_ranks_dag.events_loader import EventsLoader 
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Выполнение каждые 15 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения
    catchup=False,  # Не запускать даг за предыдущие периоды
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги
    is_paused_upon_creation=False  # DAG при создании будет остановлен
)
def sprint5_example_stg_bonus_system_dag():
    # Подключение к базе DWH
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Подключение к базе подсистемы бонусов
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Таск для загрузки данных из ranks
    @task(task_id="ranks_load")
    def load_ranks():
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_ranks()

    # Таск для загрузки данных из users
    @task(task_id="users_load")
    def load_users():
        user_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        user_loader.load_users()

    @task(task_id="events_load")
    def load_events():
        event_loader = EventsLoader(origin_pg_connect, dwh_pg_connect, log)
        event_loader.load_events()        

    # Последовательность выполнения тасков
    ranks_task = load_ranks()
    users_task = load_users()
    events_task = load_events()

    # Указываем, что задачи выполняются независимо
    [ranks_task, users_task, events_task]  # type: ignore


stg_bonus_system_dag = sprint5_example_stg_bonus_system_dag()
