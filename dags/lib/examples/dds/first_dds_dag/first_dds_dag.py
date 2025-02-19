import logging
import pendulum
from airflow.decorators import dag, task
#rom examples.stg.bonus_system_ranks_dag.ranks_loader import RankLoader
from examples.dds.first_dds_dag.dm_timest import TimestampsLoader
from examples.dds.first_dds_dag.user_loader import UsersLoader  
from examples.dds.first_dds_dag.restorant_loader import RestaurantsLoader  
from examples.dds.first_dds_dag.dm_products import ProductsLoader 
from examples.dds.first_dds_dag.dm_orders import OrdersLoader
from examples.dds.first_dds_dag.fct_product_sales import ProductSalesLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Выполнение каждые 15 минут
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения
    catchup=False,  # Не запускать даг за предыдущие периоды
    tags=['sprint5', 'example', 'dds', 'origin'],  # Теги
    is_paused_upon_creation=False  # DAG при создании будет остановлен
)
def sprint5_example_dds_dag():
    # Подключение к базе DWH
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Подключение к базе подсистемы бонусов
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Таск для загрузки данных из ranks
    #@task(task_id="ranks_load")
    #def load_ranks():
    #    rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
    #    rest_loader.load_ranks()

    # Таск для загрузки данных из users
    @task(task_id="users_load")
    def load_users():
        user_loader = UsersLoader(dwh_pg_connect, dwh_pg_connect, log)
        user_loader.load_users()


    @task(task_id="Restaurants_Load")
    def load_rests():
        rest_loader = RestaurantsLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_restaurants()

    @task(task_id="Product_Load")
    def load_products():
        rest_loader = ProductsLoader(dwh_pg_connect, dwh_pg_connect, log)
        rest_loader.load_products()

    @task(task_id="Times_Load")
    def load_timestamps():
        times_loader = TimestampsLoader(dwh_pg_connect, dwh_pg_connect, log)
        times_loader.load_timestamps()

    @task(task_id="Order_Load")
    def load_orders():
        times_loader = OrdersLoader(dwh_pg_connect, dwh_pg_connect, log)
        times_loader.load_orders()

    @task(task_id="ProductSales_Load")
    def load_sales():
        sales_loader = ProductSalesLoader(dwh_pg_connect, dwh_pg_connect)
        sales_loader.load_sales()   

    # Последовательность выполнения тасков
    users_task = load_users()
    rests_task = load_rests()
    prods_task = load_products()
    times_task = load_timestamps()
    orders_task = load_orders()
    ProductSales_task = load_sales()

    # Указываем, что задачи выполняются независимо
    #[ranks_task, users_task, events_task]  # type: ignore
    [users_task, rests_task, times_task, prods_task, orders_task, ProductSales_task]

dds_load_dag = sprint5_example_dds_dag()
