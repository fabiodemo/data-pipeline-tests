from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowFlakeHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}


@dag(
    dag_id='postgres_to_snowflake',
    default_args=default_args,
    description='Load data incrementally from Postgres to Snowflake',
    schedule_interval=timedelta(days=1),
    catchup=False
)
def postgres_to_snowflake_etl():
    table_names = ['veiculos', 
                   'estados', 
                   'cidades', 
                   'concessionarias', 
                   'vendedores', 
                   'clientes', 
                   'vendas']

    for table_name in table_names:
        @task(task_id=f'get_max_id_{table_name}')
        def get_max_primary_key(table_name: str):
            sf_hook = SnowFlakeHook(snowFlake_conn_id='snowflake').get_conn()
            with sf_hook as conn:
                with conn.cursor() as cursor:
                    query = f'SELECT MAX(ID_{table_name}) FROM {table_name}'
                    max_id = cursor.execute(query)
                    return max_id if max_id is not None else 0
        
        @task(task_id=f'load_data_{table_name}')
        def load_incremental_data(table_name: str, max_id: int):
            pg_hook = PostgresHook(postgres_conn_id='postgres').get_conn()
            with pg_hook.cursor() as pg_cursor:
                primary_key = f'ID_{table_name}'
                query = f"""SELECT column_name 
                           FROM information_schema.columns 
                           WHERE table_name = '{table_name}'
                        """
                pg_cursor.execute(query)
                columns = [row[0] for row in pg_cursor.fetchall()]
                columns_list_str = ', '.join(columns)
                placeholders = ', '.join(['%s'] * len(columns))

                query = f"""SELECT {columns_list_str}
                            FROM {table_name}
                            WHERE {primary_key} = {max_id}
                        """
                pg_cursor.execute(query)
                rows = pg_cursor.fetchall()

                sf_hook = SnowFlakeHook(snowFlake_conn_id='snowflake').get_conn()
                with sf_hook as sf_conn:
                    with sf_conn.cursor() as sf_cursor:
                        insert_query = f"""INSERT INTO {table_name} 
                                           ({columns_list_str})
                                           VALUES ({placeholders})
                                        """
                        for row in rows:
                            sf_cursor.execute(insert_query)

        max_id = get_max_primary_key(table_name)
        load_incremental_data(table_name, max_id)


postgres_to_snowflake_etl_dag = postgres_to_snowflake_etl()