# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.hooks.postgres_hook import PostgresHook
# from airflow.operators.postgres_operator import PostgresOperator

import logging
import pandas as pd
from airflow.models import DAG
from airflow.utils import dates
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.exceptions import AirflowException

default_args = {
    'start_date': dates.days_ago(1)
}

def test_connection(**context):
    try:
        postgreshook_red = PostgresHook(postgres_conn_id="la_virgen_cuantos_datos")
        logging.info("Connection Success")
    except AirflowException as error:
        logging.error("The connection failed", error)

def test_download_experiment_data():
    try:
        redshift = PostgresHook(postgres_conn_id="la_virgen_cuantos_datos")
        df = redshift.get_pandas_df("SELECT * FROM experiments.experimentos_100tifikos")
        df.to_csv('data_experiment.csv', index=False)
    except AirflowException as error:
        logging.error("The download failed", error)

# Definición del DAG 
with DAG(dag_id='dag_helping_nasa',
         default_args=default_args,
         schedule_interval='@daily') as dag:
    
    start = EmptyOperator(task_id='start')

    db_test_connection = PythonOperator(
        task_id='db_test_connection',
        python_callable=test_connection
    )
    
    download_dataTable = PythonOperator(
        task_id='download_dataTable',
        python_callable=test_download_experiment_data
    )
    
    move_data = PostgresOperator(
        task_id='move_data',
        sql="""INSERT INTO comprobar_trayectorias
               SELECT coordenadas
               FROM trayectories.precision_desplezamiento 
               LIMIT 1""",
        postgres_conn_id='la_virgen_cuantos_datos'
    )
    
    end = EmptyOperator(task_id='end')

    start >> db_test_connection >> download_dataTable >> move_data >> end
