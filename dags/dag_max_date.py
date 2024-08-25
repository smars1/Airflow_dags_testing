import logging
import pandas as pd
from datetime import timedelta, datetime

from airflow.models import DAG

# airflow V >=  1.10.15 (airflow V.2) 
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash   import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.bash_operator    import BashOperator
# from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago



default_args={
    "owner": "Diego_Atzin_PC",
    "start_date": days_ago(1),
    "retry": "2",
    'retry_delay': timedelta(minutes=0.1)
}   

def get_max_date_col(**context):
    # conexion de prueba no existe por lo tanto solo pasa como ejemplo y simulamos esta parte
    if False:
        red_connection = PostgresHook('normal_redshift')
        max_date_df = red_connection.get_pandas_df("SELECT MAX('load_datetime') FROM normal_redshift.users")
    logging.info('conecction success')
    max_date_df = datetime.now()
    ti = context['task_instance']
    ti.xcom_push(key='fecha', value=max_date_df)
    return max_date_df

def show_max_date_col(**context):
    ti = context['task_instance']
    pull_max_date = ti.xcom_pull(task_ids='find_max_date')
    print(pull_max_date)


with DAG(dag_id = "dag_get_max_date",
         default_args=default_args,
         schedule_interval='@daily') as DAG:
    
    start = EmptyOperator(task_id = 'start')

    find_max_date = PythonOperator(task_id = 'find_max_date',
                                   python_callable=get_max_date_col,
                                   provide_context = True)
    
    show_max_date = PythonOperator(task_id = 'show_max_date',
                                   python_callable=show_max_date_col,
                                   provide_context = True)
    
    end = EmptyOperator(task_id = 'end')


    start >> find_max_date >> show_max_date >> end

    # AIRFLOW <= 1.10.10 ES MECESARIO do_xcom_pull=True
    
    # find_max_date = PythonOperator(task_id = 'find_max_date',
    #                                python_callable=get_max_date_col,
    #                                do_xcom_push=True,
    #                                provide_context = True)
    
    # show_max_date = PythonOperator(task_id = 'show_max_date',
    #                                python_callable=show_max_date_col,
    #                                do_xcom_pull=True,
    #                                provide_context = True)