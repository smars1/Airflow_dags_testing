import logging
import pandas as pd

from airflow.models import DAG
from airflow.utils import dates 

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


# airflow =< 1.10.10
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.hooks.postgres_hook import PostgresHook

default_args ={
    'start_date': dates.days_ago(1)
}


def get_pandas():
    try:
        # this is a example to redshift connection service
        connection = PostgresHook('redshift_production')
        df = connection.get_pandas_df("SELECT * FROM TABLE")
        logging.info("Data obtained from the query")
        df.to_csv('s3://bucket/key.csv', index=False)
        logging.info('Saved in s3')
    except ConnectionError as e:
        logging(f"This is only an example about how to use hooks: The connection will fail success:", e)
    except Exception as e:
        logging(f"This is only an example about how to use hooks: The connection will fail success: {e}", e)

with DAG(dag_id='dag_hooks',
         default_args=default_args,
         schedule_interval='@daily') as dag:
    
    start = EmptyOperator(task_id='start')

    #perform the creation to own  pandas operator with a PythonOperator and pass it the redshift hook
    get_pandas_operator = PythonOperator(task_id="get_pandas_operator",
                                         python_callable= get_pandas,
                                         email_on_failure=True, 
                                         email='diego198mayotester@gmail.com')
    
    end = EmptyOperator(task_id='end')
    

start >> get_pandas_operator >> end

