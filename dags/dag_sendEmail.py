import logging
import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash   import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


 # definimos nuetros operators airflow v <= 1.10.10
#from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.python_operator import PythonOperator
#from airflow.hooks.postgres_hook import PostgresHook

from airflow.exceptions import AirflowException
from datetime import timedelta


import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow.utils.email import send_email_smtp

def send_failure_email(context):
    # Obtener informacion del contexto
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url

    # Obtener los logs del task_instance
    try:
        log = context['task_instance'].log
    except AttributeError:
        log = "No se pudieron obtener los logs."

    # Crear el mensaje HTML
    html_content = f"""
    <html>
    <body>
        <h3>Fallo en el DAG</h3>
        <p><strong>DAG:</strong> {dag_id}</p>
        <p><strong>Task:</strong> {task_id}</p>
        <p><strong>Execution Date:</strong> {execution_date}</p>
        <p><strong>Log URL:</strong> <a href="{log_url}">{log_url}</a></p>
        <h4>Log:</h4>
        <pre>{log}</pre>
    </body>
    </html>
    """

    # Configuración del correo
    subject = f"Fallo en DAG: {dag_id}, Task: {task_id}"
    to = "diego198mayotester@gmail.com"
    
    send_email_smtp(
        to=to,
        subject=subject,
        html_content=html_content
    )

default_args = {
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email': ['diego198mayotester@gmail.com','diego.pineda@factorit.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=0.1),
    'on_failure_callback': send_failure_email  # Nota: Añadir la funcion de callback al dag
}

def get_pandas():
    try:
        connection = PostgresHook('redshift_production')
        df = connection.get_pandas_df("SELECT * FROM TABLE")
        logging.info("Data obtained from the query")
        
        df.to_csv('s3://bucket/key.csv', index=False)
        logging.info('Saved in S3')
        
    except Exception as e:
        logging.error("Error connecting to the database or executing the query: %s", e)
        raise AirflowException("Failed to connect to the database or execute the query: ",e)
    

with DAG(dag_id='dag_sendEmail_hooks',
         default_args=default_args,
         schedule_interval='@daily') as dag:
    
    start = EmptyOperator(task_id='start')

    get_pandas_operator = PythonOperator(
        task_id="get_pandas_operator",
        python_callable=get_pandas
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> get_pandas_operator >> end