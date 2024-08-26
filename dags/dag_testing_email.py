import logging
import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from datetime import timedelta
from airflow.utils.email import send_email_smtp

def send_failure_email(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    log_url = context['task_instance'].log_url

    html_content = f"""
    <html>
    <body>
        <h3>Fallo en el DAG</h3>
        <p><strong>DAG:</strong> {dag_id}</p>
        <p><strong>Task:</strong> {task_id}</p>
        <p><strong>Execution Date:</strong> {execution_date}</p>
        <p><strong>Log URL:</strong> <a href="{log_url}">{log_url}</a></p>
    </body>
    </html>
    """

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
    'on_failure_callback': send_failure_email
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
        raise AirflowException("Failed to connect to the database or execute the query: ", e)

with DAG(dag_id='dag_sendEmail_hooks',
         default_args=default_args,
         schedule_interval='@daily') as dag:
    
    start = EmptyOperator(task_id='start')

    get_pandas_operator = PythonOperator(
        task_id="get_pandas_operator",
        python_callable=get_pandas,
        on_failure_callback=send_failure_email
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> get_pandas_operator >> end
