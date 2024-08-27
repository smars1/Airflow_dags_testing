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
    try_number = context['task_instance'].try_number

    # Solo enviar el correo en el primer fallo
    if try_number == 1:

        dag_id = context['dag'].dag_id
        task_id = context['task_instance'].task_id
        execution_date = context['execution_date']
        log_url = context['task_instance'].log_url

        html_content = f"""
            <div style="font-family: Arial, sans-serif; color: #333;">
                <h2 style="color: #FF4B4B; font-size: 24px;">Task Failure Notification</h2>
                <p style="color: #017CEE; font-size: 18px;"><strong>DAG:</strong> {dag_id}</p>
                <p style="color: #017CEE; font-size: 18px;"><strong>Task:</strong> {task_id}</p>
                <p style="color: #017CEE; font-size: 18px;"><strong>Execution Date:</strong> {execution_date}</p>
                <p style="font-size: 16px;">View the log: <a href="{log_url}" style="color: #2EB67D; font-weight: bold;">Log URL</a></p>
                <hr style="border: 1px solid #017CEE;" />
                <footer style="font-size: 12px; color: #555;">
                    <p>This email was generated by Airflow. Please do not reply.</p>
                </footer>
            </div>
            """

        subject = f"Fallo en DAG: {dag_id}, Task: {task_id}"
        to = ["diego198mayo@gmail.com", "diego198mayotester@gmail.com", "diego.pineda@factorit.com"]
        
        send_email_smtp(
            to=to,
            subject=subject,
            html_content=html_content
        )

default_args = {
    'start_date': days_ago(1),
    'email_on_failure': False,  # Desactivado para usar la función de callback personalizada
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
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
    

with DAG(dag_id='dag_send_email_stilo_v2',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    start = EmptyOperator(task_id='start')

    get_pandas_operator = PythonOperator(
        task_id="get_pandas_operator",
        python_callable=get_pandas
    )
    
    end = EmptyOperator(task_id='end')
    
    start >> get_pandas_operator >> end 