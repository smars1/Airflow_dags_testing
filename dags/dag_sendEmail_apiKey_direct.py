from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.email import send_email
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=0.5),
}

def send_failure_email(context):
    subject = f"Task {context['task_instance'].task_id} in DAG {context['task_instance'].dag_id} Failed"
    html_content = f"""
    <h3>Task {context['task_instance'].task_id} in DAG {context['task_instance'].dag_id} Failed</h3>
    <p>Task Instance: {context['task_instance']}</p>
    <p>Exception: {context['exception']}</p>
    <p>Log URL: <a href="{context['task_instance'].log_url}">Log</a></p>
    """
    send_email(to=['diego198mayotester@gmail.com','diego.pineda@factorit.com'], 
               subject=subject, 
               html_content=html_content)

with DAG(
    dag_id="dag_sendEmail_apiKey_direct",
    default_args=default_args,
    description="DAG that sends email on failure using API Key for SMTP",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    on_failure_callback=send_failure_email
) as dag:

    start = EmptyOperator(task_id='start')

    def failing_task():
        divi  = 0/0
        print(divi)
        #raise ValueError("This task fails intentionally.")

    fail_task = PythonOperator(
        task_id='fail_task',
        python_callable=failing_task
    )

    end = EmptyOperator(task_id='end')

    start >> fail_task >> end
