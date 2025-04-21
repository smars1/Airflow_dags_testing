# airflow_templates.py
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import timedelta
import logging

class DagAirflow:
    def __init__(self, dag_id, schedule='@daily', retries=2, retry_delay=10):
        self.dag = DAG(
            dag_id=dag_id,
            schedule_interval=schedule,
            start_date=days_ago(1),
            catchup=False,
            default_args={
                'owner': 'airflow',
                'retries': retries,
                'retry_delay': timedelta(minutes=retry_delay),
                'on_failure_callback': self.send_failure_email,
            }
        )

    @staticmethod
    def send_failure_email(context):
        logging.error(f"Task failed: {context['task_instance'].task_id}")

    def get_dag(self):
        return self.dag

def standard_dataflow(p_dag, tasks_config):
    with p_dag as dag:
        @task
        def start():
            logging.info("Starting DAG...")

        @task
        def end():
            logging.info("DAG completed!")

        previous_task = start()
        for config in tasks_config:
            @task(task_id=config['task_id'])
            def dynamic_task(config=config):
                logging.info(f"Running task {config['task_id']} with params {config.get('params', {})}")

            task_instance = dynamic_task()
            previous_task >> task_instance
            previous_task = task_instance

        previous_task >> end()

    return dag
