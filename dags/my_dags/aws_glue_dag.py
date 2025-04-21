
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from datetime import datetime
import json
import os

@dag(
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['aws', 'glue']
)
def aws_glue_etl_pipeline():
    @task(multiple_outputs=True)
    def leer_config():
        config_path = os.path.join(os.path.dirname(__file__), '../configs/glue_configs.json')
        with open(config_path) as f:
            return json.load(f)
    
    config = leer_config()

    # Lanzar el job Glue
    glue_run = GlueJobOperator(
        task_id='launch_glue_job',
        job_name=config['job_name'],
        script_location=config['script_location'],
        iam_role_name=config['iam_role_name'],
        script_args=config['script_args'],
        region_name=config['region'],
        wait_for_completion=False
    )

    # Monitorear el job Glue
    glue_sensor = GlueJobSensor(
        task_id='monitor_glue_job',
        job_name=config['job_name'],
        run_id=glue_run.output,
        region_name=config['region'],
        verbose=True,
        poke_interval=60,  # verifica cada minuto
        timeout=3600       # timeout en una hora
    )

    @task
    def finalizar():
        print(f"Glue Job {config['job_name']} finalizado correctamente.")

    config >> glue_run >> glue_sensor >> finalizar()

aws_glue_pipeline = aws_glue_etl_pipeline()
