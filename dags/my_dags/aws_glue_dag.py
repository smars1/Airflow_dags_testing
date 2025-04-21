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

    @task()
    def lanzar_glue(config):
        return config

    config = leer_config()
    cfg = lanzar_glue(config)

    glue_run = GlueJobOperator(
        task_id='launch_glue_job',
        job_name=cfg['job_name'],
        script_location=cfg['script_location'],
        iam_role_name=cfg['iam_role_name'],
        script_args=cfg['script_args'],
        region_name='us-west-1',  # puedes hacer cfg['region'] si es necesario convertirlo previamente
        wait_for_completion=False
    )

    glue_sensor = GlueJobSensor(
        task_id='monitor_glue_job',
        job_name=cfg['job_name'],
        run_id=glue_run.output,
        region_name='us-west-1',
        verbose=True,
        poke_interval=60,
        timeout=3600
    )

    @task
    def finalizar():
        print("Glue Job finalizado correctamente.")

    config >> cfg >> glue_run >> glue_sensor >> finalizar()

aws_glue_pipeline = aws_glue_etl_pipeline()