from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import json

@dag(schedule_interval=None, start_date=days_ago(1), catchup=False, tags=["aws", "glue"])
def aws_glue_etl_pipeline():

    @task()
    def leer_config():
        with open('/opt/airflow/dags/configs/glue_configs.json') as f:
            return json.load(f)

    @task()
    def ejecutar_glue(config: dict) -> str:
        job = GlueJobOperator(
            task_id='ejecutar_glue',
            job_name=config['job_name'],
            script_location=config['script_location'],
            iam_role_name=config['iam_role_name'],
            region_name=config['region'],
            script_args=config['script_args'],
            aws_conn_id='aws_default',
            wait_for_completion=False
        )
        return job.execute({})  # Retorna el run_id

    config = leer_config()
    run_id = ejecutar_glue(config)

    esperar_finalizacion = GlueJobSensor(
        task_id='esperar_finalizacion',
        job_name="{{ ti.xcom_pull(task_ids='leer_config')['job_name'] }}",
        run_id="{{ ti.xcom_pull(task_ids='ejecutar_glue') }}",
        aws_conn_id='aws_default',
        poke_interval=30,
        timeout=600
    )

    iniciar = EmptyOperator(task_id="iniciar")
    terminar = EmptyOperator(task_id="terminar")

    iniciar >> config >> run_id >> esperar_finalizacion >> terminar

aws_glue_etl_pipeline()
