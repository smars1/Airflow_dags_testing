from airflow.decorators import dag, task
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email_smtp
from airflow.exceptions import AirflowException
import json

# Funcion para notificar por correo en caso de fallo
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
    to = ["diego198mayotester@gmail.com", "diego198mayo@gmail.com"]
    send_email_smtp(to=to, subject=subject, html_content=html_content)

@dag(
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["aws", "glue", "smtp" "test_fail"],   
    default_args={
        "email": ["diego198mayotester@gmail.com", "diego198mayo@gmail.com"],
        "email_on_failure": True,
        "on_failure_callback": send_failure_email
    }
)
def aws_glue_etl_pipeline_smtp_fail():

    @task()
    def leer_config():
        with open('/opt/airflow/dags/configs/glue_configs.json') as f:
            return json.load(f)

    @task()
    def ejecutar_glue(config):
        return GlueJobOperator(
            task_id='ejecutar_glue',
            job_name=config['job_name'],
            script_location=config['script_location'],
            iam_role_name=config['iam_role_name'],
            region_name=config['region'],
            script_args=config['script_args'],
            aws_conn_id='aws_default',
            wait_for_completion=False,
            on_failure_callback=send_failure_email
        ).execute({})

    @task.sensor(poke_interval=30, timeout=600)
    def esperar_finalizacion(ti=None):
        config = ti.xcom_pull(task_ids='leer_config')
        run_id_val = ti.xcom_pull(task_ids='ejecutar_glue')

        if not config or 'job_name' not in config:
            raise AirflowException("No se encontro 'job_name' en el XCom de 'leer_config'")

        if not run_id_val:
            raise AirflowException("No se encontro 'run_id' en el XCom de 'ejecutar_glue'")

        return GlueJobSensor(
            task_id='esperar_finalizacion_sensor',
            job_name=config['job_name'],
            run_id=run_id_val,
            aws_conn_id='aws_default'
        ).poke({})


    iniciar =  EmptyOperator(task_id="iniciar")
    terminar =  EmptyOperator(task_id="terminar")

    config = leer_config()
    run_id = ejecutar_glue(config)
    iniciar >> esperar_finalizacion() >> terminar

aws_glue_etl_pipeline_smtp_fail()