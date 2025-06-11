from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email_smtp
from airflow.exceptions import AirflowException
from datetime import timedelta
import utils.tools as tools

import os
import json
import logging
from airflow.utils.log.logging_mixin import LoggingMixin

# Configuración del logger
logger = LoggingMixin().log
logger.setLevel(logging.INFO)   
logger.info("Iniciando el script de creación de DAGs para AWS Glue") 



CONFIG_FOLDER = '/opt/airflow/dags/templates/'

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
    to = context['dag'].default_args.get('email', [])
    send_email_smtp(to=to, subject=subject, html_content=html_content)

def sanitize_script_args(script_args: dict) -> dict:
    # keys_to_clean = ["--INPUT_PATH", "--OUTPUT_PATH"]
    logging.info(f"Recibiendo script_args: {script_args}")
    # Limpiar los argumentos de script_args
    keys_to_clean = tools.get_key_arguments(script_args)

    logging.info(f"Limpiando los siguientes argumentos: {keys_to_clean}")
    logging.info(f"Argumentos originales: {script_args}")
    
    for key in keys_to_clean:
        if key in script_args:
            original = script_args[key]
            if original.startswith("s3://"):
                cleaned = original.replace("s3://", "", 1)
                script_args[key] = cleaned
    return script_args

def validar_script_args(script_args: dict):
    for k, v in script_args.items():
        if not isinstance(v, str):
            raise AirflowException(f"El argumento {k} debe ser una cadena de texto. Valor recibido: {v}")

def crear_dag_desde_config(config):
    logging.info(f"Creando DAG desde la configuración: {config['dag_id']}")
    if not config.get("dag_id"):
        raise AirflowException("El archivo de configuración no contiene 'dag_id'")
    
    logging.info(f"Configuración del DAG: {config}")
    default_args = {
        "start_date": days_ago(1),
        "email_on_failure": True,
        "email": config.get("email", []),
        "on_failure_callback": send_failure_email,
        "retries": config.get("default_args", {}).get("retries", 1),
        "retry_delay": timedelta(seconds=config.get("default_args", {}).get("retry_delay", 10)),
        "owner": config.get("owner", "airflow"),
        "description": config.get("description", ""),
    }

    with DAG(
        dag_id=config["dag_id"],
        default_args=default_args,
        description=config.get("description", ""),
        schedule_interval=config.get("schedule_interval", None),
        catchup=config.get("catchup", False),
        tags=config.get("tags", ["AWS","glue","json_dag"])
    ) as dag:

        inicio = EmptyOperator(task_id="inicio")
        fin = EmptyOperator(task_id="fin")

        def ejecutar_glue_fn(**context):
            sanitized_args = sanitize_script_args(config["script_args"])
            validar_script_args(sanitized_args)
            job = GlueJobOperator(
                task_id='ejecutar_glue_task',
                job_name=config['job_name'],
                script_location=config['script_location'],
                iam_role_name=config['iam_role_name'],
                region_name=config['region'],
                script_args=sanitized_args,
                aws_conn_id='aws_default',
                wait_for_completion=False
            )
            return job.execute(context)

        def esperar_finalizacion_fn(**context):
            ti = context['ti']
            run_id_val = ti.xcom_pull(task_ids='ejecutar_glue')
            if not run_id_val:
                raise AirflowException("No se obtuvo run_id del XCom")

            sensor = GlueJobSensor(
                task_id='esperar_finalizacion_sensor',
                job_name=config['job_name'],
                run_id=run_id_val,
                aws_conn_id='aws_default'
            )
            return sensor.execute(context)

        ejecutar_glue_task = PythonOperator(
            task_id='ejecutar_glue',
            python_callable=ejecutar_glue_fn,
            provide_context=True
        )

        esperar_finalizacion_task = PythonOperator(
            task_id='esperar_finalizacion',
            python_callable=esperar_finalizacion_fn,
            provide_context=True
        )

        inicio >> ejecutar_glue_task >> esperar_finalizacion_task >> fin

        return dag

# Detectar todos los JSON en el folder de configuraciones
for archivo in os.listdir(CONFIG_FOLDER):
    if archivo.endswith(".json"):
        ruta_config = os.path.join(CONFIG_FOLDER, archivo)
        with open(ruta_config, 'r') as f:
            config_json = json.load(f)
            dag = crear_dag_desde_config(config_json)
            globals()[config_json['dag_id']] = dag