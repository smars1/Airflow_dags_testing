# Documentacion del Entorno Airflow con AWS Glue

## Estructura General del Proyecto

La estructura recomendada para implementar Apache Airflow integrado con AWS Glue es:

```
📁 airflow_project_root/
├── 📁 dags/
│   ├── 📁 utils/
│   │   ├── airflow_templates.py        # utilidades comunes (opcional)
│   │   └── aws_handlers.py             # funciones auxiliares para AWS Glue (opcional)
│   ├── 📁 configs/
│   │   └── glue_configs.json           # configuracion dinamica para AWS Glue
│   └── 📁 my_dags/
│       └── aws_glue_dag.py             # DAG principal que orquesta el Glue Job
```

> ✅ **Airflow detecta automaticamente todos los DAGs dentro del directorio `dags/`, incluyendo subcarpetas como `my_dags/`, siempre que la ruta este correctamente configurada en `AIRFLOW_HOME` o en `airflow.cfg`.**

## Configuracion de Conexion AWS en Airflow

Para configurar la conexion AWS en Airflow:

1. Accede a la interfaz web de Airflow (`Admin → Connections`).
2. Crea una nueva conexion:
   - **Conn Id:** `aws_default` (o personalizado).
   - **Conn Type:** `Amazon Web Services`.
   - **Extra:** (opcional, recomendado)

```json
{
  "aws_access_key_id": "TU_ACCESS_KEY",
  "aws_secret_access_key": "TU_SECRET_KEY",
  "region_name": "us-east-1"
}
```

> Si estas usando Airflow en MWAA o EC2 con roles IAM, puedes dejar el campo Extra vacio y AWS usara las credenciales del entorno.

## Archivo de Configuracion JSON

Ejemplo del archivo `glue_configs.json`:

```json
{
    "job_name": "process_csv_to_parquet",
    "script_location": "s3://glue-procces-storage/scripts/process_csv_to_parquet.py",
    "iam_role_name": "GlueExecutionRole",
    "region": "us-east-1",
    "script_args": {
        "--INPUT_PATH": "s3://etl-glue-csv-input/usuarios.csv",
        "--OUTPUT_PATH": "s3://etl-glue-csv-output/"
    }
}
```

## Codigo del DAG SMTP con Manejo de Fallos

```python
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email_smtp
from airflow.exceptions import AirflowException
import json

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
    to = ["diego198mayotester@gmail.com", "diego.pineda@factorit.com"]
    send_email_smtp(to=to, subject=subject, html_content=html_content)

default_args = {
    "start_date": days_ago(1),
    "email_on_failure": True,
    "on_failure_callback": send_failure_email
}

with DAG(
    dag_id="aws_glue_etl_pipeline_smtp",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["aws", "glue"]
) as dag:

    inicio = EmptyOperator(task_id="inicio")

    def leer_config_func(**kwargs):
        with open('/opt/airflow/dags/configs/glue_configs.json') as f:
            config = json.load(f)
        kwargs['ti'].xcom_push(key='job_config', value=config)

    leer_config = PythonOperator(
        task_id="leer_config",
        python_callable=leer_config_func
    )

    def ejecutar_glue_func(**kwargs):
        ti = kwargs['ti']
        config = ti.xcom_pull(task_ids='leer_config', key='job_config')
        if not config:
            raise AirflowException("No se pudo leer la configuracion")

        glue_task = GlueJobOperator(
            task_id='ejecutar_glue_task',
            job_name=config['job_name'],
            script_location=config['script_location'],
            iam_role_name=config['iam_role_name'],
            region_name=config['region'],
            script_args=config['script_args'],
            aws_conn_id='aws_default',
            wait_for_completion=False
        )
        run_id = glue_task.execute(context=kwargs)
        ti.xcom_push(key='glue_run_id', value=run_id)

    ejecutar_glue = PythonOperator(
        task_id="ejecutar_glue",
        python_callable=ejecutar_glue_func
    )

    def sensor_poke(**kwargs):
        ti = kwargs['ti']
        config = ti.xcom_pull(task_ids='leer_config', key='job_config')
        run_id = ti.xcom_pull(task_ids='ejecutar_glue', key='glue_run_id')

        if not config or 'job_name' not in config:
            raise AirflowException("No se encontro 'job_name'")
        if not run_id:
            raise AirflowException("No se encontro 'run_id'")

        sensor = GlueJobSensor(
            task_id='esperar_finalizacion_sensor',
            job_name=config['job_name'],
            run_id=run_id,
            aws_conn_id='aws_default'
        )
        return sensor.execute(context=kwargs)

    esperar_finalizacion = PythonOperator(
        task_id="esperar_finalizacion",
        python_callable=sensor_poke
    )

    fin = EmptyOperator(task_id="fin")

    inicio >> leer_config >> ejecutar_glue >> esperar_finalizacion >> fin
```

> ✅ Este DAG envia notificaciones SMTP si algo falla, maneja correctamente los valores con XCom y evita errores comunes de tipos o decoradores incorrectos.
