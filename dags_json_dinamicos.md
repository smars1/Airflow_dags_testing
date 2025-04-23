# Documentacion del Entorno Airflow con AWS Glue

## Estructura General del Proyecto

La estructura recomendada para implementar Apache Airflow integrado con AWS Glue es:

```
📁 airflow_project_root/
├── 📁 dags/
│   ├── 📁 utils/
│   │   ├── airflow_templates.py        # utilidades comunes (opcional)
│   │   └── aws_handlers.py             # funciones auxiliares para AWS Glue (opcional)
│   ├── 📁 templates/
│   │   ├── glue_etl_cliente_ventas.json       # configuracion 1
│   │   ├── glue_etl_inventario_diario.json    # configuracion 2
│   │   └── ...                                 # se pueden agregar mas
│   └── 📁 my_dags/
│       └── aws_glue_dag_creator.py     # script generador de DAGs desde los JSON
```

> ✅ **Airflow detecta automaticamente todos los DAGs dentro del directorio `dags/`, incluyendo subcarpetas como `my_dags/`, siempre que la ruta este correctamente configurada en `AIRFLOW_HOME` o en `airflow.cfg`.**

---

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

---

## Plantilla JSON para Configurar DAGs de Glue

```json
{
  "dag_id": "glue_etl_inventario_diario",
  "description": "Carga inventario diario desde S3",
  "schedule_interval": "0 6 * * *",
  "catchup": false,
  "email": ["tu_email@empresa.com"],
  "default_args": {
    "retries": 2,
    "retry_delay": 300
  },
  "job_name": "etl_inventario",
  "iam_role_name": "glue-role-etl",
  "region": "us-east-1",
  "script_location": "s3://bucket/scripts/inventario.py",
  "script_args": {
    "--INPUT_PATH": "s3://bucket/raw/inventario/",
    "--OUTPUT_PATH": "s3://bucket/processed/inventario/"
  }
}
```

---

## Ejemplo Visual de DAG Generado

```
[inicio] → [ejecutar_glue] → [esperar_finalizacion] → [fin]
```

- **inicio**: Marca el comienzo del DAG
- **ejecutar_glue**: Inicia el Glue Job con argumentos sanitizados
- **esperar_finalizacion**: Usa un sensor para esperar el final del Glue Job
- **fin**: Marca el final del flujo

---

## Script Generador de DAGs Dinamicos desde JSONs Multiples

```python
import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email_smtp
from airflow.exceptions import AirflowException
from datetime import timedelta

CONFIG_FOLDER = '/opt/airflow/dags/configs/'

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
    keys_to_clean = ["--INPUT_PATH", "--OUTPUT_PATH"]
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
    default_args = {
        "start_date": days_ago(1),
        "email_on_failure": True,
        "email": config.get("email", []),
        "on_failure_callback": send_failure_email,
        "retries": config.get("default_args", {}).get("retries", 1),
        "retry_delay": timedelta(seconds=config.get("default_args", {}).get("retry_delay", 300))
    }

    with DAG(
        dag_id=config["dag_id"],
        default_args=default_args,
        description=config.get("description", ""),
        schedule_interval=config.get("schedule_interval", None),
        catchup=config.get("catchup", False),
        tags=config.get("tags", [])
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
```

---

## Troubleshooting: Errores Comunes y Soluciones

### ❌ Error: `Invalid type for parameter JobName, value: None`
**Causa:** El `run_id` o `job_name` no fue recuperado correctamente del XCom.
**Solucion:** Asegurate de que `ejecutar_glue` retorne correctamente el `run_id`.

### ❌ Error: `No se encontro 'job_name' en el XCom de 'leer_config'`
**Causa:** El DAG esta esperando una salida de un task anterior que no la retorno.
**Solucion:** Verifica que `xcom_push=True` este habilitado y el valor se retorne correctamente.

### ❌ Error: `s3://` en argumentos de Glue
**Causa:** AWS Glue espera paths sin el prefijo `s3://`.
**Solucion:** El script ya aplica `sanitize_script_args()` para remover el prefijo.

### ❌ Error: Email no enviado
**Causa:** Configuracion incorrecta del SMTP o falta de credenciales.
**Solucion:** Configura la conexion SMTP en Airflow (`smtp_user`, `smtp_password`, `smtp_host`, `smtp_port`).

> Puedes revisar los logs completos desde la interfaz web de Airflow → DAG → Task → Ver Logs.

---
