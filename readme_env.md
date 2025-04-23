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

## Codigo del DAG Completo

```python
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
    def ejecutar_glue(config):
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
        return job.execute({})

    config = leer_config()
    job_name = config['job_name']
    run_id = ejecutar_glue(config)

    @task.sensor(poke_interval=30, timeout=600)
    def esperar_finalizacion(ti=None):
        from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor
        job_name_val = ti.xcom_pull(task_ids='leer_config')['job_name']
        run_id_val = ti.xcom_pull(task_ids='ejecutar_glue')
        sensor = GlueJobSensor(
            task_id='esperar_finalizacion_sensor',
            job_name=job_name_val,
            run_id=run_id_val,
            aws_conn_id='aws_default'
        )
        return sensor.execute(context={'ti': ti})

    terminar = EmptyOperator(task_id="terminar")

    config >> run_id >> esperar_finalizacion() >> terminar

aws_glue_etl_pipeline()
```

## Manejo Correcto de `GlueJobSensor`

> ⚠️ **Importante:** El operador `GlueJobSensor` **no admite** el argumento `region_name` a partir de versiones recientes del proveedor AWS para Airflow. La region se debe especificar unicamente en la conexion `aws_default`.

## Analisis de Logs y Solucion de Errores

### `XComNotFound`
Significa que la tarea previa no devolvio el valor esperado o fallo antes de devolver un XCom valido.

**Solucion:** asegurar que la tarea se ejecuto correctamente y que retorna un dict si usas `.output['clave']` o un valor simple para usar `.xcom_pull(...)` directamente.

### `Invalid type for parameter Arguments`
Pasa cuando `script_args` es un string en vez de dict.

**Solucion:** asegurar que `script_args` sea tipo `dict`, como `{ "--arg": "value" }`.

### `Invalid type for parameter JobName, value: None`
Este error ocurre cuando `GlueJobSensor` intenta leer un `job_name` y `run_id` que no fueron pasados correctamente via XCom.

**Solucion:** utilizar `xcom_pull` dentro de la funcion decorada para obtener los valores directamente de las tareas previas.

## Diagrama de Flujo del DAG

```
leer_config
    ↓
ejecutar_glue
    ↓
esperar_finalizacion (Sensor)
    ↓
terminar (EmptyOperator)
```

Con esta estructura final, tu DAG es dinamico, limpio y cumple con las practicas recomendadas para ejecutar y monitorear trabajos de AWS Glue desde Airflow. ✅

