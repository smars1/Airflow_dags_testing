# Documentación del Entorno Airflow con AWS Glue

## Estructura General del Proyecto

La estructura recomendada para implementar Apache Airflow integrado con AWS Glue es:

```
📁 airflow_project_root/
├── 📁 dags/
│   ├── 📁 utils/
│   │   ├── airflow_templates.py        # utilidades comunes (opcional)
│   │   └── aws_handlers.py             # funciones auxiliares para AWS Glue (opcional)
│   ├── 📁 configs/
│   │   └── glue_configs.json           # configuración dinámica para AWS Glue
│   └── 📁 my_dags/
│       └── aws_glue_dag.py             # DAG principal que orquesta el Glue Job
```

> ✅ **Airflow detecta automáticamente todos los DAGs dentro del directorio `dags/`, incluyendo subcarpetas como `my_dags/`, siempre que la ruta esté correctamente configurada en `AIRFLOW_HOME` o en `airflow.cfg`.**

> ✅ **Airflow detecta automáticamente todos los DAGs dentro del directorio `dags/`, incluyendo subcarpetas como `my_dags/`, siempre que la ruta esté correctamente configurada en `AIRFLOW_HOME` o en `airflow.cfg`.**

## Configuración de Conexión AWS en Airflow

Para configurar la conexión AWS en Airflow:

1. Accede a la interfaz web de Airflow (`Admin → Connections`).
2. Crea una nueva conexión:
   - **Conn Id:** `aws_default` (o personalizado).
   - **Conn Type:** `Amazon Web Services`.
   - **Extra:** (opcional, recomendado)

```json
{
  "aws_access_key_id": "TU_ACCESS_KEY",
  "aws_secret_access_key": "TU_SECRET_KEY",
  "region_name": "us-west-2"
}
```

> Si estás usando Airflow en MWAA o EC2 con roles IAM, puedes dejar el campo Extra vacío y AWS usará las credenciales del entorno.

## Archivo de Configuración JSON

Ejemplo del archivo `glue_configs.json`:

```json
{
    "job_name": "mi_job_glue",
    "script_location": "s3://mi-bucket/scripts/etl_script.py",
    "iam_role_name": "GlueExecutionRole",
    "region": "us-west-2",
    "script_args": {
        "--extra-py-files": "s3://mi-bucket/scripts/libs.zip",
        "--enable-metrics": ""
    }
}
```

Asegúrate de reemplazar estas URLs y valores por recursos específicos de tu entorno AWS:
- `script_location`: ubicación del script ETL en S3.
- `iam_role_name`: rol con permisos para ejecutar Glue.
- `--extra-py-files`: librerías o dependencias adicionales (opcional).

## Descripción del Pipeline

Este pipeline Airflow tiene como propósito:

1. Leer configuración dinámica desde un archivo JSON externo.
2. Iniciar un trabajo AWS Glue mediante `GlueJobOperator`.
3. Monitorear el trabajo Glue usando `GlueJobSensor` hasta su finalización.
4. Confirmar y notificar la ejecución exitosa (puedes agregar notificaciones opcionalmente).

## Ejemplo de DAG AWS Glue

```python
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

    glue_run = GlueJobOperator(
        task_id='launch_glue_job',
        job_name=config['job_name'],
        script_location=config['script_location'],
        iam_role_name=config['iam_role_name'],
        script_args=config['script_args'],
        region_name=config['region'],
        wait_for_completion=False
    )

    glue_sensor = GlueJobSensor(
        task_id='monitor_glue_job',
        job_name=config['job_name'],
        run_id=glue_run.output,
        region_name=config['region'],
        verbose=True,
        poke_interval=60,
        timeout=3600
    )

    @task
    def finalizar():
        print(f"Glue Job {config['job_name']} finalizado correctamente.")

    config >> glue_run >> glue_sensor >> finalizar()

aws_glue_pipeline = aws_glue_etl_pipeline()
```

## Buenas Prácticas

- Utiliza configuraciones externas (JSON) para parametrizar pipelines.
- Aplica logging detallado y manejo explícito de errores.
- Modulariza tu código para facilitar mantenimiento y escalabilidad.
- Configura reintentos automáticos para manejar errores temporales.
- Usa `XComs` o `TaskFlow API` para conectar tareas de forma limpia y mantenible.

---

Esta guía provee un pipeline robusto, automatizado y fácil de mantener para gestionar trabajos ETL usando Apache Airflow y AWS Glue.

