from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException

from airflow.utils.dates import days_ago


# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.bash_operator import BashOperator
# from airflow.hooks.postgres_hook import PostgresHook


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

def guardar_fecha(**context):
    postgres = PostgresHook('normal_redshift')
    fecha_max_df = postgres.get_pandas_df('select max(load_datetime) from users')
    fecha_max_list = fecha_max_df.tolist()
    fecha_max = fecha_max_list[0]

    ti = context['task_instance']
    ti.xcom_push(key='fecha', value=fecha_max)

    
with DAG(
    dag_id="dag_normal_redshift",
    default_args=default_args
) as dag:

    start = EmptyOperator(task_id="start")

    obtener_fecha = PythonOperator(task_id='obtener_fecha',
                                python_callable=guardar_fecha,
                                provide_context=True)

    imprimir_fecha = BashOperator(task_id='imprimir_fecha',
                                bash_command="echo {‌{ ti.xcom_pull(task_ids='obtener_fecha', key='fecha') }}")

    end = EmptyOperator(task_id = "end")

    start >> obtener_fecha >> imprimir_fecha >> end