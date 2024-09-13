from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'smars1',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG (
    'dag_git_pull_main',
    default_args=default_args,
    description='DAG para actualizar otros DAGs mediante git pull',
    schedule_interval=None, 
    tags=["GitHub", "Control-Version"],
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    # Script de Bash para hacer git pull y mover los DAGs actualizados
    git_pull_update_dags = BashOperator(
        task_id='git_pull_update_dags',
        bash_command="""
        cd /tmp/repo || git clone https://github.com/smars1/Airflow_dags_testing.git /tmp/repo
        cd /tmp/repo && git pull origin main
        mv /tmp/repo/dags/* /opt/airflow/dags/
        rm -rf /tmp/repo
        """,
        dag=dag,
    )


    end = EmptyOperator(task_id="end")

    start >> git_pull_update_dags >> end