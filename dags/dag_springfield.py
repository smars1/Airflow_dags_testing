from datetime import datetime

from airflow.models import DAG

from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'Homer J. Simpsom',
    'start_date': datetime(2024, 7, 9, 21, 41, 0)
}

def print_tutule():
    print("El uso de airflow en la universidad de Sprinfield")

def print_intro():
    print("El otro día mi hija me dijo que Airflow no se utilizaba en la universidad de Springfield, y yo le dije: que acabas de ver lisa?")

def carajirijillo():
    for i in range(1, 151, 1):
        print(f"{i}:- Pudrete Flanders")

with DAG(dag_id='Homer_dag',
         default_args= default_args,
         schedule_interval='@once') as dag:
    
    start = EmptyOperator(task_id= 'start')

    radaccion_parte_1 = PythonOperator (task_id= 'radaccion_parte_1',
                                            python_callable=print_tutule)
    
    radaccion_parte_2 = PythonOperator (task_id= 'redaccion_parte_2',
                                            python_callable=print_intro)
    
    callete_flanders = PythonOperator(task_id='callete_flanders',
                                        python_callable=carajirijillo)
    

start >> radaccion_parte_1 >> radaccion_parte_2 >> callete_flanders