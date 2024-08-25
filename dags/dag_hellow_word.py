from datetime import datetime

from airflow.models import DAG
#airflow 1.10.15 >= (airflow 2+) 
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash   import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook




 # definimos nuetros operators airflow v <= 1.10.10
#from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators.python_operator import PythonOperator
#from airflow.operators.bash_operator    import BashOperator

# aiflow actualizad
#from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'Diego Atzin Pineda Cota',
    'start_date': datetime(2024, 7, 9, 20, 53, 0)
}

def helloword_loop():
    for palabra in ["hellow", "word"]:
        print(palabra)



# definifimos el DAG, se recomienda usar el dag como contexto

with DAG(dag_id = 'dag_de_prueba',
         default_args= default_args,
         schedule_interval= '@once' ) as dag:
    

    #airflow 1.10.15 >= (airflow 2+) 

    start = EmptyOperator(task_id='start')

    prueba_python = PythonOperator(task_id= 'prueba_python',
                                   python_callable = helloword_loop)


    prueba_bash = BashOperator(task_id= 'prueba_bash',
                               bash_command= 'echo {{ ds }} prueba_bash')
    
    prueba_bash_pip_list = BashOperator(task_id= 'prueba_bash_pip_list',
                               bash_command= 'pip3 list')




    # definimos nuetros operators airflow v <= 1.10.10

    #start = DummyOperator(task_id = 'start')
 
    
    #prueba_python = PythonOperator(task_id= 'prueba_python',
                                   #python_callable = helloword_loop)


    #prueba_bash = BashOperator(task_id= 'prueba_bash',
                               #bash_command= 'echo {{ ds }} prueba_bash')
    
    #prueba_bash_pip_list = BashOperator(task_id= 'prueba_bash_pip_list',
                               #bash_command= 'pip3 list')



# definimos el orden de ejecucion de cada uno
start >> prueba_python >> prueba_bash >> prueba_bash_pip_list