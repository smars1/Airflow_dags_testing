from datetime import datetime

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash    import BashOperator


# para airflow V: 1.10
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.bash_operator    import BashOperator

# aiflow actualizad
#from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'Diego Atzin Pineda Cota',
    'start_date': datetime(2024, 7, 9, 20, 53, 0)
}

# en el caso de python operator se comparte el return
def helloword_loop(**context):
    for palabra in ["hellow", "word"]:
        print(palabra)

    # iniciamos un objeto de task_instance
    task_instance = context['task_instance']
    ti = task_instance.xcom_push(key='clacve_test', value='valor_test')
    
    return palabra

def pull_test(**context):
    ti = context['task_instance']
    valor_salvado = ti.xcom_pull(task_ids = "prueba_python_xcom_push")
    print(valor_salvado)

# definifimos el DAG, se recomienda usar el dag como contexto

with DAG(dag_id = 'dag_xcom_test',
         default_args= default_args,
         schedule_interval= '@once' ) as dag:
    
    # definimos nuetros operators

    start = EmptyOperator(task_id = 'start')
 
    
    prueba_python_xcom_push = PythonOperator(task_id= 'prueba_python_xcom_push',
                                   python_callable = helloword_loop,
                                   do_xcom_push = True,
                                   provide_context=True
                                   )
    prueba_python_xcom_pull = PythonOperator(task_id= 'prueba_python_xcom_pull',
                                   python_callable = pull_test,
                                   do_xcom_push = True,
                                   provide_context=True
                                   )


    prueba_bash = BashOperator(task_id= 'prueba_bash',
                               bash_command= 'echo prueba_bash')
    
    # do_xcom_push nos permite compartir informacion entre task
    # en un bash operator se comparte el output
    prueba_bash_pip_list = BashOperator(task_id= 'prueba_bash_pip_list',
                               bash_command= 'pip3 list',
                               do_xcom_push = True)


    end = EmptyOperator(task_id = 'end')

# definimos el orden de ejecucion de cada uno
start >> prueba_python_xcom_push >> prueba_python_xcom_pull >> prueba_bash >> prueba_bash_pip_list >> end