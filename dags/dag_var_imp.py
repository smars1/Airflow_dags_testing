from airflow.models import Variable
from airflow.models import DAG
from airflow.utils import dates 
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


# AIRFLOW 2.6.2
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.operators.python_operator import PythonOperator


variables = Variable.get("variables", deserialize_json=True)

default_args = {
    "start_date": days_ago(1),
    "s3_bucket": variables["s3_bucket_test"],
    "email": variables["email"]
}


def call_s3():
   print(variables["s3_bucket_test"])

with DAG(
    dag_id ='dag_var_import',
         default_args = default_args,
         schedule_interval = '@once' ) as dag:
    
    start = EmptyOperator(task_id="start")

    s3redshift_operator = PythonOperator(task_id= "key",
                                        python_callable = call_s3
                                        )
    
    start >> s3redshift_operator