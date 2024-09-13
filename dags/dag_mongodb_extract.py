from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pymongo import MongoClient
import logging

import os
from dotenv import load_dotenv

load_dotenv()

user =  os.getenv("MONGO_INITDB_ROOT_USERNAME")
password =  os.getenv("MONGO_INITDB_ROOT_PASSWORD")

default_args = {
    'owner': 'Diego Atzin',
    'start_date': days_ago(1),
    'email_on_failure': True,
    'retries': 1,
}

def extract_data_from_mongodb():
    try:
        # Conectar a MongoDB
        client = MongoClient(f'mongodb+srv://{user}:{password}@testing-lab.6ztu2.mongodb.net/?retryWrites=true&w=majority&appName=testing-lab')
        db = client["Wolfbase"]
        collection = db["Users"]

        # Extraer datos
        data = collection.find({})
        for document in data:
            logging.info(document)
        
        client.close()
        
    except Exception as e:
        logging.error("Error extracting data from MongoDB: %s", e)
        raise

with DAG(dag_id='dag_mongodb_extract',
         default_args=default_args,
         tags=['Extrating_MongoDB'],
         schedule_interval='@daily') as dag:
    
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_from_mongodb,
    )
