import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pymongo import MongoClient
from airflow.utils.email import send_email_smtp
from jinja2 import Template
#from dotenv import load_dotenv

# Cargar las variables de entorno
#load_dotenv()

user = os.getenv("MONGO_INITDB_ROOT_USERNAME")
password = os.getenv("MONGO_INITDB_ROOT_PASSWORD")

default_args = {
    'start_date': days_ago(1),
    'email_on_failure': True,
    'retries': 1,
}

def extract_and_send_email():
    try:
        # Conectar a MongoDB
        client = MongoClient(f'mongodb+srv://{user}:{password}@testing-lab.6ztu2.mongodb.net/?retryWrites=true&w=majority&appName=testing-lab')
        db = client["Wolfbase"]
        collection = db["Users"]

        # Extraer datos
        data = collection.find({})
        users = list(data)

        # Crear una plantilla Jinja2 personalizada con estilo y logo
        template = Template("""
        <html>
        <head>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    background-color: #f4f4f4;
                    color: #333;
                    margin: 0;
                    padding: 20px;
                }
                h3 {
                    color: #4CAF50;
                    text-align: center;
                }
                table {
                    width: 100%;
                    border-collapse: collapse;
                    margin: 20px 0;
                }
                table, th, td {
                    border: 1px solid #ddd;
                }
                th, td {
                    padding: 12px;
                    text-align: left;
                }
                th {
                    background-color: #4CAF50;
                    color: white;
                }
                tr:nth-child(even) {
                    background-color: #f2f2f2;
                }
                .logo {
                    text-align: center;
                    margin-bottom: 20px;
                }
            </style>
        </head>
        <body>
            <div class="logo">
                <img src="https://w7.pngwing.com/pngs/579/1/png-transparent-gray-wolf-emerald-green-desktop-emerald-leaf-desktop-wallpaper-emerald-thumbnail.png" alt="Wolf Logo" width="100" height="100">
            </div>
            <h3>Datos de Usuarios Extraídos de MongoDB</h3>
            <table>
                <thead>
                    <tr>
                        <th>ID</th>
                        <th>Username</th>
                        <th>Name</th>
                        <th>Email</th>
                        <th>Clase</th>
                        <th>Age</th>
                        <th>Disabled</th>
                    </tr>
                </thead>
                <tbody>
                {% for user in users %}
                    <tr>
                        <td>{{ user.get('_id') }}</td>
                        <td>{{ user.get('username') }}</td>
                        <td>{{ user.get('name') }}</td>
                        <td>{{ user.get('email') }}</td>
                        <td>{{ user.get('clase') }}</td>
                        <td>{{ user.get('age') }}</td>
                        <td>{{ user.get('disabled') }}</td>
                    </tr>
                {% endfor %}
                </tbody>
            </table>
        </body>
        </html>
        """)

        # Renderizar la plantilla con los datos extraídos
        html_content = template.render(users=users)

        # Enviar el correo
        send_email_smtp(
            to='diego198mayo@gmail.com',
            subject='Datos de Usuarios Extraídos de MongoDB',
            html_content=html_content
        )
        
        client.close()
        
    except Exception as e:
        logging.error("Error extracting data from MongoDB or sending email: %s", e)
        raise

with DAG(dag_id='dag_send_email_personalizado',
         default_args=default_args,
         schedule_interval='@daily') as dag:
    
    extract_and_send_email_task = PythonOperator(
        task_id='extract_and_send_email',
        python_callable=extract_and_send_email,
    )
