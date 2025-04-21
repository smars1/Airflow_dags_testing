from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator
import random

# Opciones de restaurantes
BREAKFAST_OPTIONS = [
    "Café del día - Desayunos rápidos",
    "La Panadería - Croissants y café",
    "Frutitas - Desayunos saludables"
]

LUNCH_OPTIONS = [
    "Restaurante Central - Almuerzos completos",
    "El Rincón del Sabor - Menú económico",
    "Salad & Co - Ensaladas y wraps"
]

DINNER_OPTIONS = [
    "La Pizzería - Pizza italiana",
    "Sushi Time - Comida japonesa",
    "Asados Grill - Carnes y parrilladas"
]

# Función para seleccionar opciones de comida
def select_restaurant_options():
    breakfast = random.choice(BREAKFAST_OPTIONS)
    lunch = random.choice(LUNCH_OPTIONS)
    dinner = random.choice(DINNER_OPTIONS)
    
    return breakfast, lunch, dinner

# Función para generar el mensaje de correo en formato HTML
def compose_email_body():
    breakfast, lunch, dinner = select_restaurant_options()
    
    email_body = f"""
    <html>
        <body style="font-family: Arial, sans-serif; background-color: #f7f7f7; padding: 20px;">
            <div style="max-width: 600px; margin: 0 auto; background-color: #ffffff; padding: 20px; border-radius: 10px; box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.1);">
                <h2 style="text-align: center; color: #333;">¡Opciones de Restaurantes para Hoy!</h2>
                <p style="color: #555;">Aquí tienes las opciones recomendadas de restaurantes para hoy:</p>
                <div style="margin-bottom: 20px;">
                    <h3 style="color: #333;">Desayuno</h3>
                    <p style="color: #777;">{breakfast}</p>
                </div>
                <div style="margin-bottom: 20px;">
                    <h3 style="color: #333;">Almuerzo</h3>
                    <p style="color: #777;">{lunch}</p>
                </div>
                <div style="margin-bottom: 20px;">
                    <h3 style="color: #333;">Cena</h3>
                    <p style="color: #777;">{dinner}</p>
                </div>
                <div style="text-align: center; margin-top: 20px;">
                    <p style="font-size: 12px; color: #999;">Este es un correo automático enviado desde Airflow.</p>
                </div>
            </div>
        </body>
    </html>
    """
    
    return email_body

# DAG settings
default_args = {
    'owner': 'airflow',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Definir el DAG
with DAG(
    dag_id='daily_restaurant_options_email_custom',
    default_args=default_args,
    description='Envía un correo diario personalizado con opciones de restaurantes para desayuno, almuerzo y cena',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['Test_restuarante'],
) as dag:

    # Composición del correo
    compose_email = PythonOperator(
        task_id='compose_email',
        python_callable=compose_email_body
    )

    # Enviar el correo
    send_email = EmailOperator(
        task_id='send_email',
        to= ["diego198mayo@gmail.com", "diego198mayotester@gmail.com"],  # Cambia esto por tu email
        subject='Opciones de Restaurantes para Hoy',
        html_content="{{ task_instance.xcom_pull(task_ids='compose_email') }}",
    )

    # Definir el flujo de tareas
    compose_email >> send_email
