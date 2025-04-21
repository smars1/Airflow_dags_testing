from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator
import random


# Opciones de restaurantes y URLs de imágenes
BREAKFAST_OPTIONS = [
    {"name": "La Cafeteria - Café del día", "image": "/opt/airflow/assets/cafe.jpg"},
    {"name": "La Panadería - Croissants y café", "image": "/opt/airflow/assets/panaderia.jpg"},
    {"name": "Frutitas - Desayunos saludables", "image": "/opt/airflow/assets/fruta.jpg"}
]

LUNCH_OPTIONS = [
    {"name": "Restaurante Central - Almuerzos completos", "image": "/opt/airflow/assets/almuerzo_completo.jpg"},
    {"name": "El Rincón del Sabor - Menú económico", "image": "/opt/airflow/assets/almuerzo_eco.jpg"},
    {"name": "Salad & Co - Ensaladas y wraps", "image": "/opt/airflow/assets/wrap_variados.jpg"}
]

DINNER_OPTIONS = [
    {"name": "La Pizzería - Pizza italiana", "image": "/opt/airflow/assets/pizza.jpeg"},
    {"name": "Sushi Time - Comida japonesa", "image": "/opt/airflow/assets/sushi.jpg"},
    {"name": "Asados Grill - Carnes y parrilladas", "image": "/opt/airflow/assets/grill.jpg"}
]

# Función para seleccionar opciones de comida
def select_restaurant_options():
    breakfast = random.choice(BREAKFAST_OPTIONS)
    lunch = random.choice(LUNCH_OPTIONS)
    dinner = random.choice(DINNER_OPTIONS)
    
    return breakfast, lunch, dinner

# Función para generar el mensaje de correo en formato HTML con imágenes y estilo minimalista
def compose_email_body():
    breakfast, lunch, dinner = select_restaurant_options()
    
    email_body = f"""
    <html>
        <body style="font-family: Arial, sans-serif; background-color: #f4f4f4; padding: 20px;">
            <div style="max-width: 600px; margin: 0 auto; background-color: #ffffff; padding: 20px; border-radius: 8px; box-shadow: 0px 2px 10px rgba(0, 0, 0, 0.1);">
                <h2 style="text-align: center; color: #333333; font-family: Georgia, serif;">🍽️ Opciones de Restaurantes 🍽️</h2>
                <p style="color: #666666; text-align: center;">Las mejores opciones para ti hoy:</p>
                
                <!-- Sección de desayuno -->
                <div style="margin-bottom: 20px; text-align: center;">
                    <h3 style="color: #555555; font-family: Georgia, serif;">Desayuno</h3>
                    <img src="{breakfast['image']}" alt="Desayuno" style="width: 100%; max-width: 150px; border-radius: 12px; box-shadow: 0px 4px 8px rgba(0, 0, 0, 0.1);">
                    <p style="color: #777777; margin-top: 10px;">{breakfast['name']}</p>
                </div>
                
                <!-- Sección de almuerzo -->
                <div style="margin-bottom: 20px; text-align: center;">
                    <h3 style="color: #555555; font-family: Georgia, serif;">Almuerzo</h3>
                    <img src="{lunch['image']}" alt="Almuerzo" style="width: 100%; max-width: 150px; border-radius: 12px; box-shadow: 0px 4px 8px rgba(0, 0, 0, 0.1);">
                    <p style="color: #777777; margin-top: 10px;">{lunch['name']}</p>
                </div>
                
                <!-- Sección de cena -->
                <div style="margin-bottom: 20px; text-align: center;">
                    <h3 style="color: #555555; font-family: Georgia, serif;">Cena</h3>
                    <img src="{dinner['image']}" alt="Cena" style="width: 100%; max-width: 150px; border-radius: 12px; box-shadow: 0px 4px 8px rgba(0, 0, 0, 0.1);">
                    <p style="color: #777777; margin-top: 10px;">{dinner['name']}</p>
                </div>
                
                <!-- Botón de acción -->
                <div style="text-align: center; margin-top: 20px;">
                    <a href="#" style="text-decoration: none; background-color: #333333; color: #ffffff; padding: 10px 25px; border-radius: 5px; font-size: 16px;">Ver más opciones</a>
                </div>

                <div style="text-align: center; margin-top: 20px;">
                    <p style="font-size: 12px; color: #999999;">Correo automático enviado desde Airflow.</p>
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
    dag_id='daily_restaurant_options_email_minimal',
    default_args=default_args,
    description='Envía un correo diario con opciones de restaurantes para desayuno, almuerzo y cena con un diseño minimalista y moderno',
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
        to=["diego198mayo@gmail.com", "diego198mayotester@gmail.com"],  # Cambia esto por tu email
        subject='Opciones de Restaurantes para Hoy',
        html_content="{{ task_instance.xcom_pull(task_ids='compose_email') }}",
    )

    # Definir el flujo de tareas
    compose_email >> send_email
