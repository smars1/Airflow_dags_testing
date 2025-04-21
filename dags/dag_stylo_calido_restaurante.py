from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.email import EmailOperator
import random

# Opciones de restaurantes con URLs de imágenes de S3
BREAKFAST_OPTIONS = [
    {"name": "La Cafeteria - Café del día", "image": "https://atzin-bucket.s3.amazonaws.com/assets/cafe.jpg"},
    {"name": "La Panadería - Croissants y café", "image": "https://atzin-bucket.s3.amazonaws.com/assets/panaderia.jpg"},
    {"name": "Frutas del dia - Desayunos saludables", "image": "https://atzin-bucket.s3.amazonaws.com/assets/fruta.jpg"}
]

LUNCH_OPTIONS = [
    {"name": "Restaurante Central - Almuerzos completos", "image": "https://atzin-bucket.s3.amazonaws.com/assets/almuerzo_completo.jpg"},
    {"name": "El Rincón del Sabor - Menú económico", "image": "https://atzin-bucket.s3.amazonaws.com/assets/almuerzo_eco.jpg"},
    {"name": "Salad & Co - Ensaladas y wraps", "image": "https://atzin-bucket.s3.amazonaws.com/assets/wrap_variados.jpg"}
]

DINNER_OPTIONS = [
    {"name": "La Pizzería - Pizza italiana", "image": "https://atzin-bucket.s3.amazonaws.com/assets/pizza.jpeg"},
    {"name": "Sushi Time - Comida japonesa", "image": "https://atzin-bucket.s3.amazonaws.com/assets/sushi.jpg"},
    {"name": "Asados Grill - Carnes y parrilladas", "image": "https://atzin-bucket.s3.amazonaws.com/assets/grill.jpg"}
]


# Función para seleccionar opciones de comida
def select_restaurant_options():
    breakfast = random.choice(BREAKFAST_OPTIONS)
    lunch = random.choice(LUNCH_OPTIONS)
    dinner = random.choice(DINNER_OPTIONS)
    
    return breakfast, lunch, dinner

# Función para generar el mensaje de correo en formato HTML con imágenes y estilo personalizado
def compose_email_body():
    breakfast, lunch, dinner = select_restaurant_options()
    
    email_body = f"""
    <html>
        <body style="font-family: Arial, sans-serif; background-color: #f5e6cc; padding: 20px;">
            <div style="max-width: 600px; margin: 0 auto; background-color: #ffffff; padding: 20px; border-radius: 10px; box-shadow: 0px 0px 10px rgba(0, 0, 0, 0.1); text-align: center;">
                <h2 style="text-align: center; color: #d35400;">🍽️ ¡Opciones de Restaurantes para Hoy! 🍽️</h2>
                <p style="color: #555; text-align: center;">Aquí tienes las opciones recomendadas de restaurantes para hoy:</p>
                
                <!-- Sección de desayuno -->
                <div style="margin-bottom: 20px; text-align: center;">
                    <h3 style="color: #e67e22;">Desayuno</h3>
                    <img src="{breakfast['image']}" alt="Desayuno" style="width: 100%; max-width: 150px; border-radius: 10px;">
                    <p style="color: #777;">{breakfast['name']}</p>
                </div>
                
                <!-- Sección de almuerzo -->
                <div style="margin-bottom: 20px; text-align: center;">
                    <h3 style="color: #e67e22;">Almuerzo</h3>
                    <img src="{lunch['image']}" alt="Almuerzo" style="width: 100%; max-width: 150px; border-radius: 10px;">
                    <p style="color: #777;">{lunch['name']}</p>
                </div>
                
                <!-- Sección de cena -->
                <div style="margin-bottom: 20px; text-align: center;">
                    <h3 style="color: #e67e22;">Cena</h3>
                    <img src="{dinner['image']}" alt="Cena" style="width: 100%; max-width: 150px; border-radius: 10px;">
                    <p style="color: #777;">{dinner['name']}</p>
                </div>
                
                <!-- Botón de acción -->
                <div style="text-align: center; margin-top: 20px;">
                    <a href="#" style="text-decoration: none; background-color: #d35400; color: #ffffff; padding: 10px 20px; border-radius: 5px; font-size: 16px;">Ver más opciones</a>
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
    dag_id='daily_restaurant_options_email_with_colors',
    default_args=default_args,
    description='Envía un correo diario personalizado con imágenes de opciones de restaurantes para desayuno, almuerzo y cena con colores cálidos y modernos',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['Test_restuarante']
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
