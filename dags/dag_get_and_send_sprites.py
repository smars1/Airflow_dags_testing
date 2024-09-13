import logging
import pandas as pd
import requests
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.drawing.image import Image  # Para insertar imagenes
from PIL import Image as PILImage, UnidentifiedImageError
from io import BytesIO
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email_smtp
from airflow.exceptions import AirflowException
from datetime import timedelta

default_args = {
    'owner': 'Diego Atzin',
    'start_date': days_ago(1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

# URL de la API de Charmander en PokeAPI
url = "https://pokeapi.co/api/v2/pokemon/charmander"

# Funcion para recorrer las estructuras anidadas y extraer las URLs de imágenes
def extract_sprite_urls(sprites):
    sprite_data = []
    for key, value in sprites.items():
        if isinstance(value, str) and value.startswith("http"):
            sprite_data.append({'sprite_name': key, 'sprite_url': value})
        elif isinstance(value, dict):
            sprite_data.extend(extract_sprite_urls(value))
    return sprite_data

def get_pokemon_data(url: str, **kwargs):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Para capturar errores HTTP
        data = response.json()
        sprites = data['sprites']
        sprite_data = extract_sprite_urls(sprites)
        df = pd.DataFrame(sprite_data)
        kwargs['ti'].xcom_push(key='pokemon_sprites_df', value=df.to_json())  # Guardar el DataFrame en XCom
        logging.info("DataFrame de sprites creado y guardado en XCom")
    except Exception as e:
        logging.error("Error al obtener datos del Pokémon: %s", e)
        raise AirflowException("Fallo al obtener los datos del Pokémon")

def create_excel_report(**kwargs):
    try:
        df_json = kwargs['ti'].xcom_pull(key='pokemon_sprites_df', task_ids='get_pokemon_sprites')
        df = pd.read_json(df_json)

        wb = Workbook()
        ws = wb.active
        ws.title = "Charmander Sprites"

        placeholder_img_url = "https://via.placeholder.com/150"
        placeholder_img_data = BytesIO(requests.get(placeholder_img_url).content)

        # Escribir los nombres de los sprites en la hoja de trabajo
        for r in dataframe_to_rows(df[['sprite_name']], index=False, header=True):
            ws.append(r)

        # Insertar los sprites en las celdas
        for idx, row in df.iterrows():
            sprite_url = row['sprite_url']
            logging.info(f"Descargando imagen desde: {sprite_url}")
            response = requests.get(sprite_url)

            if response.headers.get('Content-Type', '').startswith('image/'):
                img_data = BytesIO(response.content)
                try:
                    pil_img = PILImage.open(img_data)
                    img = Image(img_data)
                    img.width, img.height = pil_img.size
                except UnidentifiedImageError:
                    logging.error(f"No se pudo identificar la imagen {row['sprite_name']}. Usando imagen de marcador de posición.")
                    img = Image(placeholder_img_data)
            else:
                logging.error(f"No se pudo descargar una imagen válida desde {sprite_url}. Usando imagen de marcador de posición.")
                img = Image(placeholder_img_data)

            ws.add_image(img, f'B{idx + 2}')  # Coloca la imagen en la columna B

        output_path = "/tmp/reporte_charmander_sprites.xlsx"
        wb.save(output_path)
        kwargs['ti'].xcom_push(key='excel_path', value=output_path)
        logging.info("Reporte Excel con sprites creado exitosamente")
    except Exception as e:
        logging.error("Error al crear el reporte de Excel: %s", e)
        raise AirflowException("Fallo al crear el reporte de Excel")

# Funcion para enviar el correo
def send_email_report(**kwargs):
    try:
        excel_path = kwargs['ti'].xcom_pull(key='excel_path', task_ids='create_excel_operator')
        subject = "Reporte de Sprites de Charmander"
        html_content = """
        <h3>Reporte de Sprites de Charmander</h3>
        <p>Adjunto encontrarás el reporte con los sprites de Charmander extraídos de la PokeAPI.</p>
        """
        to = ["diego198mayo@gmail.com"]  # Coloca tu email aquí

        files = [excel_path]
        send_email_smtp(
            to=to,
            subject=subject,
            html_content=html_content,
            files=files
        )
        logging.info(f"Correo enviado exitosamente a {', '.join(to)}")
    except Exception as e:
        logging.error("Error al enviar el correo: %s", e)
        raise AirflowException("Fallo al enviar el correo")

# Definir el DAG
with DAG(dag_id='dag_pokemon_sprites_report',
         default_args=default_args,
         schedule_interval='@daily',
         tags=['Extrating_From_API', "SMTP"],
         catchup=False) as dag:

    start = EmptyOperator(task_id='start')

    get_pokemon_sprites = PythonOperator(
        task_id="get_pokemon_sprites",
        python_callable=get_pokemon_data,
        op_kwargs={'url': url}
    )

    create_excel_operator = PythonOperator(
        task_id="create_excel_operator",
        python_callable=create_excel_report
    )

    send_email_operator = PythonOperator(
        task_id="send_email_operator",
        python_callable=send_email_report
    )

    end = EmptyOperator(task_id='end')

    # Definir la secuencia de tareas
    start >> get_pokemon_sprites >> create_excel_operator >> send_email_operator >> end