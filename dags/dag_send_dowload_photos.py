import logging
import pandas as pd
import requests
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.drawing.image import Image  # Para insertar imagenes
from openpyxl.chart import BarChart, Reference
from PIL import Image as PILImage
from io import BytesIO

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email_smtp
from airflow.exceptions import AirflowException
from datetime import timedelta
import os

default_args = {
    'owner': 'Diego Atzin',
    'start_date': days_ago(1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

url = "https://jsonplaceholder.typicode.com/albums/1/photos"

def create_pandas_df(url:str, **kwargs):
    try:
        response = requests.get(url)
        data = response.json()

        # Crear un DataFrame con los datos relevantes
        df = pd.DataFrame(data)
        df = df[['id', 'title', 'url', 'thumbnailUrl']]

        kwargs['ti'].xcom_push(key='photos_df', value=df.to_json())  # Guardar el DataFrame en JSON para XCom
        logging.info("DataFrame created and pushed to XCom")
    except Exception as e:
        logging.error("Error downloading data: %s", e)
        raise AirflowException("Failed to create DataFrame")

def create_excel_report(**kwargs):
    try:
        df_json = kwargs['ti'].xcom_pull(key='photos_df', task_ids='get_photos_operator')
        df = pd.read_json(df_json)

        wb = Workbook()
        ws = wb.active
        ws.title = "Photos"

        # Descargar imagen de marcador de posición
        placeholder_img_url = "https://via.placeholder.com/150"  # Imagen de marcador de posición
        placeholder_img_data = BytesIO(requests.get(placeholder_img_url).content)

        # Escribir los datos en la hoja de trabajo
        for r in dataframe_to_rows(df[['id', 'title']], index=False, header=True):
            ws.append(r)

        # Insertar las imágenes en las celdas
        for idx, row in df.iterrows():
            # Descargar la imagen
            response = requests.get(row['thumbnailUrl'])

            # Verificar si el contenido es una imagen
            if response.headers['Content-Type'].startswith('image/'):
                img_data = BytesIO(response.content)
                try:
                    pil_img = PILImage.open(img_data)
                    img = Image(img_data)
                    img.width, img.height = pil_img.size  # Ajustar el tamaño original
                except PILImage.UnidentifiedImageError:
                    logging.error(f"No se pudo identificar la imagen desde {row['thumbnailUrl']}. Usando imagen de marcador de posición.")
                    img = Image(placeholder_img_data)  # Usar imagen de marcador de posición
            else:
                logging.error(f"No se pudo descargar una imagen válida desde {row['thumbnailUrl']}. Usando imagen de marcador de posición.")
                img = Image(placeholder_img_data)

            # Insertar la imagen en la celda correspondiente
            ws.add_image(img, f'D{idx + 2}')  # Ajustar la celda segun el indice

        output_path = "/tmp/reporte_photos_with_images.xlsx"
        wb.save(output_path)
        kwargs['ti'].xcom_push(key='excel_path', value=output_path)
        logging.info("Excel report with images created successfully")
    except Exception as e:
        logging.error("Error creating Excel report: %s", e)
        raise AirflowException("Failed to create Excel report")

def send_email_report(**kwargs):
    try:
        excel_path = kwargs['ti'].xcom_pull(key='excel_path', task_ids='create_excel_operator')
        subject = "Reporte de Fotos del Álbum"
        html_content = """
        <h3>Reporte de Fotos del Álbum</h3>
        <p>Adjunto encontrarás el reporte con información sobre las fotos del álbum, incluyendo imágenes.</p>
        """
        to = ["diego198mayo@gmail.com", "diego198mayotester@gmail.com"]

        files = [excel_path]
        send_email_smtp(
            to=to,
            subject=subject,
            html_content=html_content,
            files=files
        )
        logging.info(f"Email sent successfully to {', '.join(to)}")
    except Exception as e:
        logging.error("Error sending email: %s", e)
        raise AirflowException("Failed to send email")

with DAG(dag_id='dag_send_photos_with_images_report',
         default_args=default_args,
         schedule_interval='@daily',
         tags=['Extrating_From_API', "SMTP"],
         catchup=False) as dag:

    start = EmptyOperator(task_id='start')

    get_photos_operator = PythonOperator(
        task_id="get_photos_operator",
        python_callable=create_pandas_df,
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

    start >> get_photos_operator >> create_excel_operator >> send_email_operator >> end
