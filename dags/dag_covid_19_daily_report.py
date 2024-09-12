import logging
import pandas as pd
import requests
from openpyxl import Workbook
from openpyxl.chart import LineChart, Reference
from openpyxl.utils.dataframe import dataframe_to_rows

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email_smtp
from airflow.exceptions import AirflowException
from datetime import timedelta
import os

default_args = {
    'start_date': days_ago(1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

url = "https://api.covidtracking.com/v1/us/daily.json"

def create_pandas_df(url:str, **kwargs):
    try:
        response = requests.get(url)
        data = response.json()
        df = pd.DataFrame(data)
        df = df[['date', 'positive', 'death', 'hospitalizedCurrently']]
        df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
        df = df.sort_values('date')
        kwargs['ti'].xcom_push(key='covid_df', value=df.to_json())  # Guardar el DataFrame en JSON para XCom
        logging.info("DataFrame created and pushed to XCom")
    except Exception as e:
        logging.error("Error downloading data: %s", e)
        raise AirflowException("Failed to create DataFrame")

def create_excel_report(**kwargs):
    try:
        df_json = kwargs['ti'].xcom_pull(key='covid_df', task_ids='get_pandas_operator')
        df = pd.read_json(df_json)

        wb = Workbook()
        ws = wb.active

        for r in dataframe_to_rows(df, index=False, header=True):
            ws.append(r)

        chart = LineChart()
        data = Reference(ws, min_col=2, min_row=1, max_col=4, max_row=len(df) + 1)
        chart.add_data(data, titles_from_data=True)
        chart.title = "COVID-19 en EE.UU."
        chart.x_axis.title = "Fecha"
        chart.y_axis.title = "Conteo"
        ws.add_chart(chart, "E5")

        output_path = "/tmp/reporte_covid_con_grafico.xlsx"
        wb.save(output_path)
        kwargs['ti'].xcom_push(key='excel_path', value=output_path)
        logging.info("Excel report created successfully")
    except Exception as e:
        logging.error("Error creating Excel report: %s", e)
        raise AirflowException("Failed to create Excel report")

def send_email_report(**kwargs):
    try:
        excel_path = kwargs['ti'].xcom_pull(key='excel_path', task_ids='create_excel_operator')
        subject = "Reporte diario de COVID-19"
        html_content = """
        <h3>Reporte Diario de COVID-19</h3>
        <p>Adjunto encontrarás el reporte actualizado con los últimos datos sobre el COVID-19 en EE.UU.</p>
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

with DAG(dag_id='dag_send_covid19_daily_report',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    start = EmptyOperator(task_id='start')

    get_pandas_df_operator = PythonOperator(
        task_id="get_pandas_operator",
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

    start >> get_pandas_df_operator >> create_excel_operator >> send_email_operator >> end
