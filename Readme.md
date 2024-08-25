# Proyecto Airflow con Docker y Docker Compose

Este proyecto utiliza Apache Airflow, Docker, y Docker Compose para orquestar un entorno de Airflow completamente funcional. Los DAGs se clonan automáticamente desde un repositorio de Git en cada despliegue.

## Estructura del Proyecto

- `Dockerfile`: Define la imagen Docker personalizada que clona los DAGs y configura el entorno de Airflow.
- `docker-compose.yml`: Orquesta los servicios necesarios para ejecutar Airflow, incluyendo la base de datos PostgreSQL, el webserver, y el scheduler.

## Dockerfile

El `Dockerfile` realiza los siguientes pasos:

1. Usa la imagen base de Apache Airflow `apache/airflow:2.6.2`.
2. Instala dependencias necesarias como `libpq-dev`, `gcc`, y `git`.
3. Clona el repositorio de DAGs en `/opt/airflow/dags`.
4. Copia `requirements.txt` al contenedor e instala las dependencias de Python.

```Dockerfile
FROM apache/airflow:2.6.2

# Instalar las dependencias del sistema necesarias para compilar psycopg2
USER root

RUN apt-get update && apt-get install -y \
    libpq-dev gcc git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Cambiar a usuario airflow
USER airflow

# Clonar el repositorio de DAGs en el directorio correcto
RUN git clone https://github.com/tu-usuario/tu-repo-dags.git /opt/airflow/dags

# Copiar el archivo de requerimientos
COPY requirements.txt /requirements.txt

# Instalar las dependencias de Python
RUN pip install --upgrade pip
RUN pip install -r /requirements.txt