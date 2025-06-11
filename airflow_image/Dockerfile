FROM apache/airflow:2.9.0-python3.9
#FROM apache/airflow:2.6.2
#FROM apache/airflow:2.8.0
# Cambiar al usuario root para instalar dependencias del sistema
USER root

# Copia la carpeta de assets (imágenes) al contenedor
COPY assets /opt/airflow/assets/

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    libpq-dev gcc bash \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

#USER root
RUN apt-get update && apt-get install -y git && apt-get clean
#USER airflow
# Cambiar al usuario airflow para manejar las instalaciones de Python
USER airflow

# Copiar el archivo .env
COPY .env /opt/airflow/.env

# Copiar y instalar las dependencias de Python
COPY requirements.txt /requirements.txt
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r /requirements.txt

# Copiar el resto del codigo fuente al contenedor
#COPY . /opt/airflow/

# Copiar el script que descargará los DAGs de GitHub
# COPY entrypoint.sh /entrypoint.sh
# USER root
# RUN chmod +x /entrypoint.sh
# USER airflow
# # Usar el entrypoint modificado para clonar DAGs y arrancar Airflow
# ENTRYPOINT ["/entrypoint.sh"]

