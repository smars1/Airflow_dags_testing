# Proyecto Airflow con Docker y Docker Compose

Este proyecto utiliza Apache Airflow, Docker, y Docker Compose para orquestar un entorno de Airflow completamente funcional. Los DAGs se clonan automaticamente desde un repositorio de Git en cada despliegue.

## Estructura del Proyecto

- `Dockerfile`: Define la imagen Docker personalizada que clona los DAGs y configura el entorno de Airflow.
- `docker-compose.yml`: Orquesta los servicios necesarios para ejecutar Airflow, incluyendo la base de datos PostgreSQL, el webserver, y el scheduler.

## Dockerfile

El `Dockerfile` realiza los siguientes pasos:

1. Usa la imagen base de Apache Airflow `apache/airflow:2.6.2`.
2. Instala dependencias necesarias como `libpq-dev`, `gcc`, y `git`.
3. Clona el repositorio de DAGs en `/opt/airflow/dags`.
4. Copia `requirements.txt` al contenedor e instala las dependencias de Python.



# Clonacion de la Carpeta `dags` en Docker

Este documento describe como configurar un `Dockerfile` para clonar y utilizar solo la carpeta `dags` desde un repositorio Git en un entorno de Apache Airflow, evitando conflictos con otros archivos del repositorio.

```dockerfile
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
```

## Métodos para Clonar Solo `dags`

### Método 1: Clonar Todo y Copiar Solo `dags`

    Este método clona todo el repositorio en un directorio temporal dentro del contenedor Docker y luego mueve solo la carpeta `dags` a la ubicacion correcta en `/opt/airflow/dags`. Finalmente, se elimina el resto del repositorio para mantener el contenedor limpio.

#### Dockerfile

```dockerfile
FROM apache/airflow:2.6.2

# Instalar las dependencias del sistema necesarias para compilar psycopg2 y git
USER root

RUN apt-get update && apt-get install -y \
    libpq-dev gcc git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Cambiar a usuario airflow
USER airflow

# Clonar el repositorio de DAGs en un directorio temporal
RUN git clone https://github.com/tu-usuario/tu-repo-dags.git /tmp/repo && \
    mv /tmp/repo/dags/* /opt/airflow/dags/ && \
    rm -rf /tmp/repo

# Copiar el archivo de requerimientos
COPY requirements.txt /requirements.txt

# Instalar las dependencias de Python
RUN pip install --upgrade pip
RUN pip install -r /requirements.txt
```
## Explicacion
    - Clonacion Temporal: El repositorio completo se clona en /tmp/repo.
    - Mover dags: Solo los archivos de la carpeta dags se mueven a /opt/airflow/dags.
    - Limpieza: Se elimina el resto del repositorio para mantener el contenedor limpio.

# Método 2: Usar git sparse-checkout para Clonar Solo dags
Este método utiliza git sparse-checkout para clonar directamente solo la carpeta dags, evitando así la necesidad de clonar el resto del repositorio. Requiere Git 2.25 o superior.

```dockerfile
FROM apache/airflow:2.6.2

# Instalar las dependencias del sistema necesarias para compilar psycopg2 y git
USER root

RUN apt-get update && apt-get install -y \
    libpq-dev gcc git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Cambiar a usuario airflow
USER airflow

# Configurar sparse-checkout para clonar solo la carpeta dags
RUN git init /opt/airflow/dags && \
    cd /opt/airflow/dags && \
    git remote add origin https://github.com/tu-usuario/tu-repo-dags.git && \
    git config core.sparseCheckout true && \
    echo "dags/*" >> /opt/airflow/dags/.git/info/sparse-checkout && \
    git pull origin master

# Copiar el archivo de requerimientos
COPY requirements.txt /requirements.txt

# Instalar las dependencias de Python
RUN pip install --upgrade pip
RUN pip install -r /requirements.txt
```

# Explicacion
    - Inicializacion de Git: Se inicializa un repositorio vacío en /opt/airflow/dags.
    - Configuracion de Sparse-Checkout: Se configura Git para clonar solo los archivos de la carpeta dags.
    -  Pull de dags: Se realiza un git pull para obtener solo los archivos necesarios.

# Consideraciones
    - Método 1: Es mas simple y compatible, ya que clona todo el repositorio y luego mueve solo lo necesario.
    - Método 2: Es mas eficiente en cuanto a uso de espacio y tiempo de clonacion, pero requiere una version mas reciente de Git.

# Uso en docker-compose.yml
No necesitas hacer cambios adicionales en tu docker-compose.yml si estas utilizando uno de estos métodos, ya que la clonacion y copia estan manejadas en el Dockerfile.