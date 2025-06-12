# Generacion y Uso de la Clave Fernet en Apache Airflow

Apache Airflow requiere una clave **Fernet** para cifrar y descifrar datos sensibles como contraseñas en conexiones y variables.

---

## ᴾ Por que es necesaria la Fernet Key?

* Airflow utiliza Fernet para **cifrar datos sensibles** almacenados en la base de datos.
* Sin esta clave, se muestran errores como:

```
Could not create Fernet object: Fernet key must be 32 url-safe base64-encoded bytes.
```

---

## ᴼ Como generar la Fernet Key

Puedes generarla tanto fuera como dentro del contenedor Docker.

### Opcion 1: Desde tu sistema local (recomendado)

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

* Si necesitas instalar la libreria:

```bash
pip install cryptography
```

### Opcion 2: Desde dentro de un contenedor Airflow

```bash
docker exec -it airflow_dags_testing-webserver-1 python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

---

## ᴼ Donde usarla

Copia la clave generada y colócala en tu archivo `.env` como:

```dotenv
AIRFLOW__CORE__FERNET_KEY=3br5oelRUTtNvYQKD_s9co0UHR3E-bTKlKXqcmOxMFc=
```

---

## ᴼ Reiniciar servicios

Luego de actualizar la clave Fernet en tu archivo `.env`, reinicia los contenedores:

```bash
docker compose --env-file .env down

docker compose --env-file .env up -d --build
```

---

## ᴼ Consideraciones

* La clave debe tener exactamente **32 bytes codificados en base64**.
* Nunca cambies la clave Fernet en un sistema que ya tiene datos cifrados, o **perderás acceso a esos datos cifrados previamente**.

---

> ✅ Recomendado: Generar la Fernet Key una vez y mantenerla segura en un archivo `.env` o gestor de secretos.
