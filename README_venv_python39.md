
# Crear un Entorno Virtual con Python 3.9 en Windows usando `venv`

Este proyecto describe cómo crear y activar un entorno virtual en Windows usando `venv` con Python 3.9, ideal para proyectos como Airflow.

---

## ✅ Requisitos Previos

- Tener instaladas múltiples versiones de Python.
- Confirmar que Python 3.9 está disponible ejecutando:

```powershell
py -0
```

Ejemplo de salida:

```
 -V:3.11 *        Python 3.11 (64-bit)
 -V:3.9           Python 3.9 (64-bit)
```

---

## 🧱 Crear el Entorno Virtual

1. Abrí PowerShell y ubicáte en la carpeta donde querés crear el entorno virtual.

2. Ejecutá el siguiente comando:

```powershell
py -3.9 -m venv venv_airflow
```

Esto creará una carpeta llamada `venv_airflow` con el entorno virtual basado en Python 3.9.

---

## ▶️ Activar el Entorno Virtual

3. Activá el entorno con:

```powershell
.env_airflow\Scripts\Activate.ps1
```

### ⚠️ Si ves un error como `ExecutionPolicy`, ejecutá:

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

Luego volvé a intentar activar el entorno.

---

## 🔎 Verificar la Versión de Python

4. Una vez activado, verificá que estás usando Python 3.9:

```powershell
python --version
```

Salida esperada:

```
Python 3.9.x
```

---

## 📦 (Opcional) Instalar Requisitos de Airflow

Para instalar los requisitos básicos de Airflow:

```powershell
pip install apache-airflow
```

O bien, usá un archivo `requirements.txt` personalizado.

```powershell
pip install -r requirements.txt
```

---

## 🧹 Salir del Entorno

Cuando termines, podés desactivar el entorno virtual con:

```powershell
deactivate
```

---

## 📝 Notas

- Este procedimiento es útil para entornos aislados donde necesites una versión específica de Python.
- Podés usar este entorno para proyectos como DAGs de Apache Airflow, scripts de automatización, entre otros.
