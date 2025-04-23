# from utils.airflow_templates import DagAirflow, standard_dataflow
# import json
# import os

# # Ruta dinámica desde archivo de configuración JSON
# CONFIG_PATH = os.path.join(os.path.dirname(__file__), '../configs/dataflow_configs.json')

# with open(CONFIG_PATH, 'r') as file:
#     dag_config = json.load(file)

# dag_airflow = DagAirflow(
#     dag_id=dag_config["dag_id"],
#     schedule=dag_config["schedule_interval"],
#     retries=dag_config["retries"],
#     retry_delay=dag_config["retry_delay_minutes"]
# ).get_dag()

# Crear tareas dinámicamente desde el JSON
# dag = standard_dataflow(dag_airflow, dag_config["tasks"])
