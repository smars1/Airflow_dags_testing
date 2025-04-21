# utils/oci_handlers.py
import oci
import logging
from airflow.exceptions import AirflowFailException

def run_dataflow(oci_config, application_id, compartment_id, config_run_details, ti):
    data_flow_client = oci.data_flow.DataFlowClient(oci_config)
    
    application = data_flow_client.get_application(application_id=application_id)
    application_name = application.data.display_name.replace(" ", "_")
    logging.info(f"Iniciando Data Flow: {application_name}")

    run_details = oci.data_flow.models.CreateRunDetails(
        compartment_id=compartment_id,
        application_id=application_id,
        display_name=f"PE_AIRFLOW_{application_name}",
        driver_shape_config=oci.data_flow.models.ShapeConfig(
            ocpus=config_run_details.get("v_driver_ocpus", 4),
            memory_in_gbs=config_run_details.get("v_driver_memory_in_gbs", 32)
        ),
        executor_shape_config=oci.data_flow.models.ShapeConfig(
            ocpus=config_run_details.get("v_executor_ocpus", 4),
            memory_in_gbs=config_run_details.get("v_executor_memory_in_gbs", 32)
        ),
        **config_run_details
    )

    run = data_flow_client.create_run(create_run_details=run_details)
    run_id = run.data.id
    logging.info(f"Data Flow iniciado con ID: {run_id}")

    ti.xcom_push(key="id_run_dataflow", value=run_id)

def run_function(oci_config, function_id, invoke_function_body=None):
    functions_client = oci.functions.FunctionsInvokeClient(oci_config)
    logging.info(f"Invocando función OCI con ID: {function_id}")
    
    response = functions_client.invoke_function(function_id=function_id)
    
    logging.info("Función OCI invocada exitosamente")
    return response.data

def check_status_run_dataflow(oci_config, task_id_run_dataflow, ti):
    run_id = ti.xcom_pull(key="id_run_dataflow", task_ids=task_id_run_dataflow)
    data_flow_client = oci.data_flow.DataFlowClient(oci_config)

    run_status = data_flow_client.get_run(run_id=run_id)

    if run_status.status != 200:
        raise AirflowFailException(f"Falla al obtener estado del run {run_id}")

    state = run_status.data.lifecycle_state
    logging.info(f"Estado actual del run {run_id}: {state}")

    if state in ["ACCEPTED", "IN_PROGRESS"]:
        return False
    elif state == "SUCCEEDED":
        return True
    else:
        raise AirflowFailException(f"Run Data Flow fallido ({state}): {run_status.data.lifecycle_details}")
