import logging
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

def launch_glue_job(config, task_id='launch_glue_job'):
    logging.info(f"Lanzando Glue Job {config['job_name']} en AWS")
    return GlueJobOperator(
        task_id=task_id,
        job_name=config['job_name'],
        script_location=config['script_location'],
        iam_role_name=config['iam_role_name'],
        region_name=config['region'],
        script_args=config.get('script_args', {}),
        wait_for_completion=False
    )
