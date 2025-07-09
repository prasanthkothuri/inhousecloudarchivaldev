# airflow/dags/archive_tables.py

from __future__ import annotations

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from datetime import datetime
import pprint
import json

# --- Constants ---
DISCOVER_LAMBDA_FUNCTION_NAME = "archive-discover-tables"
ARCHIVE_GLUE_JOB_NAME = "archive-table-to-iceberg"

# --- Task Functions ---

def print_config(**context):
    """
    Print the configuration dict passed in via dag_run.conf.
    """
    conf = context["dag_run"].conf or {}
    print("=== RAW CONF dict ===")
    pprint.pprint(conf)

    if conf:
        print("=== JSON SERIALIZED ===")
        print(json.dumps(conf, indent=2))
    else:
        print("No configuration received in dag_run.conf")

def parse_and_validate_config(**context):
    """
    Validates the config schema and naming conventions.
    Pushes the validated config to XComs for downstream tasks.
    """
    print("--- Starting configuration validation ---")
    conf = context["dag_run"].conf
    if not conf:
        raise AirflowException("Configuration is empty. Cannot proceed.")

    required_keys = ["env", "warehouses", "connections", "sources", "retention_policies"]
    for key in required_keys:
        if key not in conf:
            raise AirflowException(f"Validation failed: Required top-level key '{key}' is missing.")
    
    env_suffix = f"_{conf['env']}"
    
    for conn_name in conf["connections"]:
        if not conn_name.endswith(env_suffix):
            raise AirflowException(
                f"Validation failed: Connection '{conn_name}' does not have the required '{env_suffix}' suffix."
            )

    for i, source in enumerate(conf["sources"]):
        warehouse_ref = source.get("warehouse")
        if not warehouse_ref or warehouse_ref not in conf["warehouses"]:
            raise AirflowException(f"Validation failed for source #{i}: warehouse '{warehouse_ref}' not found in warehouses block.")

        connection_ref = source.get("connection")
        if not connection_ref or connection_ref not in conf["connections"]:
            raise AirflowException(f"Validation failed for source #{i}: connection '{connection_ref}' not found in connections block.")

    print("Configuration is valid.")
    context["ti"].xcom_push(key="validated_config", value=conf)

def prepare_glue_job_args(**context):
    """
    Takes the list of discovered tables and prepares a list of dictionaries
    formatted for the .expand_kwargs() method. This function now returns
    the list directly, which is the standard pattern for dynamic tasks.
    """
    print("--- Preparing arguments for Glue jobs ---")
    ti = context["ti"]
    
    # Pull the JSON string payload from the Lambda task's XCom
    lambda_payload_str = ti.xcom_pull(task_ids='discover_tables_to_archive', key='return_value')
    
    if not lambda_payload_str:
        print("Received empty payload from Lambda. Nothing to prepare.")
        return []

    # --- CORRECTED CODE: Parse the JSON string into a Python dictionary ---
    print(f"Received payload string: {lambda_payload_str}")
    lambda_payload = json.loads(lambda_payload_str)
    
    discovered_tables = lambda_payload.get("discovered_tables", [])
    
    if not discovered_tables:
        print("No new tables to archive. Nothing to prepare.")
        return []

    print(f"Preparing arguments for {len(discovered_tables)} tables.")
    
    config = ti.xcom_pull(task_ids='parse_and_validate_config', key='validated_config')
    source_config = config['sources'][0]
    
    glue_job_kwargs_list = []
    
    for table_info in discovered_tables:
        schema = table_info["schema"]
        table = table_info["table"]
        
        warehouse_name = source_config['warehouse']
        warehouse_path = config['warehouses'][warehouse_name]
        connection_name = source_config['connection']
        retention_policy_name = source_config['retention_policy']
        retention_policy_value = config['retention_policies'][retention_policy_name]
        
        target_glue_db = f"archive_{connection_name}"
        target_glue_table = f"{schema}_{table}"
        target_s3_path = f"{warehouse_path.rstrip('/')}/{connection_name}/{schema}/{table}/"
        
        script_args = {
            "--source_schema": schema,
            "--source_table": table,
            "--glue_connection_name": config['connections'][connection_name],
            "--target_s3_path": target_s3_path,
            "--target_glue_db": target_glue_db,
            "--target_glue_table": target_glue_table,
            "--retention_policy_value": retention_policy_value,
        }
        
        glue_job_kwargs_list.append({"script_args": script_args})
        
    print("Successfully prepared all Glue job arguments.")
    pprint.pprint(glue_job_kwargs_list)
    
    return glue_job_kwargs_list


# --- DAG Definition ---

default_args = {
    "owner": "natwest-ilm",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="archive_tables",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["archival", "triggered"],
) as dag:

    print_raw_config = PythonOperator(
        task_id="print_config",
        python_callable=print_config,
    )

    validate_config = PythonOperator(
        task_id="parse_and_validate_config",
        python_callable=parse_and_validate_config,
    )

    discover_tables_to_archive = LambdaInvokeFunctionOperator(
        task_id="discover_tables_to_archive",
        function_name=DISCOVER_LAMBDA_FUNCTION_NAME,
        payload="{{ task_instance.xcom_pull(task_ids='parse_and_validate_config', key='validated_config')['sources'][0] | tojson }}"
    )

    prepare_args = PythonOperator(
        task_id="prepare_glue_job_args",
        python_callable=prepare_glue_job_args,
    )

    run_archive_job = GlueJobOperator.partial(
        task_id="run_archive_glue_job",
        job_name=ARCHIVE_GLUE_JOB_NAME,
        wait_for_completion=True,
    ).expand_kwargs(
        prepare_args.output
    )

    # Set the task dependency chain
    print_raw_config >> validate_config >> discover_tables_to_archive >> prepare_args >> run_archive_job
