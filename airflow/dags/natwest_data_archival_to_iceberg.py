from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.exceptions import AirflowException
from botocore.waiter import WaiterModel, create_waiter_with_client
from botocore.exceptions import WaiterError
from datetime import datetime
import json
import pprint
import boto3

# --- Constants ---
DISCOVER_LAMBDA_FUNCTION_NAME = "natwest_data_archive_discover_tables"
ARCHIVE_GLUE_JOB_NAME = "natwest-data-archive-table-to-iceberg"
ASSUME_ROLE_ARN = "arn:aws:iam::934336705194:role/DOC-Airflow-role-dev"
DYNAMODB_TABLE_NAME = "natwest-archival-configs"

# --- Create a dedicated helper function for the waiter ---
def get_glue_job_run_waiter(glue_client):
    """
    Creates and returns a custom Boto3 waiter for Glue job run completion.
    """
    waiter_name = "JobRunCompleted"
    waiter_config = {
        "version": 2,
        "waiters": {
            "JobRunCompleted": {
                "operation": "GetJobRun",
                "delay": 30,
                "maxAttempts": 100,
                "acceptors": [
                    {
                        "matcher": "path",
                        "expected": "SUCCEEDED",
                        "argument": "JobRun.JobRunState",
                        "state": "success"
                    },
                    {
                        "matcher": "path",
                        "expected": "FAILED",
                        "argument": "JobRun.JobRunState",
                        "state": "failure"
                    },
                    {
                        "matcher": "path",
                        "expected": "STOPPED",
                        "argument": "JobRun.JobRunState",
                        "state": "failure"
                    },
                    {
                        "matcher": "path",
                        "expected": "TIMEOUT",
                        "argument": "JobRun.JobRunState",
                        "state": "failure"
                    }
                ]
            }
        }
    }
    waiter_model = WaiterModel(waiter_config)
    return create_waiter_with_client(waiter_name, waiter_model, glue_client)

# --- Cross-account boto3 session ---
def get_boto3_session():
    sts = boto3.client(
        "sts",
        region_name="eu-west-1",
        endpoint_url="https://sts.eu-west-1.amazonaws.com"
        )
    creds = sts.assume_role(
        RoleArn=ASSUME_ROLE_ARN,
        RoleSessionName="airflow-cross-account"
    )["Credentials"]
    return boto3.Session(
        aws_access_key_id=creds["AccessKeyId"],
        aws_secret_access_key=creds["SecretAccessKey"],
        aws_session_token=creds["SessionToken"],
        region_name="eu-west-1"
    )

# --- Task Functions ---

def print_config(**context):
    conf = context["dag_run"].conf or {}
    print("=== RAW CONF dict ===")
    pprint.pprint(conf)
    if conf:
        print("=== JSON SERIALIZED ===")
        print(json.dumps(conf, indent=2))
    else:
        print("No configuration received in dag_run.conf")

def parse_and_validate_config(**context):
    conf = context["dag_run"].conf
    if not conf:
        raise AirflowException("Configuration is empty. Cannot proceed.")
    
    required_keys = ["env", "warehouses", "connections", "sources", "retention_policies"]
    for key in required_keys:
        if key not in conf:
            raise AirflowException(f"Missing key: '{key}'")

    # env_suffix = f"_{conf['env']}"
    # for conn_name in conf["connections"]:
    #     if not conn_name.endswith(env_suffix):
    #         raise AirflowException(
    #             f"Connection '{conn_name}' missing required suffix '{env_suffix}'"
    #         )

    for i, source in enumerate(conf["sources"]):
        wh = source.get("warehouse")
        if not wh or wh not in conf["warehouses"]:
            raise AirflowException(f"Source #{i}: unknown warehouse '{wh}'")
        conn = source.get("connection")
        if not conn or conn not in conf["connections"]:
            raise AirflowException(f"Source #{i}: unknown connection '{conn}'")
        legal_hold = source.get("legal_hold")
        if legal_hold is None:
            raise AirflowException(f"Validation failed for source #{i}: 'legal_hold' is missing.")
        if not isinstance(legal_hold, bool):
            raise AirflowException(f"Validation failed for source #{i}: 'legal_hold' must be Boolean Value (true/false).")


    print("Configuration validated.")
    context["ti"].xcom_push(key="validated_config", value=conf)

def ensure_dynamodb_table_exists():
    """Create DynamoDB table if it doesn't exist"""
    session = get_boto3_session()
    dynamodb = session.resource('dynamodb')

    try:
        table = dynamodb.Table(DYNAMODB_TABLE_NAME)
        table.load()  # This will raise an exception if table doesn't exist
        print(f"Table {DYNAMODB_TABLE_NAME} already exists")
        return table
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        print(f"Creating table {DYNAMODB_TABLE_NAME}...")
        table = dynamodb.create_table(
            TableName=DYNAMODB_TABLE_NAME,
            KeySchema=[
                {
                    'AttributeName': 'source_name',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'source_name',
                    'AttributeType': 'S'
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )

        # Wait for table to be created
        table.wait_until_exists()
        print(f"Table {DYNAMODB_TABLE_NAME} created successfully")
        return table

def store_config_in_dynamodb(**context):
    """
    Store the validated configuration in DynamoDB for future retrieval by purge DAG.
    """
    print("--- Starting DynamoDB config storage ---")
    # Ensure table exists first
    table = ensure_dynamodb_table_exists()

    # Get validated config and DAG run info
    ti = context["ti"]
    conf = ti.xcom_pull(task_ids="parse_and_validate_config", key="validated_config")
    dag_run_id = context["dag_run"].run_id

    # Extract source name from config
    source_name = conf['sources'][0]['name']
    env = conf.get('env', 'dev')

    try:
        # Store config with source_name as partition key
        table.put_item(
            Item={
                'source_name': source_name,                    # Partition Key
                'env': env,
                'dag_run_id': dag_run_id,
                'archival_date': datetime.now().isoformat(),
                'config': conf
            }
        )

        print(f"Config successfully stored in DynamoDB:")
        print(f"Source: {source_name}")
        print(f"Environment: {env}")
        print(f"DAG Run ID: {dag_run_id}")
        print(f"Table: {DYNAMODB_TABLE_NAME}")

    except Exception as e:
        print(f"Failed to store config in DynamoDB: {str(e)}")
        raise AirflowException(f"DynamoDB storage failed: {str(e)}")

def invoke_discover_lambda(**context):
    session = get_boto3_session()
    lambda_client = session.client("lambda")
    ti = context["ti"]
    config = ti.xcom_pull(task_ids="parse_and_validate_config", key="validated_config")
    source_config = config["sources"][0]

    payload = json.dumps(source_config)

    response = lambda_client.invoke(
        FunctionName=DISCOVER_LAMBDA_FUNCTION_NAME,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=payload.encode("utf-8"),
    )

    raw = response["Payload"].read().decode("utf-8")
    print("Lambda response:", raw)
    ti.xcom_push(key="return_value", value=raw)

def prepare_glue_job_args(**context):
    ti = context["ti"]
    payload_str = ti.xcom_pull(task_ids="discover_tables_to_archive", key="return_value")
    if not payload_str:
        print("Empty Lambda payload. Nothing to do.")
        return []

    payload = json.loads(payload_str)
    discovered = payload.get("discovered_tables", [])
    if not discovered:
        print("No discovered tables.")
        return []

    config = ti.xcom_pull(task_ids="parse_and_validate_config", key="validated_config")
    source = config["sources"][0]
    wh_path = config["warehouses"][source["warehouse"]]
    conn_name = source["connection"]
    conn_val = config["connections"][conn_name]
    retention_value = config["retention_policies"][source["retention_policy"]]
    legal_hold = source['legal_hold']

    args = []
    for t in discovered:
        schema = t["schema"]
        table = t["table"]
        condition = t.get("condition")

        s3_path = f"{wh_path.rstrip('/')}/{conn_name}/{schema}/{table}/"

        glue_args = {
        "--source_schema": schema,
        "--source_table": table,
        "--glue_connection_name": conn_val,
        "--target_s3_path": s3_path,
        "--target_glue_db": f"ilm_{conn_val}",
        "--target_glue_table": f"{schema}_{table}",
        "--retention_policy_value": retention_value,
        "--legal_hold": str(legal_hold).lower(),
    }
        if condition:
            glue_args["--condition"] = condition

        args.append(glue_args)

    print(f"Prepared args for {len(args)} tables.")
    return args

@task(task_id="run_archive_glue_job", max_active_tis_per_dag=2)
def run_glue_job(script_args):
    session = get_boto3_session()
    glue = session.client("glue")
    print("Running Glue with args:")
    pprint.pprint(script_args)
    response = glue.start_job_run(
        JobName=ARCHIVE_GLUE_JOB_NAME,
        Arguments=script_args
    )
    job_run_id = response["JobRunId"]
    print(f"Started Glue job. Run ID: {job_run_id}")

    # --- Get and use the custom waiter from the helper function ---
    custom_waiter = get_glue_job_run_waiter(glue)
    
    print("Waiting for Glue job to complete...")
    try:
        custom_waiter.wait(JobName=ARCHIVE_GLUE_JOB_NAME, RunId=job_run_id)
        print("Glue job succeeded.")
    except WaiterError as e:
        print(f"Waiter failed: {e}")
        status_response = glue.get_job_run(JobName=ARCHIVE_GLUE_JOB_NAME, RunId=job_run_id)
        job_status = status_response["JobRun"]["JobRunState"]
        error_message = status_response["JobRun"].get("ErrorMessage", "No error message provided.")
        raise AirflowException(f"Glue job failed with status '{job_status}'. Error: {error_message}")

# --- DAG Definition ---

default_args = {
    "owner": "natwest",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="natwest_data_archival_to_iceberg",
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

    # store_config = PythonOperator(
    #     task_id="store_config_in_dynamodb",
    #     python_callable=store_config_in_dynamodb,
    # )

    discover_tables_to_archive = PythonOperator(
        task_id="discover_tables_to_archive",
        python_callable=invoke_discover_lambda,
    )

    prepare_args = PythonOperator(
        task_id="prepare_glue_job_args",
        python_callable=prepare_glue_job_args,
    )

    run_archive_jobs = run_glue_job.expand(
        script_args=prepare_args.output
    )

    print_raw_config >> validate_config >> discover_tables_to_archive >> prepare_args >> run_archive_jobs
