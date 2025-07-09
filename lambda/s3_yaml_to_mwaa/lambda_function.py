import os, json, boto3, yaml, urllib3

s3   = boto3.client("s3")
mwaa = boto3.client("mwaa")
http = urllib3.PoolManager()

MWAA_ENV = os.environ["MWAA_ENV_NAME"]
DEFAULT_DAG_ID = "archive_tables"

def lambda_handler(event, context):
    # 1. Get the S3 key
    rec    = event["Records"][0]
    bucket = rec["s3"]["bucket"]["name"]
    key    = rec["s3"]["object"]["key"]
    print(f"Received file: s3://{bucket}/{key}")

    # 2. Download & parse the YAML
    body   = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode()
    config = yaml.safe_load(body)

    # 3. Serialize to compact JSON for --conf
    conf_json = json.dumps(config, separators=(",", ":"))

    # 4. Get MWAA CLI token + endpoint
    tok   = mwaa.create_cli_token(Name=MWAA_ENV)
    token = tok["CliToken"]
    url   = f"https://{tok['WebServerHostname']}/aws_mwaa/cli"

    # 5. Build the CLI command as a SINGLE string
    #    This is the first key change. We now create one string and wrap the
    #    --conf payload in single quotes.
    dag_id = config.get("dag_id", DEFAULT_DAG_ID)
    run_id = f"run_{context.aws_request_id}"
    
    cli_command_str = f"dags trigger {dag_id} --run-id {run_id} --conf '{conf_json}'"
    print("CLI command string:", cli_command_str)

    # 6. Fire it off using a plain text body
    #    This is the second key change, matching the successful local script.
    resp = http.request(
      "POST", url,
      headers={
        "Authorization": f"Bearer {token}",
        "Content-Type":  "text/plain",  # Changed from application/json
      },
      body=cli_command_str.encode()     # The body is now just the command string
    )
    
    if resp.status != 200:
        # Decode the response to see the actual error from Airflow
        response_data = json.loads(resp.data.decode('utf-8'))
        stderr = base64.b64decode(response_data.get('stderr', '')).decode('utf-8')
        raise RuntimeError(f"MWAA invoke error {resp.status}: {stderr}")

    print("Success:", resp.data.decode())
    return {"status": "Triggered"}
