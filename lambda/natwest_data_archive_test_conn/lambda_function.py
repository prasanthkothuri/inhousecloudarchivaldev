import json
import boto3
import oracledb
import re
import os
from botocore.exceptions import ClientError

# AWS clients
glue = boto3.client('glue')
secretsmanager = boto3.client('secretsmanager')

# Constants
GLUE_CONNECTION_NAME = 'onprem_orcl_conn'
CA_CERT_PATH = '/var/task/certs/ewallet.pem'  # Update if placed elsewhere in your Lambda package

def get_glue_connection(name):
    response = glue.get_connection(Name=name)
    props = response['Connection']['ConnectionProperties']
    secret_arn = props['SECRET_ID']
    jdbc_url = props['JDBC_CONNECTION_URL']
    return jdbc_url, secret_arn

def parse_jdbc_url(jdbc_url):
    # Extract host, port, and service name from the JDBC URL
    pattern = r'@//([^:/]+):(\d+)/(.*)'
    match = re.search(pattern, jdbc_url)
    if match:
        host, port, service_name = match.groups()
        return host, int(port), service_name
    else:
        raise ValueError("Invalid JDBC URL format")

def get_credentials(secret_arn):
    secret_response = secretsmanager.get_secret_value(SecretId=secret_arn)
    secret = json.loads(secret_response['SecretString'])
    return secret['username'], secret['password']

def lambda_handler(event, context):
    try:
        # Get JDBC URL and secret
        jdbc_url, secret_arn = get_glue_connection(GLUE_CONNECTION_NAME)
        username, password = get_credentials(secret_arn)
        host, port, service_name = parse_jdbc_url(jdbc_url)

        # Construct DSN for TCPS connection
        dsn = (
            f"(DESCRIPTION="
            f"(ADDRESS=(PROTOCOL=tcps)(HOST={host})(PORT={port}))"
            f"(CONNECT_DATA=(SERVICE_NAME={service_name})))"
        )
        
        connection = oracledb.connect(
            user=username,
            password=password,
            dsn=f'{host}:{port}/{service_name}',
            wallet_location="certs/", 
            protocol="tcps",
            ssl_server_dn_match="CN=DataOrchestrationAWS_NFT, OU=Devices, OU=Proving G1 PKI Service, O=The Royal Bank of Scotland Group, C=gb"
            )

        cursor = connection.cursor()
        cursor.execute("SELECT table_name FROM all_tables WHERE OWNER = 'EDI_SUP_OWNER' and ROWNUM < 10")
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        connection.close()

        return {
            'statusCode': 200,
            'body': json.dumps({'tables': tables})
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
