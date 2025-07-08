import boto3
import json
import os
import pg8000.dbapi # Pure-Python PostgreSQL database driver
import re
from urllib.parse import urlparse

# Initialize clients outside the handler for performance
glue_client = boto3.client("glue")
secrets_manager_client = boto3.client("secretsmanager")

def get_db_connection(glue_connection_name):
    """
    Fetches connection details from AWS Glue and establishes a database connection.
    """
    print(f"Fetching details for Glue Connection: {glue_connection_name}")
    
    # Get the connection properties from AWS Glue
    response = glue_client.get_connection(Name=glue_connection_name)
    properties = response['Connection']['ConnectionProperties']
    
    # The JDBC URL has the host, port, and dbname
    # Example: "jdbc:postgresql://host:port/dbname"
    jdbc_url = properties['JDBC_CONNECTION_URL']
    parsed_url = urlparse(jdbc_url.replace("jdbc:", ""))
    
    # The secret ID is stored in the Glue Connection properties
    secret_id = properties['SECRET_ID']
    
    print(f"Fetching credentials from Secrets Manager secret: {secret_id}")
    secret = secrets_manager_client.get_secret_value(SecretId=secret_id)
    credentials = json.loads(secret['SecretString'])

    print(f"Connecting to database '{parsed_url.path.strip('/')}' on host '{parsed_url.hostname}'...")
    
    # Establish and return the database connection using pg8000
    conn = pg8000.dbapi.connect(
        host=parsed_url.hostname,
        port=parsed_url.port,
        database=parsed_url.path.strip('/'),
        user=credentials['username'],
        password=credentials['password']
    )
    return conn

def lambda_handler(event, context):
    """
    Connects to a source database, discovers tables based on include/exclude
    rules, and returns the filtered list.
    """
    print("Received event:", json.dumps(event, indent=2))
    
    # --- 1. Parse Input from Airflow ---
    source_config = event
    connection_name = source_config.get("connection")
    include_rules = source_config.get("include", {})
    exclude_rules = source_config.get("exclude", {})
    
    if not connection_name or not include_rules:
        raise ValueError("Event must include 'connection' and 'include' keys.")

    # --- 2. Discover All Tables from Schemas ---
    all_tables = []
    conn = None
    try:
        conn = get_db_connection(connection_name)
        cursor = conn.cursor()
        
        schemas_list = include_rules.get("schemas", [])
        if not schemas_list:
            raise ValueError("Include rules must specify at least one schema.")

        # --- CORRECTED SQL PARAMETERIZATION ---
        # 1. Create a placeholder string like '%s, %s, %s'
        placeholders = ', '.join(['%s'] * len(schemas_list))
        
        # 2. Format the SQL query with the dynamic placeholders
        sql_query = f"SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema IN ({placeholders});"
        
        # 3. Execute with the list of schemas directly
        print(f"Executing query: {sql_query} with params: {schemas_list}")
        cursor.execute(sql_query, schemas_list)
        
        for row in cursor.fetchall():
            all_tables.append({"schema": row[0], "table": row[1]})
            
        cursor.close()
            
    except Exception as e:
        print(f"ERROR: Failed to connect or query database. {e}")
        raise
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

    print(f"Found {len(all_tables)} total tables in specified schemas.")

    # --- 3. Filter Tables Based on Rules ---
    discovered_tables = []
    include_patterns = [re.compile(p) for p in include_rules.get("tables", [])]
    exclude_patterns = [re.compile(p) for p in exclude_rules.get("tables", [])]

    for item in all_tables:
        table_name = item["table"]
        
        is_included = not include_patterns or any(p.match(table_name) for p in include_patterns)
        is_excluded = any(p.match(table_name) for p in exclude_patterns)
        
        if is_included and not is_excluded:
            discovered_tables.append(item)

    print(f"Filtered down to {len(discovered_tables)} tables to be archived.")
    
    # --- 4. Return the Final List ---
    return {
        "source_name": source_config.get("name"),
        "discovered_tables": discovered_tables
    }
