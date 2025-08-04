import sys
import re
import json
from datetime import datetime, timedelta
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import functions as F
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def create_glue_database_if_not_exists(db_name: str, region: str = "eu-west-1"):
    glue = boto3.client("glue", region_name=region)
    try:
        glue.get_database(Name=db_name)
        print(f"Glue database '{db_name}' already exists.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            print(f"Glue database '{db_name}' not found. Creating it …")
            try:
                glue.create_database(DatabaseInput={"Name": db_name})
                print(f"Glue database '{db_name}' created.")
            except ClientError as create_error:
                if create_error.response["Error"]["Code"] == "AlreadyExistsException":
                    print(f"Glue database '{db_name}' was created by another job.")
                else:
                    raise
        else:
            raise

def _parse_retention_duration(text: str) -> timedelta:
    m = re.match(r"^(\d+)([dmy])$", text, re.I)
    if not m:
        raise ValueError(f"Unsupported duration literal: {text!r}")
    num, unit = int(m[1]), m[2].lower()
    return timedelta(days=num * {"d": 1, "m": 30, "y": 365}[unit])

# --------------------------------------------------------------------------- #
# Arg parsing
# --------------------------------------------------------------------------- #

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "source_schema",
        "source_table",
        "glue_connection_name",
        "target_s3_path",
        "target_glue_db",
        "target_glue_table",
        "retention_policy_value",
        "legal_hold",
    ],
)

print(f"Starting archival for table: {args['source_schema']}.{args['source_table']}")
if args.get("condition"):
    print(f"Applying condition: {args['condition']}")

# --------------------------------------------------------------------------- #
# Initialize Spark/Glue context with Iceberg support
# --------------------------------------------------------------------------- #
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

parsed = urlparse(args["target_s3_path"])
warehouse_root = f"s3://{parsed.netloc}/iceberg/"

spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", warehouse_root)
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.defaultCatalog", "glue_catalog")

# --------------------------------------------------------------------------- #
# Prepare query with optional WHERE condition
# --------------------------------------------------------------------------- #
source_schema = args["source_schema"]
source_table = args["source_table"]
condition = args.get("condition")

connection_options = {
    "useConnectionProperties": "true",
    "dbtable": f"{source_schema}.{source_table}",
    "connectionName": args["glue_connection_name"],
}

print(f"Connection options:\n{json.dumps(connection_options, indent=2)}")

# --------------------------------------------------------------------------- #
# Read from source
# --------------------------------------------------------------------------- #
df = glueContext.create_dynamic_frame.from_options(
    connection_type="oracle",
    connection_options=connection_options
).toDF()

if condition:
    df = df.filter(condition)

row_count = df.count()
print(f"Read {row_count} rows from {source_schema}.{source_table}")

# --------------------------------------------------------------------------- #
# Add retention metadata columns
# --------------------------------------------------------------------------- #
legal_hold = args["legal_hold"].lower() == "true"
now = datetime.utcnow()
expires = now + _parse_retention_duration(args["retention_policy_value"])

df = (
    df.withColumn("archived_at", F.lit(now))
      .withColumn("retention_expires_at", F.lit(expires))
      .withColumn("legal_hold", F.lit(legal_hold))
)

# --------------------------------------------------------------------------- #
# Write to Iceberg table
# --------------------------------------------------------------------------- #
create_glue_database_if_not_exists(args["target_glue_db"])

iceberg_table = f"glue_catalog.{args['target_glue_db']}.{args['target_glue_table'].lower()}"
print(f"Writing to Iceberg table: {iceberg_table}")

df.writeTo(iceberg_table).using("iceberg").createOrReplace()

print("Archive complete.")
job.commit()
