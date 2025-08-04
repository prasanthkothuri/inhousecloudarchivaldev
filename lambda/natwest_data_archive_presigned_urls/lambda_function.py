import sys
import re
import json
import uuid
import mimetypes
from datetime import datetime, timedelta
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import functions as F
from pyspark.sql.types import BinaryType, StringType, StructType, StructField, TimestampType, BooleanType, Row
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def create_glue_database_if_not_exists(db_name: str, region: str = "eu-west-1"):
    """Creates a Glue database if it doesn't already exist."""
    glue = boto3.client("glue", region_name=region)
    try:
        glue.get_database(Name=db_name)
        print(f"Glue database '{db_name}' already exists.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            print(f"Glue database '{db_name}' not found. Creating it...")
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
    """Parses a simple duration string like '30d', '6m', or '1y'."""
    m = re.match(r"^(\d+)([dmy])$", text, re.I)
    if not m:
        raise ValueError(f"Unsupported duration literal: {text!r}")
    num, unit = int(m[1]), m[2].lower()
    return timedelta(days=num * {"d": 1, "m": 30, "y": 365}[unit])

def upload_blob_to_s3(binary_data, path):
    """Uploads a binary object to a given S3 path."""
    s3 = boto3.client("s3")
    parsed = urlparse(path)
    bucket, key = parsed.netloc, parsed.path.lstrip("/")
    s3.put_object(Bucket=bucket, Key=key, Body=binary_data)
    print(f"Uploaded blob to s3://{bucket}/{key}")

def get_extension_from_content(data: bytes) -> str:
    """
    Inspects the first few bytes of data to determine the file extension
    by looking for "magic numbers" or common file signatures.
    Returns a file extension (e.g., '.jpg') or '.bin' as a fallback.
    """
    if not data:
        return '.bin'
    if data.startswith(b'\xff\xd8\xff'):
        return '.jpg'
    if data.lstrip().startswith(b'<?xml') or data.lstrip().startswith(b'<'):
        return '.xml'
    if data.startswith(b'\x89PNG\r\n\x1a\n'):
        return '.png'
    if data.startswith(b'%PDF-'):
        return '.pdf'
    if data.startswith(b'GIF87a') or data.startswith(b'GIF89a'):
        return '.gif'
    return '.bin'

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
condition = sys.argv[sys.argv.index("--condition") + 1] if "--condition" in sys.argv else None
if condition:
    print(f"Applying condition: {condition}")

# --------------------------------------------------------------------------- #
# Initialize Spark/Glue context
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
# Read from source
# --------------------------------------------------------------------------- #
source_schema = args["source_schema"]
source_table = args["source_table"]

connection_options = {
    "useConnectionProperties": "true",
    "dbtable": f"{source_schema}.{source_table}",
    "connectionName": args["glue_connection_name"],
    "oracle.jdbc.defaultLobPrefetchSize": "104857600",
}

df = glueContext.create_dynamic_frame.from_options(
    connection_type="oracle",
    connection_options=connection_options
).toDF()

if condition:
    df = df.filter(condition)

row_count = df.count()
print(f"Read {row_count} rows from {source_schema}.{source_table}")
if row_count == 0:
    print("No rows to process. Exiting.")
    job.commit()
    sys.exit(0)

# --------------------------------------------------------------------------- #
# Extract and Upload Blob Columns
# --------------------------------------------------------------------------- #
blob_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, BinaryType)]
print(f"Binary columns detected: {blob_columns}")

# *** FIX: Define schema for the intermediate DataFrame after blob processing ***
# This schema only includes the original non-blob columns and the new blob path columns.
schema_fields = [
    StructField(field.name, field.dataType) for field in df.schema.fields
    if not isinstance(field.dataType, BinaryType)
]
schema_fields.extend([StructField(col, StringType(), True) for col in blob_columns])
schema = StructType(schema_fields)

def handle_row(row):
    """Processes a single row, uploading blobs and returning a transformed Row object."""
    row_dict = row.asDict()
    # Start with the non-blob columns
    result_dict = {k: v for k, v in row_dict.items() if k not in blob_columns}

    for col in blob_columns:
        data = row_dict.get(col)

        if data and len(data) > 0:
            blob_uuid = str(uuid.uuid4())
            ext = get_extension_from_content(data)

            # Construct S3 path using job arguments
            s3_path = f"s3://{parsed.netloc}/blobs/{args['target_glue_db'].lower()}/{source_schema.lower()}/{source_table.lower()}/{col.lower()}/{blob_uuid}{ext}"
            upload_blob_to_s3(data, s3_path)
            result_dict[col] = s3_path
        else:
            result_dict[col] = None

    # Ensure all fields from the schema are present in the dictionary before creating the Row
    # This prevents potential errors if a column is missing from the result_dict
    ordered_values = [result_dict.get(field.name) for field in schema.fields]
    return Row(*ordered_values)

if blob_columns:
    rdd = df.rdd.map(handle_row)
    df = spark.createDataFrame(rdd, schema=schema)

# --------------------------------------------------------------------------- #
# Add retention metadata columns
# --------------------------------------------------------------------------- #
# *** FIX: Add the metadata columns *after* the complex RDD transformation ***
# This is a cleaner and more reliable way to add columns with fixed values.
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

# Using append to avoid overwriting on subsequent runs
df.writeTo(iceberg_table).using("iceberg").createOrReplace()

print("Archive complete.")
job.commit()
