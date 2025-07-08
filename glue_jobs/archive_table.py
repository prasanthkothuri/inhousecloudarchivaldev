#!/usr/bin/env python3
"""
Archive a PostgreSQL table to Iceberg in S3 + AWS Glue Catalog.

Improvements
------------
* Build Spark **after** parsing arguments so we can wire Iceberg-catalog
  settings dynamically.
* Register `glue_catalog` (Iceberg on AWS Glue) and point its warehouse
  to the bucket that holds your Iceberg data.
* Use the fully-qualified name `glue_catalog.<db>.<table>` when calling
  `saveAsTable`.
* Verifies/creates the Glue database before the write.
"""

import sys, re, json
from datetime import datetime, timedelta
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession, functions as F
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def create_glue_database_if_not_exists(db_name: str, region: str = "eu-west-2"):
    glue = boto3.client("glue", region_name=region)
    try:
        glue.get_database(Name=db_name)
        print(f"Glue database '{db_name}' already exists.")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityNotFoundException":
            print(f"Glue database '{db_name}' not found. Creating it …")
            glue.create_database(DatabaseInput={"Name": db_name})
        else:
            raise


def _parse_retention_duration(text: str) -> timedelta:
    m = re.match(r"^(\d+)([dmy])$", text, re.I)
    if not m:
        raise ValueError(f"Unsupported duration literal: {text!r}")
    num, unit = int(m[1]), m[2].lower()
    return timedelta(days=num * {"d": 1, "m": 30, "y": 365}[unit])


# --------------------------------------------------------------------------- #
# Arg parsing  — do this *first*
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
    ],
)

print(f"Starting archival for table: {args['source_schema']}.{args['source_table']}")

# --------------------------------------------------------------------------- #
# Build an Iceberg-aware Spark session
# --------------------------------------------------------------------------- #

parsed = urlparse(args["target_s3_path"])
# s3://bucket/… → s3://bucket/iceberg/
warehouse_root = f"s3://{parsed.netloc}/iceberg/"

spark = (
    SparkSession.builder
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue_catalog.warehouse", warehouse_root)
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.defaultCatalog", "glue_catalog")
    .getOrCreate()
)

glueContext = GlueContext(spark.sparkContext)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------------------------------- #
# Retrieve JDBC connection info from Glue + Secrets Manager
# --------------------------------------------------------------------------- #

print("Fetching connection details …")
glue = boto3.client("glue")
conn = glue.get_connection(Name=args["glue_connection_name"])["Connection"]
jdbc_url   = conn["ConnectionProperties"]["JDBC_CONNECTION_URL"]
secret_id  = conn["ConnectionProperties"]["SECRET_ID"]

secrets = boto3.client("secretsmanager")
creds   = json.loads(secrets.get_secret_value(SecretId=secret_id)["SecretString"])
db_user, db_password = creds["username"], creds["password"]

print(f"JDBC URL resolved: {jdbc_url}")

# --------------------------------------------------------------------------- #
# Read source table
# --------------------------------------------------------------------------- #

db_table = f'"{args["source_schema"]}"."{args["source_table"]}"'
source_df = (
    spark.read.format("jdbc")
    .option("url", jdbc_url)
    .option("dbtable", db_table)
    .option("user", db_user)
    .option("password", db_password)
    .option("driver", "org.postgresql.Driver")
    .load()
)

print(f"Read {source_df.count()} rows from {db_table}")

# --------------------------------------------------------------------------- #
# Add retention columns
# --------------------------------------------------------------------------- #

now       = datetime.utcnow()
expires   = now + _parse_retention_duration(args["retention_policy_value"])
data_df   = (source_df
             .withColumn("retention_added_at",  F.lit(now))
             .withColumn("retention_expires_at", F.lit(expires)))

# --------------------------------------------------------------------------- #
# Write to Iceberg
# --------------------------------------------------------------------------- #

create_glue_database_if_not_exists(args["target_glue_db"])

iceberg_name = f'glue_catalog.{args["target_glue_db"]}.{args["target_glue_table"]}'
print(f"Writing to Iceberg table {iceberg_name}")

(data_df.write
        .format("iceberg")
        .mode("overwrite")
        .option("path", args["target_s3_path"])  # location for this table
        .saveAsTable(iceberg_name))

print("Archive complete.")
job.commit()
