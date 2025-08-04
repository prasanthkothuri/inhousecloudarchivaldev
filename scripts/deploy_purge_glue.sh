#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# Configuration
###############################################################################
AWS_REGION="eu-west-1"

# Buckets
S3_ASSET_BUCKET="natwest-data-archive-assets"
S3_WAREHOUSE_BUCKET="natwest-data-archive-vault"   # Iceberg warehouse bucket

# Glue assets for PURGE job
GLUE_JOB_NAME="natwest-data-purge-from-iceberg"
GLUE_ROLE_NAME="natwest-data-archive-glue-role"  # Use existing role
GLUE_CONNECTION_NAME="onprem_orcl_conn"
GLUE_SCRIPT_LOCAL_PATH="glue_jobs/purge_table.py"
GLUE_SCRIPT_S3_KEY="glue_jobs/purge_table.py"
GLUE_VERSION="5.0"
MAX_CONCURRENCY=10

###############################################################################
# Helpers
###############################################################################
AWS_CMD=(aws --region "$AWS_REGION")
TMP_DIR="$(mktemp -d)"
cleanup() { rm -rf "$TMP_DIR"; }
trap cleanup EXIT

ACCOUNT_ID=$("${AWS_CMD[@]}" sts get-caller-identity --query Account --output text)
GLUE_ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${GLUE_ROLE_NAME}"
S3_SCRIPT_PATH="s3://${S3_ASSET_BUCKET}/${GLUE_SCRIPT_S3_KEY}"

###############################################################################
# Skip IAM role creation - use existing archival role
###############################################################################
echo "Using existing IAM role ${GLUE_ROLE_NAME} ..."

# Verify the role exists
if ! "${AWS_CMD[@]}" iam get-role --role-name "$GLUE_ROLE_NAME" >/dev/null 2>&1; then
  echo "ERROR: Role ${GLUE_ROLE_NAME} does not exist. Please run deploy_glue.sh for archival first."
  exit 1
fi

echo "Existing IAM role verified."

###############################################################################
# Upload Glue script
###############################################################################
echo "Uploading ${GLUE_SCRIPT_LOCAL_PATH} -> ${S3_SCRIPT_PATH}"
"${AWS_CMD[@]}" s3 cp "$GLUE_SCRIPT_LOCAL_PATH" "$S3_SCRIPT_PATH"

###############################################################################
# Glue job: create or update
###############################################################################
echo "Ensuring Glue job ${GLUE_JOB_NAME} exists/updated ..."

SPARK_CONF="spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \
--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
--conf spark.sql.catalog.glue_catalog.warehouse=s3://${S3_WAREHOUSE_BUCKET}/iceberg/ \
--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
--conf spark.sql.defaultCatalog=glue_catalog"

JOB_COMMAND=$(cat <<EOF
{
  "Name": "glueetl",
  "ScriptLocation": "$S3_SCRIPT_PATH",
  "PythonVersion": "3"
}
EOF
)

DEFAULT_ARGUMENTS=$(cat <<EOF
{
  "--job-language": "python",
  "--disable-proxy-v2": "true",
  "--enable-glue-datacatalog": "true",
  "--enable-metrics": "true",
  "--enable-spark-ui": "true",
  "--spark-event-logs-path": "s3://${S3_ASSET_BUCKET}/spark-history/",
  "--TempDir": "s3://${S3_ASSET_BUCKET}/temp/",
  "--spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
  "--packages": "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.9.1",
  "--conf": "$SPARK_CONF"
}
EOF
)

EXECUTION_PROPERTY='{"MaxConcurrentRuns": 10}'
CONNECTIONS_ARG="{\"Connections\": [\"$GLUE_CONNECTION_NAME\"]}"

if ! "${AWS_CMD[@]}" glue get-job --job-name "$GLUE_JOB_NAME" >/dev/null 2>&1; then
  "${AWS_CMD[@]}" glue create-job \
    --name "$GLUE_JOB_NAME" \
    --role "$GLUE_ROLE_ARN" \
    --command "$JOB_COMMAND" \
    --default-arguments "$DEFAULT_ARGUMENTS" \
    --execution-property "$EXECUTION_PROPERTY" \
    --glue-version "$GLUE_VERSION" \
    --worker-type "G.1X" \
    --number-of-workers 2 \
    --connections "$CONNECTIONS_ARG" >/dev/null
  echo "Glue job created."
else
  "${AWS_CMD[@]}" glue update-job \
    --job-name "$GLUE_JOB_NAME" \
    --job-update "{
      \"Role\": \"$GLUE_ROLE_ARN\",
      \"Command\": $JOB_COMMAND,
      \"DefaultArguments\": $DEFAULT_ARGUMENTS,
      \"ExecutionProperty\": $EXECUTION_PROPERTY,
      \"GlueVersion\": \"$GLUE_VERSION\",
      \"WorkerType\": \"G.1X\",
      \"NumberOfWorkers\": 2,
      \"Connections\": $CONNECTIONS_ARG
    }" >/dev/null
  echo "Glue job updated."
fi

echo "Glue purge job deployment complete."

###############################################################################
# Summary
###############################################################################
echo ""
echo "Deployment Summary:"
echo "Job Name: ${GLUE_JOB_NAME}"
echo "Role: ${GLUE_ROLE_ARN} (shared with archival)"
echo "Script: ${S3_SCRIPT_PATH}"
echo "Connection: ${GLUE_CONNECTION_NAME}"
echo ""
echo "Usage:"
echo "Discovery Mode: --mode discovery --source_name <name> --connection <conn> --iceberg_database <db>"
echo "Delete Mode:    --mode delete --source_name <name> --connection <conn> --iceberg_database <db> --max_parallel_tables <num>"
echo ""
