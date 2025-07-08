#!/usr/bin/env bash
set -euo pipefail

###############################################################################
# Configuration
###############################################################################
AWS_REGION="eu-west-2"

# Buckets
S3_ASSET_BUCKET="{s3_asset_bucket}"
S3_WAREHOUSE_BUCKET="{s3_warehouse_bucket}"   # Iceberg warehouse bucket

# Glue assets
GLUE_JOB_NAME="archive-table-to-iceberg"
GLUE_ROLE_NAME="ilm-glue-archival-role"
GLUE_CONNECTION_NAME="nucleus_dev"
GLUE_SCRIPT_LOCAL_PATH="glue_jobs/archive_table.py"
GLUE_SCRIPT_S3_KEY="glue_jobs/archive_table.py"
REQUIRED_GLUE_VERSION="4.0"

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
# IAM role: create / update with least-privilege inline policy
###############################################################################
echo "Ensuring IAM role ${GLUE_ROLE_NAME} exists ..."

# 1. Trust policy
cat >"$TMP_DIR/trust.json" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "glue.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
EOF

if ! "${AWS_CMD[@]}" iam get-role --role-name "$GLUE_ROLE_NAME" >/dev/null 2>&1; then
  "${AWS_CMD[@]}" iam create-role \
    --role-name "$GLUE_ROLE_NAME" \
    --assume-role-policy-document "file://$TMP_DIR/trust.json" >/dev/null
else
  "${AWS_CMD[@]}" iam update-assume-role-policy \
    --role-name "$GLUE_ROLE_NAME" \
    --policy-document "file://$TMP_DIR/trust.json" >/dev/null
fi

# 2. Attach managed Glue service-role policy
"${AWS_CMD[@]}" iam attach-role-policy \
  --role-name "$GLUE_ROLE_NAME" \
  --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole" >/dev/null

# 3. Inline least-privilege policy
cat >"$TMP_DIR/inline.json" <<EOF
{
  "Version": "2012-10-17",
  "Statement": [

    {
      "Sid": "ReadGlueScript",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::${S3_ASSET_BUCKET}/glue_jobs/*"
    },

    {
      "Sid": "WriteSparkLogsTemp",
      "Effect": "Allow",
      "Action": "s3:PutObject",
      "Resource": [
        "arn:aws:s3:::${S3_ASSET_BUCKET}/spark-history/*",
        "arn:aws:s3:::${S3_ASSET_BUCKET}/temp/*"
      ]
    },

    {
      "Sid": "IcebergDataRW",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::${S3_WAREHOUSE_BUCKET}/*"
    },
    {
      "Sid": "IcebergBucketList",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::${S3_WAREHOUSE_BUCKET}"
    },

    {
      "Sid": "GlueConnection",
      "Effect": "Allow",
      "Action": "glue:GetConnection",
      "Resource": "arn:aws:glue:${AWS_REGION}:${ACCOUNT_ID}:connection/${GLUE_CONNECTION_NAME}"
    },
    {
      "Sid": "ReadConnectionSecret",
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "*"
    },

    {
      "Sid": "GlueDatabaseOps",
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase",
        "glue:CreateDatabase"
      ],
      "Resource": [
        "arn:aws:glue:${AWS_REGION}:${ACCOUNT_ID}:catalog",
        "arn:aws:glue:${AWS_REGION}:${ACCOUNT_ID}:database/archive_*"
      ]
    },

    {
      "Sid": "GlueTableOps",
      "Effect": "Allow",
      "Action": [
        "glue:GetTable",
        "glue:GetTables",
        "glue:CreateTable",
        "glue:UpdateTable"
      ],
      "Resource": [
        "arn:aws:glue:${AWS_REGION}:${ACCOUNT_ID}:table/archive_*/*",
        "arn:aws:glue:${AWS_REGION}:${ACCOUNT_ID}:database/archive_*",
        "arn:aws:glue:${AWS_REGION}:${ACCOUNT_ID}:catalog"
      ]
    },

    {
      "Sid": "VpcEniLifecycle",
      "Effect": "Allow",
      "Action": [
        "ec2:CreateNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DeleteNetworkInterface",
        "ec2:AssignPrivateIpAddresses",
        "ec2:UnassignPrivateIpAddresses"
      ],
      "Resource": "*"
    }
  ]
}
EOF

"${AWS_CMD[@]}" iam put-role-policy \
  --role-name "$GLUE_ROLE_NAME" \
  --policy-name "GlueArchivalInline" \
  --policy-document "file://$TMP_DIR/inline.json" >/dev/null

echo "IAM role configured."

###############################################################################
# Upload Glue script
###############################################################################
echo "Uploading ${GLUE_SCRIPT_LOCAL_PATH} -> ${S3_SCRIPT_PATH}"
"${AWS_CMD[@]}" s3 cp "$GLUE_SCRIPT_LOCAL_PATH" "$S3_SCRIPT_PATH"

###############################################################################
# Glue job: create or update
###############################################################################
echo "Ensuring Glue job ${GLUE_JOB_NAME} exists/updated ..."

SPARK_CONF="spark.jars.packages=org.postgresql:postgresql:42.7.3,\
spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog,\
spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog,\
spark.sql.catalog.glue_catalog.warehouse=s3://${S3_WAREHOUSE_BUCKET}/iceberg/,\
spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO,\
spark.sql.defaultCatalog=glue_catalog"

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
  "--datalake-formats": "iceberg",
  "--enable-glue-datacatalog": "true",
  "--enable-metrics": "true",
  "--enable-spark-ui": "true",
  "--spark-event-logs-path": "s3://${S3_ASSET_BUCKET}/spark-history/",
  "--job-language": "python",
  "--TempDir": "s3://${S3_ASSET_BUCKET}/temp/",
  "--conf": "$SPARK_CONF",
  "--user-jars-first": "true"
}
EOF
)

EXECUTION_PROPERTY='{"MaxConcurrentRuns": 50}'
CONNECTIONS_ARG="{\"Connections\": [\"$GLUE_CONNECTION_NAME\"]}"

if ! "${AWS_CMD[@]}" glue get-job --job-name "$GLUE_JOB_NAME" >/dev/null 2>&1; then
  "${AWS_CMD[@]}" glue create-job \
    --name "$GLUE_JOB_NAME" \
    --role "$GLUE_ROLE_ARN" \
    --command "$JOB_COMMAND" \
    --default-arguments "$DEFAULT_ARGUMENTS" \
    --execution-property "$EXECUTION_PROPERTY" \
    --glue-version "$REQUIRED_GLUE_VERSION" \
    --worker-type "G.1X" \
    --number-of-workers 5 \
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
      \"GlueVersion\": \"$REQUIRED_GLUE_VERSION\",
      \"WorkerType\": \"G.1X\",
      \"NumberOfWorkers\": 5,
      \"Connections\": $CONNECTIONS_ARG
    }" >/dev/null
  echo "Glue job updated."
fi

echo "Glue job deployment complete."
