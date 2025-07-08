#!/usr/bin/env bash
set -euo pipefail

# This script deploys a given Lambda function.
# Usage: ./deploy_lambda.sh <lambda_directory_name>
# Example: ./deploy_lambda.sh s3_yaml_to_mwaa
# Example: ./deploy_lambda.sh discover_tables_lambda

if [[ -z "$1" ]]; then
  echo "Usage: $0 <lambda_directory_name>"
  echo "Example: $0 s3_yaml_to_mwaa"
  exit 1
fi

LAMBDA_NAME=$1

# ---------------------------------------------------------------------------
# Static Settings
# ---------------------------------------------------------------------------
AWS_REGION="eu-west-2"
ROLE_NAME="lambda-airflow-trigger-role" # Shared role for both lambdas
BUCKET="{s3_asset_bucket}"
S3_PREFIX="configs/dev/archival/"
ZIP_NAME="lambda.zip"
MWAA_ENV_NAME="archival-dev"
# ---------------------------------------------------------------------------

# --- Dynamic Settings based on Lambda Name ---
FUNCTION_NAME=""
if [[ "$LAMBDA_NAME" == "s3_yaml_to_mwaa" ]]; then
  FUNCTION_NAME="s3-yaml-to-airflow"
elif [[ "$LAMBDA_NAME" == "discover_tables_lambda" ]]; then
  FUNCTION_NAME="archive-discover-tables"
else
  echo "Error: Unknown lambda name '$LAMBDA_NAME'"
  exit 1
fi
# ---

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LAMBDA_DIR="$SCRIPT_DIR/../lambda/$LAMBDA_NAME"

# Define the AWS command as an array for robust execution
AWS_CMD=("aws" "--region" "$AWS_REGION")

if [[ ! -d "$LAMBDA_DIR" ]]; then
    echo "Error: Lambda directory not found at $LAMBDA_DIR"
    exit 1
fi

echo "Packaging Lambda code for: $FUNCTION_NAME"
TMP_BUILD_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_BUILD_DIR"' EXIT

# 1. Install dependencies if requirements.txt is present
if [[ -f "$LAMBDA_DIR/requirements.txt" ]] && [[ -s "$LAMBDA_DIR/requirements.txt" ]]; then
  echo "Installing dependencies from requirements.txt"
  pip install --quiet --upgrade --target "$TMP_BUILD_DIR" -r "$LAMBDA_DIR/requirements.txt"
fi

# 2. Copy Lambda handler code
cp "$LAMBDA_DIR"/*.py "$TMP_BUILD_DIR"/

# 3. Create deployment zip
pushd "$TMP_BUILD_DIR" >/dev/null
zip -q -r "$ZIP_NAME" ./*
popd >/dev/null
mv "$TMP_BUILD_DIR/$ZIP_NAME" "$LAMBDA_DIR/"

echo "Built $ZIP_NAME"

# ---------------------------------------------------------------------------
# 1. IAM execution role (Shared for all lambdas)
# ---------------------------------------------------------------------------
TRUST='{
  "Version":"2012-10-17",
  "Statement":[{
    "Effect":"Allow",
    "Principal":{"Service":"lambda.amazonaws.com"},
    "Action":"sts:AssumeRole"
  }]
}'

if ! "${AWS_CMD[@]}" iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
  echo "Creating shared IAM role $ROLE_NAME"
  "${AWS_CMD[@]}" iam create-role --role-name "$ROLE_NAME" \
       --assume-role-policy-document "$TRUST" >/dev/null
  # Attach basic execution policy
  "${AWS_CMD[@]}" iam attach-role-policy \
       --role-name "$ROLE_NAME" \
       --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole
else
  echo "Re-using shared IAM role $ROLE_NAME"
fi

# -- Grant S3 read permission (for s3_yaml_to_mwaa) --
"${AWS_CMD[@]}" iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name "AllowS3ReadConfig" \
  --policy-document "{
    \"Version\":\"2012-10-17\",
    \"Statement\":[{
      \"Effect\":\"Allow\",
      \"Action\":[\"s3:GetObject\"],
      \"Resource\":\"arn:aws:s3:::$BUCKET/$S3_PREFIX*\"
    }]
  }"

# -- Grant MWAA CLI access (for s3_yaml_to_mwaa) --
ACCOUNT_ID=$("${AWS_CMD[@]}" sts get-caller-identity --query Account --output text)
MWAA_ENV_ARN="arn:aws:airflow:$AWS_REGION:$ACCOUNT_ID:environment/$MWAA_ENV_NAME"

"${AWS_CMD[@]}" iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name "AllowMWAAAccess" \
  --policy-document "{
    \"Version\":\"2012-10-17\",
    \"Statement\":[{
      \"Effect\":\"Allow\",
      \"Action\":[\"airflow:CreateCliToken\",\"airflow:InvokeCliApi\",\"airflow:GetEnvironment\"],
      \"Resource\":\"$MWAA_ENV_ARN\"
    }]
  }"

# -- Grant Glue and Secrets Manager access (for discover_tables_lambda) --
echo "Attaching policy for Glue and Secrets Manager access"
"${AWS_CMD[@]}" iam put-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-name "AllowGlueAndSecretsAccess" \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
        {
            \"Effect\": \"Allow\",
            \"Action\": [
                \"glue:GetConnection\",
                \"secretsmanager:GetSecretValue\"
            ],
            \"Resource\": \"*\"
        }
    ]
}"

ROLE_ARN=$("${AWS_CMD[@]}" iam get-role --role-name "$ROLE_NAME" \
           --query 'Role.Arn' --output text)

# ---------------------------------------------------------------------------
# 2. Lambda function (create or update)
# ---------------------------------------------------------------------------
if ! "${AWS_CMD[@]}" lambda get-function --function-name "$FUNCTION_NAME" >/dev/null 2>&1; then
  echo "Creating Lambda $FUNCTION_NAME"
  "${AWS_CMD[@]}" lambda create-function \
       --function-name "$FUNCTION_NAME" \
       --runtime python3.12 \
       --handler lambda_function.lambda_handler \
       --role "$ROLE_ARN" \
       --zip-file "fileb://$LAMBDA_DIR/$ZIP_NAME" \
       --timeout 60
else
  echo "Updating Lambda code for $FUNCTION_NAME"
  "${AWS_CMD[@]}" lambda update-function-code \
       --function-name "$FUNCTION_NAME" \
       --zip-file "fileb://$LAMBDA_DIR/$ZIP_NAME" >/dev/null
fi

echo "Waiting for Lambda to become active..."
"${AWS_CMD[@]}" lambda wait function-updated --function-name "$FUNCTION_NAME"

# ---------------------------------------------------------------------------
# 3. Environment variables (Conditional)
# ---------------------------------------------------------------------------
if [[ "$LAMBDA_NAME" == "s3_yaml_to_mwaa" ]]; then
    echo "Setting environment variables for $FUNCTION_NAME"
    "${AWS_CMD[@]}" lambda update-function-configuration \
        --function-name "$FUNCTION_NAME" \
        --environment "Variables={APP_REGION=$AWS_REGION,MWAA_ENV_NAME=$MWAA_ENV_NAME}"
fi


# ---------------------------------------------------------------------------
# 4 & 5. S3 Permissions & Trigger (ONLY for the S3 trigger lambda)
# ---------------------------------------------------------------------------
if [[ "$LAMBDA_NAME" == "s3_yaml_to_mwaa" ]]; then
    echo "Configuring S3 trigger for $FUNCTION_NAME"
    STAT_ID="s3invoke-$FUNCTION_NAME"
    "${AWS_CMD[@]}" lambda add-permission \
        --function-name "$FUNCTION_NAME" \
        --statement-id "$STAT_ID" \
        --action "lambda:InvokeFunction" \
        --principal s3.amazonaws.com \
        --source-arn "arn:aws:s3:::$BUCKET" 2>/dev/null || true

    NOTIF=$(cat <<EOF
{
  "LambdaFunctionConfigurations": [{
    "LambdaFunctionArn": "arn:aws:lambda:$AWS_REGION:$ACCOUNT_ID:function:$FUNCTION_NAME",
    "Events": ["s3:ObjectCreated:*"],
    "Filter": { "Key": { "FilterRules": [
      { "Name": "prefix", "Value": "$S3_PREFIX" },
      { "Name": "suffix", "Value": ".yaml" }
    ]}}
  }]
}
EOF
)
    echo "Setting bucket notification on $BUCKET"
    "${AWS_CMD[@]}" s3api put-bucket-notification-configuration \
        --bucket "$BUCKET" \
        --notification-configuration "$NOTIF"
else
    echo "Skipping S3 trigger configuration for $FUNCTION_NAME."
fi

echo "Lambda deployed successfully."
