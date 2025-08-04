#!/bin/bash

set -euo pipefail

# === IAM Role and Policy Configuration ===
ACCOUNT_ID="934336705194"
REGION="eu-west-1"
ROLE_NAME="natwest-data-archive-discover-role"
POLICY_NAME="natwest-data-archive-discover-policy"
POLICY_ARN="arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

# === Lambda Configuration ===
LAMBDA_NAME="natwest_data_archive_test_conn"
LAMBDA_NAME="natwest_data_archive_discover_tables"
ENTRY_DIR="lambda/${LAMBDA_NAME}"
BUILD_DIR="lambda_build"
ZIP_FILE="${LAMBDA_NAME}.zip"
RUNTIME="python3.9"
HANDLER="lambda_function.lambda_handler"
TIMEOUT=30
MEMORY_SIZE=256
SUBNET_ID="subnet-0318c7afee7d11794"
SECURITY_GROUP_ID="sg-097f6e3a3ad0b2721"

# # === Trust Policy ===
# TRUST_POLICY=$(cat <<EOF
# {
#   "Version": "2012-10-17",
#   "Statement": [{
#     "Effect": "Allow",
#     "Principal": { "Service": "lambda.amazonaws.com" },
#     "Action": "sts:AssumeRole"
#   }]
# }
# EOF
# )

# # === Inline IAM Policy ===
# POLICY_DOCUMENT=$(cat <<EOF
# {
#   "Version": "2012-10-17",
#   "Statement": [
#     {
#       "Sid": "AllowGlueConnectionAccess",
#       "Effect": "Allow",
#       "Action": "glue:GetConnection",
#       "Resource": [
#         "arn:aws:glue:${REGION}:${ACCOUNT_ID}:connection/onprem_orcl_conn",
#         "arn:aws:glue:${REGION}:${ACCOUNT_ID}:catalog"
#       ]
#     },
#     {
#       "Sid": "AllowSecretAccess",
#       "Effect": "Allow",
#       "Action": "secretsmanager:GetSecretValue",
#       "Resource": "arn:aws:secretsmanager:eu-west-1:934336705194:secret:dev/oracle/onprem_orcl-WBS9OV"
#     },
#     {
#       "Sid": "AllowLogging",
#       "Effect": "Allow",
#       "Action": [
#         "logs:CreateLogGroup",
#         "logs:CreateLogStream",
#         "logs:PutLogEvents"
#       ],
#       "Resource": "*"
#     },
#     {
#       "Sid": "AllowEC2NI",
#       "Effect": "Allow",
#       "Action": [
#         "ec2:CreateNetworkInterface",
#         "ec2:DescribeNetworkInterfaces",
#         "ec2:DeleteNetworkInterface"
#       ],
#       "Resource": "*"
#     }
#   ]
# }
# EOF
# )

# # === Create IAM Policy if Needed ===
# echo "Checking if policy '${POLICY_NAME}' exists..."
# if ! aws iam get-policy --policy-arn "${POLICY_ARN}" >/dev/null 2>&1; then
#   echo "Creating policy '${POLICY_NAME}'..."
#   aws iam create-policy \
#     --policy-name "${POLICY_NAME}" \
#     --policy-document "${POLICY_DOCUMENT}"
#   echo "Policy created."
# else
#   echo "Policy '${POLICY_NAME}' already exists. Skipping creation."
# fi

# # === Create IAM Role if Needed ===
# echo "Checking if role '${ROLE_NAME}' exists..."
# if ! aws iam get-role --role-name "$ROLE_NAME" >/dev/null 2>&1; then
#   echo "Creating role '${ROLE_NAME}'..."
#   aws iam create-role \
#     --role-name "$ROLE_NAME" \
#     --assume-role-policy-document "$TRUST_POLICY" >/dev/null
#   echo "Role created."
# else
#   echo "Role '${ROLE_NAME}' already exists."
# fi

# # === Attach Policy to Role if Not Attached ===
# echo "Ensuring policy '${POLICY_NAME}' is attached to role..."
# ATTACHED=$(aws iam list-attached-role-policies --role-name "$ROLE_NAME" \
#   --query "AttachedPolicies[?PolicyName=='${POLICY_NAME}']" --output text)
# if [[ -z "$ATTACHED" ]]; then
#   echo "Attaching policy..."
#   aws iam attach-role-policy \
#     --role-name "$ROLE_NAME" \
#     --policy-arn "$POLICY_ARN"
# else
#   echo "Policy already attached. Skipping."
# fi

# # === Create Lambda Function If It Doesn't Exist ===
# echo "Checking if Lambda '${LAMBDA_NAME}' exists..."
# if ! aws lambda get-function --function-name "${LAMBDA_NAME}" --region "${REGION}" >/dev/null 2>&1; then
#   echo "Creating Lambda '${LAMBDA_NAME}'..."
#   TMP_EMPTY_ZIP="empty.zip"
#   echo "placeholder" > placeholder.txt
#   zip -q "${TMP_EMPTY_ZIP}" placeholder.txt
#   rm placeholder.txt
#   aws lambda create-function \
#     --function-name "${LAMBDA_NAME}" \
#     --runtime "${RUNTIME}" \
#     --role "${ROLE_ARN}" \
#     --handler "${HANDLER}" \
#     --timeout "${TIMEOUT}" \
#     --memory-size "${MEMORY_SIZE}" \
#     --zip-file "fileb://${TMP_EMPTY_ZIP}" \
#     --region "${REGION}" \
#     --vpc-config SubnetIds="${SUBNET_ID}",SecurityGroupIds="${SECURITY_GROUP_ID}"
#   rm -f "${TMP_EMPTY_ZIP}"
# else
#   echo "Lambda '${LAMBDA_NAME}' already exists. Skipping creation."
# fi

# === Build Deployment Package ===
echo "Packaging Lambda code..."
rm -rf "${BUILD_DIR}" "${ZIP_FILE}"
mkdir -p "${BUILD_DIR}" "${BUILD_DIR}"/certs
pip3 install -r "${ENTRY_DIR}/requirements.txt" \
            --platform manylinux2014_x86_64 \
            --python-version 3.9 \
            --implementation cp \
            --only-binary=:all: \
            --upgrade -t "${BUILD_DIR}"
cp "${ENTRY_DIR}/lambda_function.py" "${BUILD_DIR}"
cp "${ENTRY_DIR}/ewallet.pem" "${BUILD_DIR}"/certs
cd "${BUILD_DIR}" && zip -r "../${ZIP_FILE}" . > /dev/null && cd ..

# === Upload Code ===
echo "Uploading code to Lambda..."
aws lambda update-function-code \
  --function-name "${LAMBDA_NAME}" \
  --zip-file "fileb://${ZIP_FILE}" \
  --region "${REGION}"

# === Apply VPC Config Again (in case it changed) ===
echo "Applying VPC config..."
aws lambda update-function-configuration \
  --function-name "${LAMBDA_NAME}" \
  --vpc-config SubnetIds="${SUBNET_ID}",SecurityGroupIds="${SECURITY_GROUP_ID}" \
  --region "${REGION}"

rm -rf "${BUILD_DIR}" "${ZIP_FILE}"

echo "Deployment complete."
