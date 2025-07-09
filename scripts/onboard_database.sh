#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Script: onboard_database.sh
# Purpose: Securely create or update an AWS Glue JDBC connection + secret.
# -----------------------------------------------------------------------------

print_usage() {
  cat <<EOF
Usage: $0 --connection-name <name> --jdbc-url <url> --subnet-ids "<id1,id2>" --security-group-id <sg_id>
  --connection-name     Unique Glue connection name (e.g. nucleus_dev)
  --jdbc-url            Full JDBC connection string
  --subnet-ids          Comma‑separated subnet ID list (first is used)
  --security-group-id   Security group ID for Glue job network traffic
EOF
}

# -------- Argument parsing --------
CONNECTION_NAME=""; JDBC_URL=""; SUBNET_IDS=""; SG_ID=""
while [[ $# -gt 0 ]]; do
  case $1 in
    --connection-name) CONNECTION_NAME=$2; shift 2;;
    --jdbc-url)        JDBC_URL=$2;        shift 2;;
    --subnet-ids)      SUBNET_IDS=$2;      shift 2;;
    --security-group-id) SG_ID=$2;         shift 2;;
    *) echo "Unknown flag $1"; print_usage; exit 1;;
  esac
done

for var in CONNECTION_NAME JDBC_URL SUBNET_IDS SG_ID; do
  [[ -z ${!var:-} ]] && echo "Error: $var is required." && print_usage && exit 1
done

AWS_REGION="eu-west-2"
AWS_CMD=(aws --region "$AWS_REGION")
SECRET_NAME="glue/connections/${CONNECTION_NAME}"

# -------- Resolve primary subnet + AZ --------
IFS=',' read -r -a SUBNET_ARRAY <<< "$SUBNET_IDS"
PRIMARY_SUBNET="${SUBNET_ARRAY[0]}"
AZ=$("${AWS_CMD[@]}" ec2 describe-subnets --subnet-ids "$PRIMARY_SUBNET" --query 'Subnets[0].AvailabilityZone' --output text)

echo "--- Onboarding/Updating Database: $CONNECTION_NAME ---"

# -------- Secret management --------
if ! "${AWS_CMD[@]}" secretsmanager describe-secret --secret-id "$SECRET_NAME" >/dev/null 2>&1; then
  read -rp "Enter database username: " DB_USERNAME
  read -rsp "Enter database password: " DB_PASSWORD; echo
  [[ -z $DB_USERNAME || -z $DB_PASSWORD ]] && { echo "Username/password cannot be empty"; exit 1; }
  "${AWS_CMD[@]}" secretsmanager create-secret \
    --name "$SECRET_NAME" \
    --description "Credentials for Glue Connection $CONNECTION_NAME" \
    --secret-string "$(jq -nc --arg u "$DB_USERNAME" --arg p "$DB_PASSWORD" '{username:$u,password:$p}')" >/dev/null
  echo "✅ Secret created: $SECRET_NAME"
else
  echo "Secret $SECRET_NAME already exists – skipping creation."
fi

# -------- Glue connection payload --------
CONN_PAYLOAD=$(jq -nc \
  --arg name "$CONNECTION_NAME" \
  --arg url  "$JDBC_URL" \
  --arg sid  "$SECRET_NAME" \
  --arg sub  "$PRIMARY_SUBNET" \
  --arg sg   "$SG_ID" \
  --arg az   "$AZ" \
  '{
     Name:$name,
     Description:"Connection to \($name) DB",
     ConnectionType:"JDBC",
     ConnectionProperties:{
       JDBC_CONNECTION_URL:$url,
       SECRET_ID:$sid
     },
     PhysicalConnectionRequirements:{
       SubnetId:$sub,
       SecurityGroupIdList:[$sg],
       AvailabilityZone:$az
     }
   }')

CONN_UPDATE=$(jq -nc --argjson ci "$CONN_PAYLOAD" '{Name: $ci.Name, ConnectionInput:$ci}')

if "${AWS_CMD[@]}" glue get-connection --name "$CONNECTION_NAME" >/dev/null 2>&1; then
  echo "Updating existing Glue connection…"
  "${AWS_CMD[@]}" glue update-connection --cli-input-json "${CONN_UPDATE}"
else
  echo "Creating new Glue connection…"
  "${AWS_CMD[@]}" glue create-connection --connection-input "${CONN_PAYLOAD}"
fi

echo "Glue connection '$CONNECTION_NAME' configured (subnet $PRIMARY_SUBNET | AZ $AZ)."

echo "--- Onboarding Complete ---"
