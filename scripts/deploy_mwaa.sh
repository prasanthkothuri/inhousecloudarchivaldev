#!/usr/bin/env bash
set -euo pipefail

#######################################
# User configuration (edit as needed) #
#######################################
AWS_REGION="eu-west-2"

# Core VPC & NAT information
VPC_ID="vpc-bfff81d7"
NAT_GW_ID="nat-0f7e32d5f639b83e0"          # existing NAT gateway (public subnet)

# Private subnet definitions (CIDRs must not overlap anything else in the VPC)
SUBNET_AZ_A="eu-west-2a"
SUBNET_A_CIDR="172.31.128.0/24"
SUBNET_NAME_A="private-a"

SUBNET_AZ_B="eu-west-2b"
SUBNET_B_CIDR="172.31.129.0/24"
SUBNET_NAME_B="private-b"

# MWAA / S3 / IAM parameters
S3_BUCKET="{s3_asset_bucket}"
ENV_NAME="archival-dev"
EXEC_ROLE_NAME="${ENV_NAME}-exec-role"
SG_NAME="${ENV_NAME}-sg"
AIRFLOW_VERSION="2.8.1"
ENV_CLASS="mw1.small"
MAX_WORKERS=2
#######################################

AWS="aws --region $AWS_REGION"

echo "Region: $AWS_REGION"
echo "VPC: $VPC_ID"

#######################################
# 1. S3 bucket for DAGs / plugins     #
#######################################
if ! $AWS s3api head-bucket --bucket "$S3_BUCKET" 2>/dev/null; then
  echo "Creating S3 bucket $S3_BUCKET"
  $AWS s3 mb "s3://$S3_BUCKET"
fi
$AWS s3api put-object --bucket "$S3_BUCKET" --key "dags/"     >/dev/null
$AWS s3api put-object --bucket "$S3_BUCKET" --key "plugins/"  >/dev/null

#######################################
# 2. Execution IAM role               #
#######################################
TRUST_DOC='{
  "Version":"2012-10-17",
  "Statement":[{
    "Effect":"Allow",
    "Principal":{"Service":["airflow-env.amazonaws.com","airflow.amazonaws.com"]},
    "Action":"sts:AssumeRole"
  }]
}'
if ! $AWS iam get-role --role-name "$EXEC_ROLE_NAME" >/dev/null 2>&1; then
  echo "Creating IAM role $EXEC_ROLE_NAME"
  $AWS iam create-role --role-name "$EXEC_ROLE_NAME" \
       --assume-role-policy-document "$TRUST_DOC" >/dev/null
  $AWS iam attach-role-policy --role-name "$EXEC_ROLE_NAME" \
       --policy-arn arn:aws:iam::aws:policy/AdministratorAccess
else
  echo "IAM role exists: $EXEC_ROLE_NAME (updating trust policy)"
  $AWS iam update-assume-role-policy --role-name "$EXEC_ROLE_NAME" \
       --policy-document "$TRUST_DOC"
fi
ROLE_ARN=$($AWS iam get-role --role-name "$EXEC_ROLE_NAME" --query 'Role.Arn' --output text)

# Get Account ID for ARN construction
ACCOUNT_ID=$($AWS sts get-caller-identity --query Account --output text)

# --- NEW: Grant MWAA permission to invoke the discover_tables_lambda ---
echo "Granting MWAA role permission to invoke discover-tables Lambda"
DISCOVER_LAMBDA_FUNCTION_NAME="archive-discover-tables"
DISCOVER_LAMBDA_ARN="arn:aws:lambda:$AWS_REGION:$ACCOUNT_ID:function:$DISCOVER_LAMBDA_FUNCTION_NAME"

$AWS iam put-role-policy \
  --role-name "$EXEC_ROLE_NAME" \
  --policy-name "AllowInvokeDiscoverTablesLambda" \
  --policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
        {
            \"Effect\": \"Allow\",
            \"Action\": \"lambda:InvokeFunction\",
            \"Resource\": \"$DISCOVER_LAMBDA_ARN\"
        }
    ]
}"

#######################################
# 3. Security group for MWAA          #
#######################################
SG_ID=$($AWS ec2 describe-security-groups \
          --filters Name=group-name,Values="$SG_NAME" Name=vpc-id,Values="$VPC_ID" \
          --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo "none")
if [[ "$SG_ID" == "none" || "$SG_ID" == "None" ]]; then
  echo "Creating security group $SG_NAME"
  SG_ID=$($AWS ec2 create-security-group --group-name "$SG_NAME" \
          --description "MWAA SG" --vpc-id "$VPC_ID" \
          --query 'GroupId' --output text)
else
  echo "Re-using security group $SG_NAME ($SG_ID)"
fi
$AWS ec2 authorize-security-group-ingress --group-id "$SG_ID" \
     --protocol -1 --source-group "$SG_ID" 2>/dev/null || true
$AWS ec2 authorize-security-group-egress --group-id "$SG_ID" \
     --protocol -1 --cidr 0.0.0.0/0 2>/dev/null || true

#######################################
# 4. Private subnets + route table    #
#######################################
get_subnet_id () {
  local name="$1"
  $AWS ec2 describe-subnets --filters "Name=tag:Name,Values=$name" \
       "Name=vpc-id,Values=$VPC_ID" \
       --query 'Subnets[0].SubnetId' --output text 2>/dev/null || echo "none"
}

SUBNET_ID_A=$(get_subnet_id "$SUBNET_NAME_A")
if [[ "$SUBNET_ID_A" == "none" || "$SUBNET_ID_A" == "None" ]]; then
  echo "Creating subnet $SUBNET_NAME_A"
  SUBNET_ID_A=$($AWS ec2 create-subnet --vpc-id "$VPC_ID" \
      --cidr-block "$SUBNET_A_CIDR" --availability-zone "$SUBNET_AZ_A" \
      --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=$SUBNET_NAME_A}]" \
      --query 'Subnet.SubnetId' --output text)
  $AWS ec2 modify-subnet-attribute --subnet-id "$SUBNET_ID_A" --no-map-public-ip-on-launch
fi

SUBNET_ID_B=$(get_subnet_id "$SUBNET_NAME_B")
if [[ "$SUBNET_ID_B" == "none" || "$SUBNET_ID_B" == "None" ]]; then
  echo "Creating subnet $SUBNET_NAME_B"
  SUBNET_ID_B=$($AWS ec2 create-subnet --vpc-id "$VPC_ID" \
      --cidr-block "$SUBNET_B_CIDR" --availability-zone "$SUBNET_AZ_B" \
      --tag-specifications "ResourceType=subnet,Tags=[{Key=Name,Value=$SUBNET_NAME_B}]" \
      --query 'Subnet.SubnetId' --output text)
  $AWS ec2 modify-subnet-attribute --subnet-id "$SUBNET_ID_B" --no-map-public-ip-on-launch
fi

# Route table (one per pair of private subnets is fine)
RT_NAME="private-rt"
RT_ID=$($AWS ec2 describe-route-tables \
          --filters "Name=tag:Name,Values=$RT_NAME" "Name=vpc-id,Values=$VPC_ID" \
          --query 'RouteTables[0].RouteTableId' --output text 2>/dev/null || echo "none")
if [[ "$RT_ID" == "none" || "$RT_ID" == "None" ]]; then
  echo "Creating route table $RT_NAME"
  RT_ID=$($AWS ec2 create-route-table --vpc-id "$VPC_ID" \
          --tag-specifications "ResourceType=route-table,Tags=[{Key=Name,Value=$RT_NAME}]" \
          --query 'RouteTable.RouteTableId' --output text)
  $AWS ec2 create-route --route-table-id "$RT_ID" \
       --destination-cidr-block 0.0.0.0/0 --nat-gateway-id "$NAT_GW_ID"
fi

# Associate route table with subnets (idempotent)
$AWS ec2 associate-route-table --route-table-id "$RT_ID" --subnet-id "$SUBNET_ID_A" 2>/dev/null || true
$AWS ec2 associate-route-table --route-table-id "$RT_ID" --subnet-id "$SUBNET_ID_B" 2>/dev/null || true

PRIVATE_SUBNETS=("$SUBNET_ID_A" "$SUBNET_ID_B")
echo "Private subnets ready: ${PRIVATE_SUBNETS[*]}"

#######################################
# 5. MWAA environment                 #
#######################################
ENV_STATUS=$($AWS mwaa get-environment --name "$ENV_NAME" \
                --query 'Environment.Status' --output text 2>/dev/null || echo "NONE")
if [[ "$ENV_STATUS" == "CREATING" || "$ENV_STATUS" == "AVAILABLE" ]]; then
  echo "MWAA environment exists (status: $ENV_STATUS)"
else
  echo "Creating MWAA environment $ENV_NAME (≈30 min)"
  $AWS mwaa create-environment \
    --name "$ENV_NAME" \
    --airflow-version "$AIRFLOW_VERSION" \
    --dag-s3-path "dags" \
    --source-bucket-arn "arn:aws:s3:::$S3_BUCKET" \
    --execution-role-arn "$ROLE_ARN" \
    --environment-class "$ENV_CLASS" \
    --max-workers "$MAX_WORKERS" \
    --webserver-access-mode PUBLIC_ONLY \
    --network-configuration "$(jq -n \
        --arg sg "$SG_ID" \
        --arg s1 "${PRIVATE_SUBNETS[0]}" \
        --arg s2 "${PRIVATE_SUBNETS[1]}" \
        '{SecurityGroupIds:[$sg], SubnetIds:[$s1,$s2]}')" \
    --logging-configuration "$(jq -n '
        {DagProcessingLogs:{Enabled:true,LogLevel:"INFO"},
         SchedulerLogs:{Enabled:true,LogLevel:"INFO"},
         TaskLogs:{Enabled:true,LogLevel:"INFO"},
         WebserverLogs:{Enabled:true,LogLevel:"INFO"},
         WorkerLogs:{Enabled:true,LogLevel:"INFO"}}')" >/dev/null
fi

echo "Done. Check MWAA status with:"
echo "$AWS mwaa get-environment --name $ENV_NAME --query 'Environment.Status'"
