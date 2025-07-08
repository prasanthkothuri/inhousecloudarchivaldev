#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Resolve repo root (directory that contains *this* deploy.sh)
# ---------------------------------------------------------------------------
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
REPO_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"

# --- Shared Settings ---
BUCKET="{s3_asset_bucket}"

# Convenience
DEPLOY_LAMBDA="$REPO_ROOT/scripts/deploy_lambda.sh"
DEPLOY_GLUE="$REPO_ROOT/scripts/deploy_glue.sh"
DAGS_DIR="$REPO_ROOT/airflow"

echo "==> Deploying MWAA infrastructure"
# "$REPO_ROOT/scripts/deploy_mwaa.sh"  # uncomment if you want infra each run

echo "==> Deploying Lambda functions"
# Call the generic deploy script for each lambda
#"$DEPLOY_LAMBDA" s3_yaml_to_mwaa
#"$DEPLOY_LAMBDA" discover_tables_lambda

echo "==> Deploying Glue Job"
# Call the new Glue deployment script
"$DEPLOY_GLUE"

echo "==> Uploading DAGs to Airflow S3 bucket"
aws s3 sync "$DAGS_DIR" "s3://$BUCKET/" \
  --exclude "*" --include "dags/*.py"

echo "Deployment complete"
