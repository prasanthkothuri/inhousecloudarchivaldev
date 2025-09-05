#!/usr/bin/env bash
set -euo pipefail
export AWS_PAGER=""

FILE="${1:-connections.json}"
[ -f "$FILE" ] || { echo "Missing $FILE (pass a path or create it)"; exit 1; }
command -v jq >/dev/null || { echo "jq is required"; exit 1; }
command -v aws >/dev/null || { echo "aws CLI is required"; exit 1; }

echo "--- Onboarding connections from $FILE ---"

jq -c '.[]' "$FILE" | while read -r row; do
  NAME=$(jq -r '.name' <<<"$row")
  REGION=$(jq -r '.region' <<<"$row")
  JDBC_URL=$(jq -r '.jdbc_url' <<<"$row")
  SUBNET_ID=$(jq -r '.subnet_id' <<<"$row")
  SG_JSON=$(jq -c '.security_group_ids' <<<"$row")
  ENFORCE_SSL=$(jq -r '.enforce_ssl // false' <<<"$row")    # boolean
  SECRET_NAME=$(jq -r '.secret_name // ("glue/connections/" + .name)' <<<"$row")
  EXTRA_PROPS=$(jq -c '.properties // {}' <<<"$row")
  USER_ENV=$(jq -r '.username_env // empty' <<<"$row")
  PASS_ENV=$(jq -r '.password_env // empty' <<<"$row")

  AWS_CMD=(aws --region "$REGION")
  echo ""
  echo "==> [$REGION] $NAME"

  # ---- Secret: create if missing (using env var names from JSON); otherwise reuse ----
  if "${AWS_CMD[@]}" secretsmanager describe-secret --secret-id "$SECRET_NAME" >/dev/null 2>&1; then
    echo "   Secret exists: $SECRET_NAME — reusing."
  else
    if [[ "$SECRET_NAME" == arn:aws:secretsmanager:* ]]; then
      echo "   ERROR: secret ARN not found: $SECRET_NAME (cannot auto-create by ARN)"; exit 1
    fi
    [[ -n "$USER_ENV" && -n "$PASS_ENV" ]] || { echo "   ERROR: username_env/password_env must be set in JSON for $NAME"; exit 1; }
    DB_USERNAME="${!USER_ENV-}"; DB_PASSWORD="${!PASS_ENV-}"
    [[ -n "$DB_USERNAME" && -n "$DB_PASSWORD" ]] || { echo "   ERROR: env vars $USER_ENV / $PASS_ENV are not set"; exit 1; }

    "${AWS_CMD[@]}" secretsmanager create-secret \
      --name "$SECRET_NAME" \
      --description "Credentials for Glue Connection $NAME" \
      --secret-string "$(jq -nc --arg u "$DB_USERNAME" --arg p "$DB_PASSWORD" '{username:$u,password:$p}')" >/dev/null
    echo "   Secret created: $SECRET_NAME"
  fi

  # ---- Build Glue connection payload (no AZ lookup) ----
  ENF_STR=$([ "$ENFORCE_SSL" = "true" ] && echo "true" || echo "false")
  DESC="Connection to ${NAME}"

  CONN_PAYLOAD=$(jq -nc \
    --arg name "$NAME" \
    --arg desc "$DESC" \
    --arg url  "$JDBC_URL" \
    --arg sid  "$SECRET_NAME" \
    --arg sub  "$SUBNET_ID" \
    --arg enf  "$ENF_STR" \
    --argjson sgs "$SG_JSON" \
    --argjson extra "$EXTRA_PROPS" \
    '{
       Name:$name,
       Description:$desc,
       ConnectionType:"JDBC",
       ConnectionProperties: (
         { JDBC_CONNECTION_URL:$url,
           SECRET_ID:$sid,
           JDBC_ENFORCE_SSL:$enf
         } + $extra
       ),
       PhysicalConnectionRequirements:{
         SubnetId:$sub,
         SecurityGroupIdList:$sgs
       }
     }')

  # ---- Create if missing; skip if exists ----
  if "${AWS_CMD[@]}" glue get-connection --name "$NAME" >/dev/null 2>&1; then
    echo "   Glue connection '$NAME' already exists — skipping."
  else
    echo "   Creating Glue connection '$NAME'…"
    "${AWS_CMD[@]}" glue create-connection --connection-input "$CONN_PAYLOAD" >/dev/null
    echo "   Created."
  fi
done

echo ""
echo "--- Done. ---"
