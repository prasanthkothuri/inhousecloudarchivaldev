#!/usr/bin/env bash
#
# delete_iceberg_db_athena.sh
#
# Deletes all tables in a Glue database via Athena (to purge Iceberg metadata + data),
# then deletes the database itself.
#
# Usage:
#   ./delete_iceberg_db_athena.sh <database_name> [athena_workgroup] [region]
#
# Example:
#   ./delete_iceberg_db_athena.sh archive_nucleus_dev primary eu-west-2
#

set -euo pipefail

DB_NAME=${1:-}
WG_NAME=${2:-primary}
REGION=${3:-eu-west-2}

if [[ -z "$DB_NAME" ]]; then
  echo "Usage: $0 <database_name> [athena_workgroup] [region]"
  exit 1
fi

# Fetch the Athena workgroup output location
OUTPUT_LOCATION=$(aws athena get-work-group \
  --region "$REGION" \
  --work-group "$WG_NAME" \
  --query 'WorkGroup.Configuration.ResultConfiguration.OutputLocation' \
  --output text)

if [[ "$OUTPUT_LOCATION" == "None" ]]; then
  echo "Workgroup '$WG_NAME' has no OutputLocation. Configure one before running."
  exit 1
fi

echo "Athena workgroup: $WG_NAME"
echo "Query result S3: $OUTPUT_LOCATION"
echo "Region: $REGION"
echo

# List all tables in the Glue database
TABLES=$(aws glue get-tables \
  --region "$REGION" \
  --database-name "$DB_NAME" \
  --query 'TableList[].Name' \
  --output text)

if [[ -z "$TABLES" ]]; then
  echo "No tables found in database '$DB_NAME'."
else
  echo "The following tables will be dropped:"
  for t in $TABLES; do
    echo "  $t"
  done
fi
echo

read -r -p "Type DELETE to proceed: " CONFIRM
[[ "$CONFIRM" == "DELETE" ]] || { echo "Aborted."; exit 0; }

run_athena() {
  local sql="$1"
  local qid
  qid=$(aws athena start-query-execution \
          --region "$REGION" \
          --work-group "$WG_NAME" \
          --query-string "$sql" \
          --query 'QueryExecutionId' \
          --result-configuration "OutputLocation=$OUTPUT_LOCATION" \
          --output text)

  while true; do
    state=$(aws athena get-query-execution \
              --region "$REGION" \
              --query-execution-id "$qid" \
              --query 'QueryExecution.Status.State' \
              --output text)
    case $state in
      SUCCEEDED) return 0 ;;
      FAILED|CANCELLED)
        echo "Query failed: $sql"
        aws athena get-query-execution \
          --region "$REGION" \
          --query-execution-id "$qid" \
          --query 'QueryExecution.Status.StateChangeReason' \
          --output text
        exit 1 ;;
      *) sleep 2 ;;
    esac
  done
}

# Drop each table
for t in $TABLES; do
  echo "Dropping table: $DB_NAME.$t"
  run_athena "DROP TABLE IF EXISTS \`$DB_NAME\`.\`$t\`;"
  echo "Dropped: $DB_NAME.$t"
done

# Drop the database
run_athena "DROP DATABASE IF EXISTS \`$DB_NAME\`;"

echo "Database '$DB_NAME' and all tables have been deleted in region '$REGION'."
