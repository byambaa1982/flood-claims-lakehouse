#!/usr/bin/env bash
# Run one Athena query in the project workgroup and print its status,
# bytes scanned, run time in milliseconds, and result rows.
#
#   scripts/athena_query.sh "select count(*) from flood_claims_lakehouse_curated.nfip_claims"
set -euo pipefail

workgroup="${ATHENA_WORKGROUP:-flood-claims-lakehouse}"
sql="$1"

id=$(aws athena start-query-execution --work-group "$workgroup" \
  --query-string "$sql" --query QueryExecutionId --output text)

while true; do
  state=$(aws athena get-query-execution --query-execution-id "$id" \
    --query "QueryExecution.Status.State" --output text)
  if [ "$state" != "QUEUED" ] && [ "$state" != "RUNNING" ]; then
    break
  fi
  sleep 2
done

aws athena get-query-execution --query-execution-id "$id" --output text --query \
  "QueryExecution.[Status.State,Status.StateChangeReason,Statistics.DataScannedInBytes,Statistics.EngineExecutionTimeInMillis]"

if [ "$state" = "SUCCEEDED" ]; then
  aws athena get-query-results --query-execution-id "$id" --output text \
    --query "ResultSet.Rows[].[join(' | ', Data[].VarCharValue)]"
fi
