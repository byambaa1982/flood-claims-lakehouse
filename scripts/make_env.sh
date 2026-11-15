#!/usr/bin/env bash
# Write .env for docker compose: temporary AWS credentials from the CLI session
# and the resource names from Terraform. .env is ignored by git.
set -euo pipefail
cd "$(dirname "$0")/.."

terraform_output() { terraform -chdir=terraform output -raw "$1"; }

{
  aws configure export-credentials --format env-no-export
  echo "AWS_DEFAULT_REGION=$(aws configure get region)"
  echo "RAW_BUCKET=$(terraform_output raw_bucket)"
  echo "CURATED_BUCKET=$(terraform_output curated_bucket)"
  echo "ATHENA_RESULTS_BUCKET=$(terraform_output athena_results_bucket)"
  echo "GLUE_JOB=$(terraform_output glue_job)"
} > .env
chmod 600 .env
echo "wrote .env"
