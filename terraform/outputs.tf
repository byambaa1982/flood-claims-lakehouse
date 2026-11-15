output "raw_bucket" {
  value = aws_s3_bucket.this["raw"].bucket
}

output "curated_bucket" {
  value = aws_s3_bucket.this["curated"].bucket
}

output "glue_job" {
  value = aws_glue_job.clean_claims.name
}

output "glue_database" {
  value = aws_glue_catalog_database.curated.name
}

output "athena_workgroup" {
  value = aws_athena_workgroup.this.name
}
