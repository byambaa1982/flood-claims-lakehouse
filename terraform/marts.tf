# dbt builds its models in this database.
resource "aws_glue_catalog_database" "marts" {
  name = replace("${var.project}_marts", "-", "_")
}

output "marts_database" {
  value = aws_glue_catalog_database.marts.name
}

output "athena_results_bucket" {
  value = aws_s3_bucket.this["athena-results"].bucket
}
