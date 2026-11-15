terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
  default_tags {
    tags = {
      project = var.project
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  # Bucket names are global, so the account id keeps them unique.
  bucket_prefix = "${var.project}-${data.aws_caller_identity.current.account_id}"
  buckets       = toset(["raw", "curated", "athena-results", "scripts"])
}

# ---------- Storage ----------

resource "aws_s3_bucket" "this" {
  for_each      = local.buckets
  bucket        = "${local.bucket_prefix}-${each.key}"
  force_destroy = true # demo project: allow 'terraform destroy' to empty the buckets
}

resource "aws_s3_bucket_public_access_block" "this" {
  for_each                = aws_s3_bucket.this
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "this" {
  for_each = aws_s3_bucket.this
  bucket   = each.value.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Query results are throwaway; delete them after a week.
resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.this["athena-results"].id
  rule {
    id     = "expire-query-results"
    status = "Enabled"
    filter {}
    expiration {
      days = 7
    }
  }
}

# ---------- Catalog and query ----------

resource "aws_glue_catalog_database" "curated" {
  name = replace("${var.project}_curated", "-", "_")
}

resource "aws_athena_workgroup" "this" {
  name          = var.project
  force_destroy = true
  configuration {
    enforce_workgroup_configuration = true
    # Stop any single query that would scan more than this. Athena bills per byte scanned.
    bytes_scanned_cutoff_per_query = var.athena_scan_limit_bytes
    result_configuration {
      output_location = "s3://${aws_s3_bucket.this["athena-results"].bucket}/"
    }
  }
}

# ---------- Glue job ----------

resource "aws_s3_object" "glue_scripts" {
  for_each = toset(["clean_claims.py", "transforms.py"])
  bucket   = aws_s3_bucket.this["scripts"].id
  key      = "glue_jobs/${each.key}"
  source   = "${path.module}/../glue_jobs/${each.key}"
  etag     = filemd5("${path.module}/../glue_jobs/${each.key}")
}

data "aws_iam_policy_document" "glue_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue" {
  name               = "${var.project}-glue"
  assume_role_policy = data.aws_iam_policy_document.glue_assume.json
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# The job reads raw and scripts, and writes curated. Nothing else.
data "aws_iam_policy_document" "glue_data" {
  statement {
    actions = ["s3:GetObject", "s3:ListBucket"]
    resources = [
      aws_s3_bucket.this["raw"].arn, "${aws_s3_bucket.this["raw"].arn}/*",
      aws_s3_bucket.this["scripts"].arn, "${aws_s3_bucket.this["scripts"].arn}/*",
    ]
  }
  statement {
    actions = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
    resources = [
      aws_s3_bucket.this["curated"].arn, "${aws_s3_bucket.this["curated"].arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_data" {
  name   = "data-access"
  role   = aws_iam_role.glue.id
  policy = data.aws_iam_policy_document.glue_data.json
}

resource "aws_glue_job" "clean_claims" {
  name              = "${var.project}-clean-claims"
  role_arn          = aws_iam_role.glue.arn
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2  # the minimum for a Spark job
  timeout           = 30 # minutes; caps the cost of a stuck run
  max_retries       = 0

  command {
    name            = "glueetl"
    python_version  = "3"
    script_location = "s3://${aws_s3_bucket.this["scripts"].bucket}/${aws_s3_object.glue_scripts["clean_claims.py"].key}"
  }

  default_arguments = {
    "--extra-py-files" = "s3://${aws_s3_bucket.this["scripts"].bucket}/${aws_s3_object.glue_scripts["transforms.py"].key}"
    "--raw_path"       = "s3://${aws_s3_bucket.this["raw"].bucket}/raw/nfip_claims/"
    "--curated_path"   = "s3://${aws_s3_bucket.this["curated"].bucket}/nfip_claims/"
    "--job-language"   = "python"
  }
}
