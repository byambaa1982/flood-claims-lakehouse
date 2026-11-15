locals {
  # Must match the columns and types written by glue_jobs/transforms.py.
  claims_columns = [
    ["claim_id", "bigint"],
    ["as_of_date", "date"],
    ["date_of_loss", "date"],
    ["state", "string"],
    ["county_fips", "string"],
    ["zip_code", "string"],
    ["rated_flood_zone", "string"],
    ["flood_event", "string"],
    ["cause_of_damage_code", "string"],
    ["occupancy_type_code", "int"],
    ["is_primary_residence", "boolean"],
    ["original_construction_date", "date"],
    ["policy_count", "int"],
    ["building_property_value", "double"],
    ["building_damage_amount", "double"],
    ["contents_damage_amount", "double"],
    ["building_coverage", "double"],
    ["contents_coverage", "double"],
    ["paid_building", "double"],
    ["paid_contents", "double"],
    ["paid_icc", "double"],
    ["total_paid", "double"],
  ]
}

resource "aws_glue_catalog_table" "claims" {
  name          = "nfip_claims"
  database_name = aws_glue_catalog_database.curated.name
  table_type    = "EXTERNAL_TABLE"

  # Partition projection: Athena works out the year folders itself, so a new
  # year needs no crawler and no 'MSCK REPAIR TABLE'.
  parameters = {
    "classification"                = "parquet"
    "projection.enabled"            = "true"
    "projection.year_of_loss.type"  = "integer"
    "projection.year_of_loss.range" = "1970,2100"
    "storage.location.template"     = "s3://${aws_s3_bucket.this["curated"].bucket}/nfip_claims/year_of_loss=$${year_of_loss}/"
  }

  partition_keys {
    name = "year_of_loss"
    type = "int"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.this["curated"].bucket}/nfip_claims/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    dynamic "columns" {
      for_each = local.claims_columns
      content {
        name = columns.value[0]
        type = columns.value[1]
      }
    }
  }
}
