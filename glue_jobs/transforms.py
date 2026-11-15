"""Cleaning rules for raw NFIP claims.

Kept apart from the Glue entry point so the same code runs in a local Spark
session and in the unit tests.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# raw API field -> (clean column name, Spark type)
COLUMNS: dict[str, tuple[str, str]] = {
    "id": ("claim_id", "bigint"),
    "asOfDate": ("as_of_date", "date"),
    "dateOfLoss": ("date_of_loss", "date"),
    "yearOfLoss": ("year_of_loss", "int"),
    "state": ("state", "string"),
    "countyCode": ("county_fips", "string"),
    "reportedZipCode": ("zip_code", "string"),
    "ratedFloodZone": ("rated_flood_zone", "string"),
    "floodEvent": ("flood_event", "string"),
    "causeOfDamage": ("cause_of_damage_code", "string"),
    "occupancyType": ("occupancy_type_code", "int"),
    "primaryResidenceIndicator": ("is_primary_residence", "boolean"),
    "originalConstructionDate": ("original_construction_date", "date"),
    "policyCount": ("policy_count", "int"),
    "buildingPropertyValue": ("building_property_value", "double"),
    "buildingDamageAmount": ("building_damage_amount", "double"),
    "contentsDamageAmount": ("contents_damage_amount", "double"),
    "totalBuildingInsuranceCoverage": ("building_coverage", "double"),
    "totalContentsInsuranceCoverage": ("contents_coverage", "double"),
    "amountPaidOnBuildingClaim": ("paid_building", "double"),
    "amountPaidOnContentsClaim": ("paid_contents", "double"),
    "amountPaidOnIncreasedCostOfComplianceClaim": ("paid_icc", "double"),
}

PAID_COLUMNS = ["paid_building", "paid_contents", "paid_icc"]


def clean_claims(raw: DataFrame) -> DataFrame:
    """Select, rename and type the columns, then drop bad and duplicate rows.

    Rules:
    - a row needs a claim id and a year of loss, otherwise it is dropped
    - when the same claim id appears more than once, the row with the latest
      as_of_date wins
    - total_paid adds the three payment columns, treating a missing one as zero
    """
    missing = [name for name in COLUMNS if name not in raw.columns]
    if missing:
        raise ValueError(f"raw data is missing expected fields: {missing}")

    typed = raw.select(
        *[F.col(source).cast(dtype).alias(name) for source, (name, dtype) in COLUMNS.items()]
    )
    valid = typed.where(F.col("claim_id").isNotNull() & F.col("year_of_loss").isNotNull())

    latest_first = Window.partitionBy("claim_id").orderBy(F.col("as_of_date").desc_nulls_last())
    deduped = (
        valid.withColumn("_row", F.row_number().over(latest_first))
        .where(F.col("_row") == 1)
        .drop("_row")
    )

    total_paid = sum(F.coalesce(F.col(name), F.lit(0.0)) for name in PAID_COLUMNS)
    return deduped.withColumn("state", F.upper(F.trim("state"))).withColumn(
        "total_paid", total_paid
    )
