"""Glue job: raw NFIP claims (JSON Lines) to cleaned Parquet, partitioned by year.

On AWS Glue the job receives --raw_path and --curated_path as job arguments.
The same file runs locally:

    python glue_jobs/clean_claims.py --raw_path data/raw/nfip_claims \
        --curated_path data/curated/nfip_claims
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession

from transforms import clean_claims


def run(spark: SparkSession, raw_path: str, curated_path: str) -> None:
    raw = spark.read.json(raw_path)
    cleaned = clean_claims(raw)
    # Only the partitions present in this run are replaced, so one year can be reloaded.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    cleaned.repartition("year_of_loss").write.mode("overwrite").partitionBy(
        "year_of_loss"
    ).parquet(curated_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw_path", required=True)
    parser.add_argument("--curated_path", required=True)
    # Glue passes extra arguments of its own (--JOB_NAME and others); ignore them.
    args, _ = parser.parse_known_args(sys.argv[1:])

    spark = SparkSession.builder.appName("clean_nfip_claims").getOrCreate()
    run(spark, args.raw_path, args.curated_path)
    spark.stop()


if __name__ == "__main__":
    main()
