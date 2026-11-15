# flood-claims-lakehouse

A small, end-to-end data lakehouse on AWS, built on public flood insurance
claims. It takes about 2.7 million claims from FEMA's National Flood Insurance
Program, lands them in S3, cleans them with a Glue Spark job, models them with
dbt on Athena, and runs the whole thing from one Airflow DAG.

This is a personal project. I built it to show how I put a pipeline together
on AWS: infrastructure as code, tested transformations, data quality checks
that stop a bad load, and a cost limit on every piece.

## Architecture

```
OpenFEMA API
     |
     |  ingest/download_claims.py      (Python, one folder per year of loss)
     v
S3 raw            gzipped JSON Lines
     |
     |  glue_jobs/clean_claims.py      (Glue Spark job: type, dedupe, derive)
     v
S3 curated        Parquet, partitioned by year_of_loss
     |
     |  dbt on Athena                  (staging and mart models, tests)   [planned]
     v
Marts             loss by state and year, paid against property value
     ^
     |
Airflow DAG       ingest -> Glue job -> dbt run -> dbt test               [planned]
```

## Status

| Part | State |
|---|---|
| Ingest from the OpenFEMA API | Working. Run locally against the live API |
| Cleaning rules (`glue_jobs/transforms.py`) | Working. Unit tested, and checked on a 4,000-row sample of real claims |
| Terraform for S3, Glue and Athena | Written. Not yet applied to an AWS account |
| Glue job run on AWS | Not yet run |
| Lake Formation permissions | Planned |
| dbt models and tests on Athena | Planned |
| Airflow DAG | Planned |
| Measured run time, throughput and cost | Planned, after the first full run |

I update this table as each part is finished. Nothing is marked working until
I have run it.

## The data

[OpenFEMA NFIP Redacted Claims, version 3](https://www.fema.gov/openfema-data-page/nfip-redacted-claims-v3):
one row per flood insurance claim, 84 fields, 2,725,989 rows as of November
2026. FEMA redacts it for privacy, so there are no names or street addresses.
It is public data published by FEMA. FEMA and the federal government do not
vouch for this project or for any analysis made from the data.

## Run it locally

Requires Python 3.11 and Java 17.

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Download one year of claims, about 85,000 rows for 2016
python ingest/download_claims.py --out data/raw --years 2016

# Clean it into partitioned Parquet
python glue_jobs/clean_claims.py \
    --raw_path data/raw/nfip_claims \
    --curated_path data/curated/nfip_claims

pytest -q tests
```

On Windows, Spark needs a `winutils.exe` and `hadoop.dll` that match its
Hadoop version before it can read and write local files. The tests do not
touch the file system and run without them.

## Deploy to AWS

```bash
cd terraform
terraform init
terraform apply

python ingest/download_claims.py --out s3://<raw_bucket>/raw --years 1978-2026
aws glue start-job-run --job-name flood-claims-lakehouse-clean-claims
```

Credentials come from your AWS CLI profile or environment variables. None are
stored in this repository.

## Cost controls

- The Glue job runs on 2 workers, the minimum, with a 30-minute timeout.
- The Athena workgroup stops any query that would scan more than 1 GB.
- Query results are deleted after 7 days.
- Every bucket blocks public access and is encrypted.
- `terraform destroy` removes everything, including the data.

## Design choices

- **One folder per year of loss in raw.** A failed download or a corrected year
  can be reloaded on its own, and the Glue job overwrites only the partitions
  it was given.
- **Cleaning rules live apart from the Glue entry point.** `transforms.py` has
  no Glue imports, so the same code runs in a local Spark session and in CI.
- **A missing source field fails the job.** If FEMA renames or drops a field,
  the run stops with the field name, instead of writing a column of nulls.
- **Duplicates resolve to the latest `asOfDate`.** FEMA republishes claims as
  they are updated; the newest version of a claim wins.

## License

MIT. See [LICENSE](LICENSE).
