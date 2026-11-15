"""Download NFIP flood insurance claims from the OpenFEMA API.

Writes one gzipped JSON Lines file per API page, laid out by year of loss:

    <out>/nfip_claims/year_of_loss=2016/part-00000.jsonl.gz

<out> is a local folder or an s3://bucket/prefix location. Each year is
downloaded separately so that a failed run can be repeated for one year, and
so that paging never has to skip deep into the full 2.7 million rows.

Examples:
    python ingest/download_claims.py --out data/raw --years 2016
    python ingest/download_claims.py --out s3://my-raw-bucket/raw --years 2010-2024
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import logging
import sys
import time
from pathlib import Path

import requests

API_URL = "https://www.fema.gov/api/open/v3/NfipClaims"
ENTITY = "NfipClaims"
PAGE_SIZE = 10_000  # the API's maximum page size
MAX_ATTEMPTS = 5

log = logging.getLogger("ingest")


def parse_years(text: str) -> list[int]:
    """Turn '2016', '2010-2012' or '2005,2010-2012' into a sorted list of years."""
    years: set[int] = set()
    for part in text.split(","):
        part = part.strip()
        if "-" in part:
            first, last = (int(p) for p in part.split("-", 1))
            if first > last:
                raise ValueError(f"year range runs backwards: {part}")
            years.update(range(first, last + 1))
        else:
            years.add(int(part))
    return sorted(years)


def fetch_page(session: requests.Session, year: int, skip: int, page_size: int) -> dict:
    """Fetch one page for one year of loss, retrying on network and server errors."""
    params = {
        "$filter": f"yearOfLoss eq {year}",
        "$orderby": "id",
        "$top": page_size,
        "$skip": skip,
        "$count": "true",
    }
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = session.get(API_URL, params=params, timeout=120)
            if response.status_code < 500:
                response.raise_for_status()
                return response.json()
            log.warning("server error %s on attempt %s", response.status_code, attempt)
        except (requests.ConnectionError, requests.Timeout) as error:
            log.warning("network error on attempt %s: %s", attempt, error)
        if attempt < MAX_ATTEMPTS:
            time.sleep(2**attempt)
    raise RuntimeError(f"gave up on year {year}, skip {skip} after {MAX_ATTEMPTS} attempts")


def to_jsonl_gz(records: list[dict]) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as gz:
        for record in records:
            gz.write(json.dumps(record, separators=(",", ":")).encode("utf-8"))
            gz.write(b"\n")
    return buffer.getvalue()


class LocalWriter:
    def __init__(self, root: str) -> None:
        self.root = Path(root)

    def write(self, key: str, body: bytes) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)


class S3Writer:
    def __init__(self, location: str) -> None:
        import boto3  # imported here so local runs do not need AWS installed

        bucket, _, prefix = location.removeprefix("s3://").partition("/")
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.client = boto3.client("s3")

    def write(self, key: str, body: bytes) -> None:
        full_key = f"{self.prefix}/{key}" if self.prefix else key
        self.client.put_object(Bucket=self.bucket, Key=full_key, Body=body)


def make_writer(out: str) -> LocalWriter | S3Writer:
    return S3Writer(out) if out.startswith("s3://") else LocalWriter(out)


def download_year(
    session: requests.Session,
    writer: LocalWriter | S3Writer,
    year: int,
    page_size: int = PAGE_SIZE,
    max_pages: int | None = None,
) -> int:
    """Download every page for one year. Returns the number of rows written."""
    rows = 0
    page = 0
    while max_pages is None or page < max_pages:
        payload = fetch_page(session, year, skip=page * page_size, page_size=page_size)
        records = payload[ENTITY]
        if not records:
            break
        key = f"nfip_claims/year_of_loss={year}/part-{page:05d}.jsonl.gz"
        writer.write(key, to_jsonl_gz(records))
        rows += len(records)
        expected = payload["metadata"]["count"]
        log.info("year %s: page %s written, %s of %s rows", year, page, rows, expected)
        if rows >= expected:
            break
        page += 1
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--out", required=True, help="local folder or s3://bucket/prefix")
    parser.add_argument("--years", required=True, help="e.g. 2016, 2010-2024 or 2005,2010-2012")
    parser.add_argument("--page-size", type=int, default=PAGE_SIZE)
    parser.add_argument("--max-pages", type=int, default=None, help="stop early; for testing")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    writer = make_writer(args.out)
    started = time.monotonic()
    total = 0
    with requests.Session() as session:
        for year in parse_years(args.years):
            total += download_year(session, writer, year, args.page_size, args.max_pages)
    log.info("done: %s rows in %.0f seconds", total, time.monotonic() - started)
    return 0


if __name__ == "__main__":
    sys.exit(main())
