import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "glue_jobs"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingest"))

from download_claims import parse_years  # noqa: E402
from transforms import COLUMNS, clean_claims  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    yield session
    session.stop()


def raw_row(**overrides):
    row = {name: None for name in COLUMNS}
    row.update(
        id=1,
        asOfDate="2026-09-08T00:00:00.000Z",
        dateOfLoss="2016-10-08T00:00:00.000Z",
        yearOfLoss=2016,
        state=" va ",
        amountPaidOnBuildingClaim=100.0,
        amountPaidOnContentsClaim=50.0,
    )
    row.update(overrides)
    return row


SCHEMA = ", ".join(
    f"{name} {'string' if dtype == 'date' else dtype}" for name, (_, dtype) in COLUMNS.items()
)


def make_raw(spark, rows):
    return spark.createDataFrame([tuple(r[name] for name in COLUMNS) for r in rows], SCHEMA)


def test_columns_are_renamed_and_typed(spark):
    result = clean_claims(make_raw(spark, [raw_row()])).first()
    assert result["claim_id"] == 1
    assert str(result["date_of_loss"]) == "2016-10-08"
    assert result["state"] == "VA"


def test_total_paid_treats_missing_as_zero(spark):
    result = clean_claims(make_raw(spark, [raw_row()])).first()
    assert result["paid_icc"] is None
    assert result["total_paid"] == 150.0


def test_duplicate_claim_keeps_latest_as_of_date(spark):
    rows = [
        raw_row(asOfDate="2025-01-01T00:00:00.000Z", amountPaidOnBuildingClaim=1.0),
        raw_row(asOfDate="2026-09-08T00:00:00.000Z", amountPaidOnBuildingClaim=2.0),
    ]
    result = clean_claims(make_raw(spark, rows)).collect()
    assert len(result) == 1
    assert result[0]["paid_building"] == 2.0


def test_rows_without_id_or_year_are_dropped(spark):
    rows = [raw_row(), raw_row(id=None), raw_row(id=3, yearOfLoss=None)]
    assert clean_claims(make_raw(spark, rows)).count() == 1


def test_placeholder_dates_become_null(spark):
    row = raw_row(originalConstructionDate="0001-01-01T00:00:00.000Z")
    result = clean_claims(make_raw(spark, [row])).first()
    assert result["original_construction_date"] is None
    assert str(result["date_of_loss"]) == "2016-10-08"


def test_missing_source_field_is_an_error(spark):
    raw = make_raw(spark, [raw_row()]).drop("floodEvent")
    with pytest.raises(ValueError, match="floodEvent"):
        clean_claims(raw)


def test_parse_years():
    assert parse_years("2016") == [2016]
    assert parse_years("2005, 2010-2012") == [2005, 2010, 2011, 2012]
    with pytest.raises(ValueError):
        parse_years("2012-2010")
