"""Run one query on Athena and return its rows with the cost figures."""

from __future__ import annotations

import os
import time
from dataclasses import dataclass

import boto3

WORKGROUP = os.environ.get("ATHENA_WORKGROUP", "flood-claims-lakehouse")
DATABASE = os.environ.get("ATHENA_DATABASE", "flood_claims_lakehouse_curated")
TIMEOUT_SECONDS = 60


class QueryFailed(RuntimeError):
    """Athena rejected the query or it did not finish in time."""


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[list[str | None]]
    bytes_scanned: int
    milliseconds: int


def run_query(sql: str, client=None) -> QueryResult:
    client = client or boto3.client("athena")
    query_id = client.start_query_execution(
        QueryString=sql,
        WorkGroup=WORKGROUP,
        QueryExecutionContext={"Database": DATABASE},
    )["QueryExecutionId"]

    deadline = time.monotonic() + TIMEOUT_SECONDS
    while True:
        execution = client.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        state = execution["Status"]["State"]
        if state not in ("QUEUED", "RUNNING"):
            break
        if time.monotonic() > deadline:
            client.stop_query_execution(QueryExecutionId=query_id)
            raise QueryFailed(f"the query did not finish in {TIMEOUT_SECONDS} seconds")
        time.sleep(0.5)

    if state != "SUCCEEDED":
        raise QueryFailed(execution["Status"].get("StateChangeReason", state))

    result = client.get_query_results(QueryExecutionId=query_id)["ResultSet"]
    columns = [column["Name"] for column in result["ResultSetMetadata"]["ColumnInfo"]]
    # The first row repeats the column names.
    rows = [[cell.get("VarCharValue") for cell in row["Data"]] for row in result["Rows"][1:]]
    statistics = execution.get("Statistics", {})
    return QueryResult(
        columns=columns,
        rows=rows,
        bytes_scanned=statistics.get("DataScannedInBytes", 0),
        milliseconds=statistics.get("EngineExecutionTimeInMillis", 0),
    )
