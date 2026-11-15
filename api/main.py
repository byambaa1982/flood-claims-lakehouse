"""HTTP API over the claims marts and the question assistant.

    GET  /health                      no key needed
    GET  /claims/by-state?year=2017   claims and dollars per state for one year
    GET  /claims/events?limit=10      largest named flood events by amount paid
    POST /ask                         plain-English question, answered with SQL

Every route except /health needs the X-API-Key header. Run locally with:

    API_KEY=change-me uvicorn api.main:app
"""

from __future__ import annotations

import hmac
import os
import time
from typing import Annotated, Callable

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel, Field

from ai.athena import QueryFailed, QueryResult, run_query

MARTS = os.environ.get("MARTS_DATABASE", "flood_claims_lakehouse_marts")
CACHE_SECONDS = 300

app = FastAPI(title="Flood claims API", version="1.0.0")


async def require_api_key(x_api_key: Annotated[str | None, Header()] = None) -> None:
    expected = os.environ.get("API_KEY")
    if not expected:
        # Fail closed: an API started without a key serves nothing.
        raise HTTPException(status_code=503, detail="API_KEY is not configured")
    if x_api_key is None or not hmac.compare_digest(x_api_key, expected):
        raise HTTPException(status_code=401, detail="missing or wrong X-API-Key")


Protected = Depends(require_api_key)


class QueryCache:
    """Keeps each query's rows for a few minutes. The marts change once a month."""

    def __init__(self, runner: Callable[[str], QueryResult], seconds: int = CACHE_SECONDS):
        self.runner = runner
        self.seconds = seconds
        self.entries: dict[str, tuple[float, list[dict]]] = {}

    def fresh(self, sql: str) -> list[dict] | None:
        """The cached rows, or None when there are none or they are too old."""
        cached = self.entries.get(sql)
        if cached and time.monotonic() - cached[0] < self.seconds:
            return cached[1]
        return None

    def load(self, sql: str) -> list[dict]:
        """Run the query (this blocks) and remember its rows."""
        result = self.runner(sql)
        rows = [dict(zip(result.columns, row)) for row in result.rows]
        self.entries[sql] = (time.monotonic(), rows)
        return rows


cache = QueryCache(run_query)


async def get_cache() -> QueryCache:
    return cache


async def get_asker() -> Callable:
    from ai.agent import ask  # imported late so the data routes work without Bedrock

    return ask


def as_number(value: str | None) -> float | None:
    return None if value is None else float(value)


async def query(cache: QueryCache, sql: str) -> list[dict]:
    # A cache hit is answered on the event loop. Only a miss, which waits on
    # Athena, is handed to a worker thread.
    rows = cache.fresh(sql)
    if rows is not None:
        return rows
    try:
        return await run_in_threadpool(cache.load, sql)
    except QueryFailed as error:
        raise HTTPException(status_code=502, detail=f"query failed: {error}") from error


@app.get("/health")
async def health() -> dict:
    return {"status": "ok"}


@app.get("/claims/by-state", dependencies=[Protected])
async def claims_by_state(
    year: Annotated[int, Query(ge=2009, le=2100)],
    cache: Annotated[QueryCache, Depends(get_cache)],
) -> dict:
    # year is validated as an integer above, so it is safe to place in the SQL.
    rows = await query(
        cache,
        f"select state, claims, claims_paid, total_paid, avg_paid_per_paid_claim "
        f"from {MARTS}.mart_loss_by_state_year where year_of_loss = {year} "
        f"order by total_paid desc",
    )
    return {
        "year": year,
        "states": [
            {
                "state": row["state"],
                "claims": int(row["claims"]),
                "claims_paid": int(row["claims_paid"]),
                "total_paid": as_number(row["total_paid"]),
                "avg_paid_per_paid_claim": as_number(row["avg_paid_per_paid_claim"]),
            }
            for row in rows
        ],
    }


@app.get("/claims/events", dependencies=[Protected])
async def flood_events(
    cache: Annotated[QueryCache, Depends(get_cache)],
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
) -> dict:
    rows = await query(
        cache,
        f"select flood_event, first_loss_date, states_affected, claims, total_paid "
        f"from {MARTS}.mart_flood_events order by total_paid desc limit {limit}",
    )
    return {
        "events": [
            {
                "flood_event": row["flood_event"],
                "first_loss_date": row["first_loss_date"],
                "states_affected": int(row["states_affected"]),
                "claims": int(row["claims"]),
                "total_paid": as_number(row["total_paid"]),
            }
            for row in rows
        ]
    }


class Question(BaseModel):
    question: str = Field(min_length=5, max_length=500)


class AskResponse(BaseModel):
    answer: str
    queries: list[str]
    input_tokens: int
    output_tokens: int
    megabytes_scanned: float


@app.post("/ask", dependencies=[Protected])
async def ask_question(
    body: Question, asker: Annotated[Callable, Depends(get_asker)]
) -> AskResponse:
    # The assistant makes blocking calls to Bedrock and Athena, so it runs in a worker thread.
    result = await run_in_threadpool(asker, body.question)
    return AskResponse(
        answer=result.text,
        queries=result.queries,
        input_tokens=result.input_tokens,
        output_tokens=result.output_tokens,
        megabytes_scanned=round(result.bytes_scanned / 1e6, 2),
    )
