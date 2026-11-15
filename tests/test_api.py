import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ai.agent import Answer  # noqa: E402
from ai.athena import QueryFailed, QueryResult  # noqa: E402
from api.main import QueryCache, app, get_asker, get_cache  # noqa: E402

KEY = {"X-API-Key": "test-key"}


class FakeAthena:
    """Stands in for Athena. Records each statement it is asked to run."""

    def __init__(self):
        self.statements = []
        self.fail = False

    def __call__(self, sql):
        self.statements.append(sql)
        if self.fail:
            raise QueryFailed("athena is down")
        if "mart_flood_events" in sql:
            return QueryResult(
                ["flood_event", "first_loss_date", "states_affected", "claims", "total_paid"],
                [["Hurricane Sandy", "2012-10-27", "17", "144848", "8.9E9"]],
                1000,
                300,
            )
        return QueryResult(
            ["state", "claims", "claims_paid", "total_paid", "avg_paid_per_paid_claim"],
            [
                ["TX", "92000", "76000", "9.078567037E9", "119000.5"],
                ["FL", "30000", "20000", "1.0E9", None],
            ],
            1000,
            300,
        )


@pytest.fixture
def athena(monkeypatch):
    monkeypatch.setenv("API_KEY", "test-key")
    fake = FakeAthena()
    fake_cache = QueryCache(fake)
    app.dependency_overrides[get_cache] = lambda: fake_cache
    yield fake
    app.dependency_overrides.clear()


client = TestClient(app)


def test_health_needs_no_key():
    assert client.get("/health").json() == {"status": "ok"}


def test_missing_or_wrong_key_is_rejected(athena):
    assert client.get("/claims/by-state?year=2017").status_code == 401
    wrong = client.get("/claims/by-state?year=2017", headers={"X-API-Key": "nope"})
    assert wrong.status_code == 401
    assert athena.statements == []


def test_api_without_a_configured_key_serves_nothing(athena, monkeypatch):
    monkeypatch.delenv("API_KEY")
    assert client.get("/claims/by-state?year=2017", headers=KEY).status_code == 503


def test_by_state_returns_typed_rows(athena):
    body = client.get("/claims/by-state?year=2017", headers=KEY).json()
    assert body["year"] == 2017
    assert body["states"][0] == {
        "state": "TX",
        "claims": 92000,
        "claims_paid": 76000,
        "total_paid": 9078567037.0,
        "avg_paid_per_paid_claim": 119000.5,
    }
    assert body["states"][1]["avg_paid_per_paid_claim"] is None
    assert "year_of_loss = 2017" in athena.statements[0]


@pytest.mark.parametrize("year", ["1999", "abc", "2017; drop table x", ""])
def test_bad_year_never_reaches_athena(athena, year):
    response = client.get("/claims/by-state", params={"year": year}, headers=KEY)
    assert response.status_code == 422
    assert athena.statements == []


def test_repeat_requests_are_served_from_the_cache(athena):
    for _ in range(3):
        assert client.get("/claims/by-state?year=2017", headers=KEY).status_code == 200
    assert len(athena.statements) == 1


def test_events_limit_is_bounded(athena):
    body = client.get("/claims/events?limit=5", headers=KEY).json()
    assert body["events"][0]["claims"] == 144848
    assert "limit 5" in athena.statements[0]
    assert client.get("/claims/events?limit=1000", headers=KEY).status_code == 422


def test_athena_failure_is_a_502(athena):
    athena.fail = True
    response = client.get("/claims/by-state?year=2018", headers=KEY)
    assert response.status_code == 502
    assert "athena is down" in response.json()["detail"]


def test_ask_returns_the_answer_and_its_queries(athena):
    def fake_ask(question):
        return Answer(
            text=f"Answer to: {question}",
            queries=["select 1"],
            input_tokens=10,
            output_tokens=5,
            bytes_scanned=2_500_000,
        )

    app.dependency_overrides[get_asker] = lambda: fake_ask
    response = client.post("/ask", json={"question": "How many claims in 2017?"}, headers=KEY)
    assert response.json() == {
        "answer": "Answer to: How many claims in 2017?",
        "queries": ["select 1"],
        "input_tokens": 10,
        "output_tokens": 5,
        "megabytes_scanned": 2.5,
    }


def test_ask_rejects_short_and_oversized_questions(athena):
    app.dependency_overrides[get_asker] = lambda: (lambda question: None)
    assert client.post("/ask", json={"question": "hi"}, headers=KEY).status_code == 422
    assert client.post("/ask", json={"question": "x" * 501}, headers=KEY).status_code == 422
