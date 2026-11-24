from __future__ import annotations

import asyncio
import math
import uuid

import asyncpg
import pytest
from httpx import ASGITransport, AsyncClient

import company_rec.database as database_module
from company_rec.api import create_app
from company_rec.database import (
    CompanyDatabase,
    DatabaseUnavailableError,
    SimilarCompany,
)
from company_rec.embeddings import GeneratedEmbedding
from tests.helpers import event


class FakeStore:
    def __init__(self, results: list[SimilarCompany] | None = None, error: Exception | None = None):
        self.results = results or []
        self.error = error
        self.calls: list[tuple[str, int]] = []
        self.closed = False

    async def get_similar_companies(self, registration_number: str, limit: int):
        self.calls.append((registration_number, limit))
        if self.error is not None:
            raise self.error
        return self.results[:limit]

    async def close(self):
        self.closed = True


async def get_json(app, path: str):
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        return await client.get(path)


def vector_embedding(x: float, y: float) -> GeneratedEmbedding:
    return GeneratedEmbedding(
        source="generic",
        text="controlled test vector",
        model="controlled-test-model",
        values=(x, y, *([0.0] * 382)),
    )


async def seed_company(database, registration_number: str, x: float, y: float, data: dict | None = None):
    company_event = event(
        registration_number=registration_number,
        message_id=f"f2-{registration_number}",
        data=data or {},
    )
    assert await database.upsert_company(company_event, vector_embedding(x, y))


def cosine_similarity(left: tuple[float, float], right: tuple[float, float]) -> float:
    dot_product = left[0] * right[0] + left[1] * right[1]
    left_norm = math.sqrt(left[0] ** 2 + left[1] ** 2)
    right_norm = math.sqrt(right[0] ** 2 + right[1] ** 2)
    return dot_product / (left_norm * right_norm)


@pytest.mark.asyncio
async def test_request_normalizes_id_and_applies_default_and_explicit_limits():
    store = FakeStore(
        [
            SimilarCompany(f"SYN-00000{number}", f"Company {number}", None, None, 0.5)
            for number in range(1, 5)
        ]
    )
    app = create_app(database=store)

    default_response = await get_json(app, "/companies/%20syn-000100%20/similar")
    limited_response = await get_json(app, "/companies/SYN-000100/similar?limit=2")
    minimum_response = await get_json(app, "/companies/SYN-000100/similar?limit=1")

    assert default_response.status_code == 200
    assert default_response.json()["registration_number"] == "SYN-000100"
    assert len(default_response.json()["recommendations"]) == 4
    assert len(limited_response.json()["recommendations"]) == 2
    assert len(minimum_response.json()["recommendations"]) == 1
    assert store.calls == [
        ("SYN-000100", 4),
        ("SYN-000100", 2),
        ("SYN-000100", 1),
    ]


@pytest.mark.parametrize(
    "path",
    [
        "/companies/not-an-id/similar",
        "/companies/SYN-000100/similar?limit=0",
        "/companies/SYN-000100/similar?limit=5",
        "/companies/SYN-000100/similar?limit=abc",
    ],
)
@pytest.mark.asyncio
async def test_invalid_identifier_or_limit_returns_sanitized_422(path: str):
    store = FakeStore()
    response = await get_json(create_app(database=store), path)

    assert response.status_code == 422
    assert response.json() == {"error": {"code": "invalid_request", "message": "Invalid request"}}
    assert store.calls == []


@pytest.mark.asyncio
async def test_database_unavailability_returns_sanitized_503():
    store = FakeStore(error=DatabaseUnavailableError("postgres://private-host/private-db"))
    response = await get_json(create_app(database=store), "/companies/SYN-000100/similar")

    assert response.status_code == 503
    assert response.json() == {"error": {"code": "database_unavailable", "message": "Database unavailable"}}
    assert "private-host" not in response.text


@pytest.mark.asyncio
async def test_database_connection_error_maps_to_sanitized_503():
    class UnavailablePool:
        def acquire(self, timeout: float):
            return self

        async def __aenter__(self):
            raise asyncpg.PostgresConnectionError("private database connection string")

        async def __aexit__(self, *_args):
            return False

    database = CompanyDatabase(UnavailablePool())
    response = await get_json(create_app(database=database), "/companies/SYN-000100/similar")

    assert response.status_code == 503
    assert response.json() == {"error": {"code": "database_unavailable", "message": "Database unavailable"}}
    assert "private database" not in response.text


@pytest.mark.asyncio
async def test_pool_acquisition_timeout_returns_sanitized_503():
    store = FakeStore(error=TimeoutError("pool wait exceeded"))
    response = await get_json(create_app(database=store), "/companies/SYN-000100/similar")

    assert response.status_code == 503
    assert response.json() == {"error": {"code": "database_unavailable", "message": "Database unavailable"}}


@pytest.mark.asyncio
async def test_unexpected_error_returns_sanitized_500():
    store = FakeStore(error=RuntimeError("internal SQL and credential details"))
    response = await get_json(create_app(database=store), "/companies/SYN-000100/similar")

    assert response.status_code == 500
    assert response.json() == {"error": {"code": "internal_error", "message": "Internal server error"}}
    assert "credential" not in response.text


@pytest.mark.asyncio
async def test_lifespan_closes_the_database_pool():
    store = FakeStore()
    received_dsn = False

    async def connect(_dsn: str):
        nonlocal received_dsn
        received_dsn = True
        return store

    app = create_app(database_factory=connect)
    async with app.router.lifespan_context(app):
        response = await get_json(app, "/companies/SYN-000100/similar")
        assert response.status_code == 200

    assert received_dsn
    assert store.closed


@pytest.mark.asyncio
async def test_pgvector_ranking_matches_independent_cosine_scores_and_limits(postgres_store):
    database, _, _ = postgres_store
    source_id = "SYN-000100"
    source_vector = (1.0, 0.0)
    candidates = {
        "SYN-000105": ((0.8, 0.6), {"companyName": "Nearest", "industry": "Retail", "state": "Johor"}),
        "SYN-000101": ((0.6, 0.8), {"companyName": "Tie B", "industry": "Food", "state": "Perak"}),
        "SYN-000104": ((0.6, 0.8), {"companyName": "Tie A", "industry": "Food", "state": "Perak"}),
        "SYN-000102": ((0.0, 1.0), {}),
        "SYN-000103": ((-1.0, 0.0), {"companyName": "Opposite", "industry": "Other", "state": "Sabah"}),
    }
    await seed_company(database, source_id, *source_vector, {"companyName": "Source"})
    for registration_number, (vector, data) in candidates.items():
        await seed_company(database, registration_number, *vector, data)

    app = create_app(database=database)
    response = await get_json(app, f"/companies/{source_id}/similar")
    body = response.json()
    expected_order = ["SYN-000105", "SYN-000101", "SYN-000104", "SYN-000102"]
    expected_scores = {
        registration_number: cosine_similarity(source_vector, vector)
        for registration_number, (vector, _) in candidates.items()
    }

    assert response.status_code == 200
    assert [item["registration_number"] for item in body["recommendations"]] == expected_order
    assert source_id not in {item["registration_number"] for item in body["recommendations"]}
    for item in body["recommendations"]:
        assert item["similarity"] == pytest.approx(expected_scores[item["registration_number"]], abs=1e-6)
    incomplete = body["recommendations"][-1]
    assert incomplete == {
        "registration_number": "SYN-000102",
        "name": "SYN-000102",
        "industry": None,
        "state": None,
        "similarity": pytest.approx(0.0, abs=1e-6),
    }

    limited = await get_json(app, f"/companies/{source_id}/similar?limit=2")
    assert [item["registration_number"] for item in limited.json()["recommendations"]] == expected_order[:2]


@pytest.mark.asyncio
async def test_unknown_source_returns_404(postgres_store):
    database, _, _ = postgres_store
    response = await get_json(create_app(database=database), "/companies/SYN-000909/similar")

    assert response.status_code == 404
    assert response.json() == {"error": {"code": "company_not_found", "message": "Company not found"}}


@pytest.mark.asyncio
async def test_fewer_candidates_and_no_candidates_return_available_results(postgres_store):
    database, _, _ = postgres_store
    await seed_company(database, "SYN-000201", 1.0, 0.0, {"companyName": "Source"})
    await seed_company(database, "SYN-000202", 0.0, 1.0, {})
    app = create_app(database=database)

    one_available = await get_json(app, "/companies/SYN-000201/similar")

    assert one_available.status_code == 200
    assert len(one_available.json()["recommendations"]) == 1
    assert one_available.json()["recommendations"][0]["name"] == "SYN-000202"
    assert one_available.json()["recommendations"][0]["industry"] is None
    assert one_available.json()["recommendations"][0]["state"] is None


@pytest.mark.asyncio
async def test_no_candidates_returns_an_empty_list(postgres_store):
    database, _, _ = postgres_store
    await seed_company(database, "SYN-000203", 1.0, 0.0, {"companyName": "Alone"})

    response = await get_json(create_app(database=database), "/companies/SYN-000203/similar")

    assert response.status_code == 200
    assert response.json() == {"registration_number": "SYN-000203", "recommendations": []}


@pytest.mark.asyncio
async def test_postgres_statement_timeout_returns_503(postgres_store, monkeypatch):
    database, _, _ = postgres_store
    await seed_company(database, "SYN-000301", 1.0, 0.0, {"companyName": "Source"})
    await seed_company(database, "SYN-000302", 0.0, 1.0, {"companyName": "Candidate"})
    monkeypatch.setattr(database_module, "RECOMMENDATION_QUERY_TIMEOUT_MS", 10)
    monkeypatch.setattr(
        database_module,
        "SIMILAR_COMPANIES_SQL",
        """
        SELECT candidate.registration_number,
               COALESCE(candidate.company_name, candidate.registration_number) AS name,
               candidate.industry,
               candidate.state,
               1 - (candidate.embedding <=> source.embedding) AS similarity
        FROM companies AS source
        JOIN companies AS candidate
            ON candidate.registration_number <> source.registration_number
        CROSS JOIN LATERAL (SELECT pg_sleep(0.1)) AS delay
        WHERE source.registration_number = $1
        ORDER BY candidate.embedding <=> source.embedding ASC,
                 candidate.registration_number COLLATE "C" ASC
        LIMIT $2
        """,
    )

    response = await get_json(create_app(database=database), "/companies/SYN-000301/similar")

    assert response.status_code == 503
    assert response.json() == {"error": {"code": "database_unavailable", "message": "Database unavailable"}}


@pytest.mark.asyncio
async def test_repeatable_read_keeps_source_snapshot_consistent_with_concurrent_delete(postgres_store, monkeypatch):
    database, control, schema = postgres_store
    source_id = "SYN-000401"
    candidate_id = "SYN-000402"
    await seed_company(database, source_id, 1.0, 0.0, {"companyName": "Source"})
    await seed_company(database, candidate_id, 1.0, 0.0, {"companyName": "Candidate"})

    lock_key = uuid.uuid4().int & ((1 << 63) - 1)
    await control.execute(f'SET search_path TO "{schema}", public')
    async with database._pool.acquire() as blocker:
        await blocker.fetchval("SELECT pg_advisory_lock($1)", lock_key)
        monkeypatch.setattr(database_module, "RECOMMENDATION_QUERY_TIMEOUT_MS", 10_000)
        monkeypatch.setattr(
            database_module,
            "SIMILAR_COMPANIES_SQL",
            f"""
            WITH advisory_gate AS MATERIALIZED (
                SELECT pg_advisory_xact_lock({lock_key}) AS acquired
            )
            SELECT candidate.registration_number,
                   COALESCE(candidate.company_name, candidate.registration_number) AS name,
                   candidate.industry,
                   candidate.state,
                   1 - (candidate.embedding <=> source.embedding) AS similarity
            FROM advisory_gate
            CROSS JOIN companies AS source
            JOIN companies AS candidate
                ON candidate.registration_number <> source.registration_number
            WHERE source.registration_number = $1
            ORDER BY candidate.embedding <=> source.embedding ASC,
                     candidate.registration_number COLLATE "C" ASC
            LIMIT $2
            """,
        )
        app = create_app(database=database)
        request = asyncio.create_task(get_json(app, f"/companies/{source_id}/similar"))

        try:
            waiting = False
            for _ in range(150):
                waiting = await control.fetchval(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM pg_stat_activity
                        WHERE datname = current_database()
                          AND wait_event_type = 'Lock'
                          AND wait_event = 'advisory'
                          AND query LIKE $1
                    )
                    """,
                    f"%pg_advisory_xact_lock({lock_key})%",
                )
                if waiting:
                    break
                await asyncio.sleep(0.02)
            assert waiting, "recommendation query did not reach the advisory-lock gate"

            await control.execute("DELETE FROM companies WHERE registration_number = $1", source_id)
            await blocker.fetchval("SELECT pg_advisory_unlock($1)", lock_key)
            response = await asyncio.wait_for(request, timeout=5.0)
        finally:
            await blocker.fetchval("SELECT pg_advisory_unlock($1)", lock_key)

    assert response.status_code == 200
    assert [item["registration_number"] for item in response.json()["recommendations"]] == [candidate_id]
