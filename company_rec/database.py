"""Async PostgreSQL persistence with atomic company version ordering."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import asyncpg
from pgvector.asyncpg import register_vector

if TYPE_CHECKING:
    from company_rec.embeddings import GeneratedEmbedding
    from company_rec.events import CompanyEvent

UPSERT_COMPANY_SQL = """
INSERT INTO companies (
    registration_number,
    company_name,
    nature_of_business,
    industry,
    state,
    embedding,
    embedding_source,
    embedding_text,
    embedding_model,
    source_updated_at,
    last_message_id
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
ON CONFLICT (registration_number) DO UPDATE SET
    company_name = EXCLUDED.company_name,
    nature_of_business = EXCLUDED.nature_of_business,
    industry = EXCLUDED.industry,
    state = EXCLUDED.state,
    embedding = EXCLUDED.embedding,
    embedding_source = EXCLUDED.embedding_source,
    embedding_text = EXCLUDED.embedding_text,
    embedding_model = EXCLUDED.embedding_model,
    source_updated_at = EXCLUDED.source_updated_at,
    last_message_id = EXCLUDED.last_message_id
WHERE
    (companies.source_updated_at, companies.last_message_id COLLATE "C")
    <
    (EXCLUDED.source_updated_at, EXCLUDED.last_message_id COLLATE "C")
RETURNING registration_number
"""

INSERT_FAILED_EVENT_SQL = """
INSERT INTO failed_events (message_id, body_sha256, raw_body, reason_code, detail)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT DO NOTHING
"""

RECOMMENDATION_QUERY_TIMEOUT_MS = 2_000
POOL_ACQUIRE_TIMEOUT_SECONDS = 2.0
SOURCE_EXISTS_SQL = "SELECT 1 FROM companies WHERE registration_number = $1"
SIMILAR_COMPANIES_SQL = """
SELECT
    candidate.registration_number,
    COALESCE(candidate.company_name, candidate.registration_number) AS name,
    candidate.industry,
    candidate.state,
    1 - (candidate.embedding <=> source.embedding) AS similarity
FROM companies AS source
JOIN companies AS candidate
    ON candidate.registration_number <> source.registration_number
WHERE source.registration_number = $1
ORDER BY
    candidate.embedding <=> source.embedding ASC,
    candidate.registration_number COLLATE "C" ASC
LIMIT $2
"""


@dataclass(frozen=True, slots=True)
class SimilarCompany:
    registration_number: str
    name: str
    industry: str | None
    state: str | None
    similarity: float


class DatabaseUnavailableError(RuntimeError):
    """A database connection or bounded recommendation query failed."""


class CompanyDatabase:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    @classmethod
    async def connect(cls, dsn: str) -> CompanyDatabase:
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=4, init=register_vector)
        database = cls(pool)
        try:
            await database.validate_schema()
        except Exception:
            await pool.close()
            raise
        return database

    async def validate_schema(self) -> None:
        async with self._pool.acquire() as connection:
            required_objects = await connection.fetchval(
                """
                SELECT
                    to_regclass('companies') IS NOT NULL
                    AND to_regclass('failed_events') IS NOT NULL
                    AND EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector')
                """
            )
            if not required_objects:
                raise RuntimeError("database schema is missing; run `just db-init` first")
            embedding_type = await connection.fetchval(
                """
                SELECT format_type(attribute.atttypid, attribute.atttypmod)
                FROM pg_attribute AS attribute
                WHERE attribute.attrelid = to_regclass('companies')
                    AND attribute.attname = 'embedding'
                    AND NOT attribute.attisdropped
                """
            )
            if embedding_type != "vector(384)":
                raise RuntimeError(f"companies.embedding must be vector(384), got {embedding_type!r}")

    async def upsert_company(self, event: CompanyEvent, embedding: GeneratedEmbedding) -> bool:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    UPSERT_COMPANY_SQL,
                    event.registration_number,
                    event.company_name,
                    event.nature_of_business,
                    event.industry,
                    event.state,
                    list(embedding.values),
                    embedding.source,
                    embedding.text,
                    embedding.model,
                    event.source_updated_at,
                    event.message_id,
                )
        return row is not None

    async def quarantine(
        self,
        raw_body: bytes,
        message_id: str | None,
        reason_code: str,
        detail: str,
    ) -> None:
        body_hash = hashlib.sha256(raw_body).digest()
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    INSERT_FAILED_EVENT_SQL,
                    message_id,
                    body_hash,
                    raw_body,
                    reason_code,
                    detail,
                )

    async def get_similar_companies(
        self,
        registration_number: str,
        limit: int,
    ) -> list[SimilarCompany] | None:
        """Return ranked candidates, or None if the source company is absent."""
        try:
            async with self._pool.acquire(timeout=POOL_ACQUIRE_TIMEOUT_SECONDS) as connection:
                async with connection.transaction(isolation="repeatable_read", readonly=True):
                    await connection.fetchval(
                        "SELECT set_config('statement_timeout', $1, true)",
                        f"{RECOMMENDATION_QUERY_TIMEOUT_MS}ms",
                    )
                    source_exists = await connection.fetchval(SOURCE_EXISTS_SQL, registration_number)
                    if source_exists is None:
                        return None

                    rows = await connection.fetch(SIMILAR_COMPANIES_SQL, registration_number, limit)
                    return [
                        SimilarCompany(
                            registration_number=row["registration_number"],
                            name=row["name"],
                            industry=row["industry"],
                            state=row["state"],
                            similarity=float(row["similarity"]),
                        )
                        for row in rows
                    ]
        except (
            asyncpg.PostgresConnectionError,
            asyncpg.InterfaceError,
            asyncpg.QueryCanceledError,
            OSError,
            TimeoutError,
        ) as error:
            raise DatabaseUnavailableError("recommendation database unavailable") from error

    async def close(self) -> None:
        await self._pool.close()


async def initialize_schema(dsn: str, schema_sql: str | None = None) -> None:
    sql = schema_sql or Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
    connection = await asyncpg.connect(dsn)
    try:
        await connection.execute(sql)
    finally:
        await connection.close()
