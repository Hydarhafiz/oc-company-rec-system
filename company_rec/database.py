"""Async PostgreSQL persistence with atomic company version ordering."""

from __future__ import annotations

import hashlib
from pathlib import Path

import asyncpg
from pgvector.asyncpg import register_vector

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

    async def close(self) -> None:
        await self._pool.close()


async def initialize_schema(dsn: str, schema_sql: str | None = None) -> None:
    sql = schema_sql or Path(__file__).with_name("schema.sql").read_text(encoding="utf-8")
    connection = await asyncpg.connect(dsn)
    try:
        await connection.execute(sql)
    finally:
        await connection.close()
