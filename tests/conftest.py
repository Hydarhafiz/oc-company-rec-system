from __future__ import annotations

import os
import uuid
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from pgvector.asyncpg import register_vector

from company_rec.database import CompanyDatabase

DATABASE_URL = os.environ.get("COMPANY_TEST_DATABASE_URL")
SCHEMA_SQL = Path(__file__).parents[1] / "company_rec" / "schema.sql"


@pytest_asyncio.fixture
async def postgres_store():
    if not DATABASE_URL:
        pytest.skip("set COMPANY_TEST_DATABASE_URL to run PostgreSQL/pgvector integration tests")

    try:
        control = await asyncpg.connect(DATABASE_URL)
    except (OSError, asyncpg.exceptions.PostgresConnectionError) as exc:
        pytest.skip(f"PostgreSQL test database is unreachable: {exc}")

    schema = f"company_rec_test_{uuid.uuid4().hex}"
    pool = None
    schema_created = False
    try:
        extension_schema = await control.fetchval(
            "SELECT nspname FROM pg_extension JOIN pg_namespace ON extnamespace = pg_namespace.oid WHERE extname = 'vector'"
        )
        if extension_schema is None:
            await control.execute("CREATE EXTENSION vector WITH SCHEMA public")
        elif extension_schema != "public":
            raise RuntimeError("pgvector must be installed in public for asyncpg codec registration")
        await register_vector(control)

        await control.execute(f'CREATE SCHEMA "{schema}"')
        schema_created = True
        await control.execute(f'SET search_path TO "{schema}", public')
        await control.execute(SCHEMA_SQL.read_text(encoding="utf-8"))

        async def initialize(connection):
            await register_vector(connection)

        async def setup(connection):
            await connection.execute(f'SET search_path TO "{schema}", public')

        pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=4,
            init=initialize,
            setup=setup,
        )
        database = CompanyDatabase(pool)
        await database.validate_schema()
        yield database, control, schema
    finally:
        if pool is not None:
            await pool.close()
        if schema_created:
            await control.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await control.close()
