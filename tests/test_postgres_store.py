from __future__ import annotations

import asyncio
import os
import uuid
from pathlib import Path

import asyncpg
import pytest
import pytest_asyncio
from pgvector.asyncpg import register_vector

from company_rec.database import CompanyDatabase
from tests.helpers import embedding, event, feed_body

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

    schema = f"f1_test_{uuid.uuid4().hex}"
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


@pytest.mark.asyncio
async def test_schema_is_repeatable_and_uses_real_pgvector(postgres_store):
    _, connection, _ = postgres_store

    await connection.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
    await connection.execute(SCHEMA_SQL.read_text(encoding="utf-8"))
    table_count = await connection.fetchval(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = current_schema() AND table_name IN ('companies', 'failed_events')"
    )
    embedding_type = await connection.fetchval(
        """
        SELECT format_type(attribute.atttypid, attribute.atttypmod)
        FROM pg_attribute AS attribute
        WHERE attribute.attrelid = to_regclass('companies')
            AND attribute.attname = 'embedding'
            AND NOT attribute.attisdropped
        """
    )

    assert table_count == 2
    assert embedding_type == "vector(384)"


@pytest.mark.parametrize("arrival_order", [("a", "z"), ("z", "a")])
@pytest.mark.asyncio
async def test_equal_timestamp_message_id_order_wins_in_both_arrival_orders(postgres_store, arrival_order):
    database, connection, _ = postgres_store
    events = {
        "a": event(
            registration_number="SYN-000111",
            updated_at="2026-08-14T01:00:00Z",
            message_id="a",
            data={"companyName": "Older tie winner", "natureOfBusiness": "A", "industry": "Old", "state": "Johor"},
        ),
        "z": event(
            registration_number="SYN-000111",
            updated_at="2026-08-14T09:00:00+08:00",
            message_id="z",
            data={"companyName": "Deterministic winner", "natureOfBusiness": "Z", "industry": "New", "state": "Selangor"},
        ),
    }

    for message_id in arrival_order:
        await database.upsert_company(events[message_id], embedding(float(ord(message_id)), text=message_id))
    row = await connection.fetchrow("SELECT * FROM companies WHERE registration_number = 'SYN-000111'")

    assert row["company_name"] == "Deterministic winner"
    assert row["industry"] == "New"
    assert row["state"] == "Selangor"
    assert row["last_message_id"] == "z"
    assert row["embedding_text"] == "z"
    assert row["embedding"].to_list()[0] == float(ord("z"))
    assert await connection.fetchval("SELECT count(*) FROM companies WHERE registration_number = 'SYN-000111'") == 1


@pytest.mark.asyncio
async def test_newer_snapshot_replaces_metadata_and_duplicate_or_stale_events_are_noops(postgres_store):
    database, connection, _ = postgres_store
    first = event(
        registration_number="SYN-000222",
        updated_at="2026-08-14T09:00:00+08:00",
        message_id="same-id",
        data={"companyName": "Current", "natureOfBusiness": "Bakery", "industry": "Food", "state": "Selangor"},
    )
    newer = event(
        registration_number="SYN-000222",
        updated_at="2026-08-15T09:00:00+08:00",
        message_id="newer-id",
        data={"companyName": "Current renamed", "industry": "Retail"},
    )
    stale = event(
        registration_number="SYN-000222",
        updated_at="2026-08-13T09:00:00+08:00",
        message_id="zzzz",
        data={"companyName": "Stale", "natureOfBusiness": "Old activity", "industry": "Old", "state": "Perak"},
    )

    assert await database.upsert_company(first, embedding(1.0, text="Bakery"))
    assert not await database.upsert_company(first, embedding(1.0, text="Bakery"))
    assert await database.upsert_company(newer, embedding(2.0, source="industry", text="Retail"))
    assert not await database.upsert_company(stale, embedding(3.0, text="Old activity"))
    row = await connection.fetchrow("SELECT * FROM companies WHERE registration_number = 'SYN-000222'")

    assert row["company_name"] == "Current renamed"
    assert row["nature_of_business"] is None
    assert row["industry"] == "Retail"
    assert row["state"] is None
    assert row["embedding_source"] == "industry"
    assert row["embedding_text"] == "Retail"
    assert row["source_updated_at"].isoformat() == "2026-08-15T01:00:00+00:00"
    assert row["last_message_id"] == "newer-id"
    assert row["embedding"].to_list()[0] == 2.0
    assert await connection.fetchval("SELECT count(*) FROM companies WHERE registration_number = 'SYN-000222'") == 1


@pytest.mark.asyncio
async def test_concurrent_upserts_leave_the_greatest_source_version(postgres_store):
    database, connection, _ = postgres_store
    older = event(
        registration_number="SYN-000333",
        updated_at="2026-08-14T09:00:00+08:00",
        message_id="zzzz",
        data={"companyName": "Older", "natureOfBusiness": "Old", "industry": "Old", "state": "Perak"},
    )
    newer = event(
        registration_number="SYN-000333",
        updated_at="2026-08-15T09:00:00+08:00",
        message_id="a",
        data={"companyName": "Newer", "natureOfBusiness": "New", "industry": "New", "state": "Selangor"},
    )
    ready_count = 0
    both_ready = asyncio.Event()

    async def write(company_event, generated):
        nonlocal ready_count
        ready_count += 1
        if ready_count == 2:
            both_ready.set()
        await both_ready.wait()
        return await database.upsert_company(company_event, generated)

    await asyncio.gather(
        write(older, embedding(1.0, text="Old")),
        write(newer, embedding(2.0, text="New")),
    )
    row = await connection.fetchrow("SELECT * FROM companies WHERE registration_number = 'SYN-000333'")

    assert row["company_name"] == "Newer"
    assert row["last_message_id"] == "a"
    assert row["embedding_text"] == "New"


@pytest.mark.asyncio
async def test_failed_company_transaction_rolls_back_and_pool_remains_usable(postgres_store):
    database, connection, schema = postgres_store
    initial = event(
        registration_number="SYN-000444",
        updated_at="2026-08-14T09:00:00+08:00",
        message_id="initial",
        data={"companyName": "Stable", "natureOfBusiness": "Stable activity", "industry": "Food", "state": "Johor"},
    )
    failing = event(
        registration_number="SYN-000444",
        updated_at="2026-08-15T09:00:00+08:00",
        message_id="failing",
        data={"companyName": "FAIL", "natureOfBusiness": "Different", "industry": "Other", "state": "Perak"},
    )
    await database.upsert_company(initial, embedding(1.0, text="Stable activity"))
    await connection.execute(
        f"""
        CREATE FUNCTION "{schema}".reject_failing_company() RETURNS trigger AS $$
        BEGIN
            IF NEW.company_name = 'FAIL' THEN
                RAISE EXCEPTION 'test transaction failure';
            END IF;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql
        """
    )
    await connection.execute(
        f"CREATE TRIGGER reject_failing_company BEFORE INSERT OR UPDATE ON companies "
        f"FOR EACH ROW EXECUTE FUNCTION \"{schema}\".reject_failing_company()"
    )

    with pytest.raises(asyncpg.PostgresError, match="test transaction failure"):
        await database.upsert_company(failing, embedding(9.0, text="Different"))

    row = await connection.fetchrow("SELECT * FROM companies WHERE registration_number = 'SYN-000444'")
    assert row["company_name"] == "Stable"
    assert row["industry"] == "Food"
    assert row["last_message_id"] == "initial"
    assert row["embedding_text"] == "Stable activity"
    await connection.execute("DROP TRIGGER reject_failing_company ON companies")
    assert await database.upsert_company(failing, embedding(9.0, text="Different"))


@pytest.mark.asyncio
async def test_quarantine_preserves_raw_bytes_and_deduplicates_redelivery(postgres_store):
    database, connection, _ = postgres_store
    raw_body = b"\xff{not-json"

    await database.quarantine(raw_body, "bad-message", "invalid_json", "not valid JSON")
    await database.quarantine(raw_body, "bad-message", "invalid_json", "not valid JSON")
    row = await connection.fetchrow("SELECT * FROM failed_events")

    assert row["message_id"] == "bad-message"
    assert row["raw_body"] == raw_body
    assert len(row["body_sha256"]) == 32
    assert row["reason_code"] == "invalid_json"
    assert await connection.fetchval("SELECT count(*) FROM failed_events") == 1


@pytest.mark.asyncio
async def test_missing_and_empty_message_id_failures_have_distinct_identities(postgres_store):
    database, connection, _ = postgres_store
    raw_body = feed_body()

    await database.quarantine(raw_body, None, "missing_message_id", "missing")
    await database.quarantine(raw_body, "", "missing_message_id", "empty")

    assert await connection.fetchval("SELECT count(*) FROM failed_events") == 2
