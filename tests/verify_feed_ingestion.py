"""Compare a completed live feed replay to an independent feed oracle."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
from collections import Counter
from pathlib import Path

import asyncpg
from pgvector.asyncpg import register_vector

from tests.feed_oracle import ExpectedCompany, ExpectedFailure, oracle_from_path

ROOT = Path(__file__).parents[1]
FEED_PATH = ROOT / "data" / "feed.jsonl"
EXPECTED_MODEL = (
    "BAAI/bge-small-en-v1.5 via Qdrant/bge-small-en-v1.5-onnx-Q@"
    "aa8f8b060edb00e03bfdd08813a2949946c8ba55 (fastembed 0.9.1)"
)


def _validate_company(row: asyncpg.Record, expected: ExpectedCompany) -> None:
    for field in (
        "company_name",
        "nature_of_business",
        "industry",
        "state",
        "embedding_source",
        "embedding_text",
        "last_message_id",
    ):
        actual_value = row[field]
        expected_value = getattr(expected, field)
        if actual_value != expected_value:
            raise AssertionError(
                f"{expected.registration_number} {field}: expected {expected_value!r}, got {actual_value!r}"
            )
    if row["source_updated_at"] != expected.source_updated_at:
        raise AssertionError(
            f"{expected.registration_number} source_updated_at: "
            f"expected {expected.source_updated_at!r}, got {row['source_updated_at']!r}"
        )
    if row["embedding_model"] != EXPECTED_MODEL:
        raise AssertionError(
            f"{expected.registration_number} unexpected embedding model {row['embedding_model']!r}"
        )

    vector = row["embedding"].to_list()
    if len(vector) != 384 or not all(math.isfinite(value) for value in vector) or not any(vector):
        raise AssertionError(f"{expected.registration_number} has an invalid 384-dimensional embedding")


def _validate_failure(row: asyncpg.Record, expected: ExpectedFailure) -> None:
    if row["message_id"] != expected.message_id:
        raise AssertionError(f"unexpected failure message id {row['message_id']!r}")
    if row["body_sha256"] != expected.body_sha256:
        raise AssertionError(f"unexpected failure body hash for {expected.message_id}")
    if row["raw_body"] != expected.raw_body:
        raise AssertionError(f"raw quarantined body changed for {expected.message_id}")
    if row["reason_code"] != expected.reason_code:
        raise AssertionError(
            f"{expected.message_id} failure reason: expected {expected.reason_code!r}, got {row['reason_code']!r}"
        )


async def verify_database_state(dsn: str) -> dict[str, object]:
    oracle = oracle_from_path(FEED_PATH)
    connection = await asyncpg.connect(dsn)
    try:
        await register_vector(connection)
        company_rows = await connection.fetch(
            """
            SELECT registration_number, company_name, nature_of_business, industry, state,
                   embedding, embedding_source, embedding_text, embedding_model,
                   source_updated_at, last_message_id
            FROM companies ORDER BY registration_number COLLATE "C"
            """
        )
        company_by_id = {row["registration_number"]: row for row in company_rows}
        if set(company_by_id) != set(oracle.companies):
            missing = sorted(set(oracle.companies) - set(company_by_id))
            extra = sorted(set(company_by_id) - set(oracle.companies))
            raise AssertionError(f"company ids differ: missing={missing[:5]}, extra={extra[:5]}")
        for registration_number, expected in oracle.companies.items():
            _validate_company(company_by_id[registration_number], expected)

        failure_rows = await connection.fetch(
            "SELECT message_id, body_sha256, raw_body, reason_code FROM failed_events"
        )
        failure_by_id = {(row["message_id"], row["body_sha256"]): row for row in failure_rows}
        if set(failure_by_id) != set(oracle.failures):
            missing = len(set(oracle.failures) - set(failure_by_id))
            extra = len(set(failure_by_id) - set(oracle.failures))
            raise AssertionError(f"quarantine identities differ: missing={missing}, extra={extra}")
        actual_reason_counts: Counter[str] = Counter()
        for failure_identity, expected in oracle.failures.items():
            row = failure_by_id[failure_identity]
            _validate_failure(row, expected)
            actual_reason_counts[row["reason_code"]] += 1
        expected_identity_reason_counts = Counter(
            failure.reason_code for failure in oracle.failures.values()
        )
        if actual_reason_counts != expected_identity_reason_counts:
            raise AssertionError(
                "quarantine identity reason counts differ: "
                f"expected={expected_identity_reason_counts}, got={actual_reason_counts}"
            )

        return {
            "feed_sha256": hashlib.sha256(FEED_PATH.read_bytes()).hexdigest(),
            "deliveries": oracle.deliveries,
            "accepted_upsert_deliveries": oracle.accepted_deliveries,
            "unique_companies": len(company_rows),
            "quarantined_deliveries": sum(oracle.failure_counts.values()),
            "unique_failure_identities": len(failure_rows),
            "failure_delivery_reason_counts": dict(sorted(oracle.failure_counts.items())),
            "failure_identity_reason_counts": dict(sorted(actual_reason_counts.items())),
            "embedding_model": EXPECTED_MODEL,
            "all_company_snapshots_and_embeddings_match": True,
        }
    finally:
        await connection.close()


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default="postgresql://postgres:postgres@localhost:5432/companies",
        help="application database DSN (defaults to the supplied local Compose service)",
    )
    args = parser.parse_args()
    result = await verify_database_state(args.dsn)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
