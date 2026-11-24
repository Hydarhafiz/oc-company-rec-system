"""Run the real consumer with explicit local endpoints for recovery scenarios."""

from __future__ import annotations

import argparse
import asyncio
import os

import company_rec.consumer as consumer
from company_rec.database import CompanyDatabase
from company_rec.embeddings import BusinessEmbedder


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--rabbitmq-url", required=True)
    parser.add_argument("--cache-dir")
    parser.add_argument("--crash-after-commit-for")
    args = parser.parse_args()

    consumer.DATABASE_URL = args.dsn
    consumer.RABBITMQ_URL = args.rabbitmq_url
    if args.crash_after_commit_for:
        original_upsert = CompanyDatabase.upsert_company

        async def commit_then_exit(database, event, embedding):
            changed = await original_upsert(database, event, embedding)
            if event.registration_number == args.crash_after_commit_for:
                os._exit(86)
            return changed

        CompanyDatabase.upsert_company = commit_then_exit

    cache_dir = args.cache_dir or consumer.FASTEMBED_CACHE_PATH
    embedder = BusinessEmbedder.load(cache_dir, local_files_only=True)
    try:
        asyncio.run(consumer.run(embedder))
    finally:
        embedder.close()


if __name__ == "__main__":
    main()
