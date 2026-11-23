"""Create the repeatable F1 PostgreSQL schema."""

from __future__ import annotations

import asyncio

from company_rec.config import DATABASE_URL
from company_rec.database import initialize_schema


async def run() -> None:
    await initialize_schema(DATABASE_URL)
    print("PostgreSQL schema initialized")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
