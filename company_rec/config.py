"""Local service configuration using the assessment's supplied defaults."""

from __future__ import annotations

import os

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/companies")
RABBITMQ_PORT = os.environ.get("RABBITMQ_PORT", "5672")
RABBITMQ_URL = os.environ.get(
    "RABBITMQ_URL",
    f"amqp://guest:guest@localhost:{RABBITMQ_PORT}/",
)
FASTEMBED_CACHE_PATH = os.environ.get("FASTEMBED_CACHE_PATH")
