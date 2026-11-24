set shell := ["bash", "-euo", "pipefail", "-c"]

default: setup

# Install the producer's dependencies.
setup:
    uv sync --locked

# Start Postgres (with pgvector) and RabbitMQ, and wait until both are healthy.
up:
    docker compose up -d --wait

# Stop everything and delete the volumes.
down:
    docker compose down -v

# Replay the registry feed onto the company-events queue.
publish:
    uv run python -m producer.publish

# Create the consumer's PostgreSQL schema and pgvector extension.
db-init:
    uv run python -m company_rec.init_schema

# Consume company events and persist the latest snapshot for each company.
consume:
    uv run python -m company_rec.consumer

# Serve the similar-company recommendation API locally.
api:
    uv run uvicorn company_rec.api:app --host 127.0.0.1 --port 8000

# Run F1's focused regression and integration coverage.
test:
    uv run pytest
