# Planning sources

Inspected on 2026-11-23. This compact record separates assessment authority, observed inputs, and candidate decisions. It is included because the architect-planner protocol requires planning sources and unresolved gaps. It is not runtime verification.

## Repository authority

| Source | What it establishes |
| --- | --- |
| [README](../../README.md), [ASSESSMENT_VERSION](../../ASSESSMENT_VERSION), [CHANGELOG](../../CHANGELOG.md) | Original OneCredit MLE starter, version 2026.11; consumer and CPU embedding requirement; PostgreSQL/pgvector search; current-company/no-loss portal behavior; final DESIGN, startup README, AI disclosure; optional depth up to one hour within six total. No mandated schema, API, model, or error behavior. |
| [producer/publish.py](../../producer/publish.py) | Publishes each feed line unchanged, in order, to durable `company-events` with no extra arguments; persistent JSON messages; publisher confirms; AMQP `message_id` is SHA-256 of the line's UTF-8 bytes. Every publish repeats the entire feed. IDs are metadata, not event JSON. |
| [data/feed.jsonl](../../data/feed.jsonl) | Actual synthetic input; full read-only profiling below. Preserve this authoritative fixture. |
| [docker-compose.yml](../../docker-compose.yml), [Justfile](../../Justfile) | `pgvector/pgvector:0.8.7-pg16`, `rabbitmq:3.13.7-management`, health checks, localhost ports/overrides; setup/up/publish/down commands. `down` uses `docker compose down -v`. No named persistent volumes or application services are supplied. |
| [pyproject.toml](../../pyproject.toml), [.python-version](../../.python-version), [uv.lock](../../uv.lock) | Python >=3.14,<3.15; uv non-package project; only `aio-pika` is a direct dependency (locked at 10.2.3). No application, tests, schema, formatter/linter/type/CI configuration is supplied. |

The initial tracked tree contains all original starter files needed for planning. No existing canonical planning documents or repository `AGENTS.md` were found. No secret files or environment values were inspected. Runtime installation and service health have not been tested.

## Observed feed and proposed interpretation

Feed SHA-256: `3c479dbc3bf0dd3ec9a10a4a31ab52d5f43471aa5c875dcaf3d182e79962d73b`.

Read-only JSON/field/version profiling found **2,518 lines**, including **68 repeated bodies**. There are 5 malformed JSON lines, 6 JSON `null` bodies, 8 `company-struck-off` events, missing required envelope fields, 3 numeric registration values, and 4 string-valued `data` fields. Normal object bodies use `type`, `registrationNumber`, `updatedAt`, and nested `data`; extra `source`/`ingestedBy` fields occur. Registration strings include lowercase and surrounding-space variants (2,049 distinct raw string IDs collapse to 2,000 valid normalized company IDs).

Under the milestone's proposed interpretation, **2,473 valid upserts cover 2,000 companies**. Their timestamps comprise 2,401 explicit-offset strings, 28 `Z` strings, 14 timezone-free strings, and 30 integer epoch-millisecond values. Valid upserts contain 44 blank/null business descriptions, 120 null industries, and 60 null states. Some descriptions are Malay. Arrival order includes older versions after newer versions. There are no distinct-body equal-timestamp conflicts among accepted events, so that behavior needs an independent test case.

The proposed quarantine classification is **45 deliveries / 40 distinct failure identities**: 5 malformed JSON, 6 non-object JSON, 8 unsupported types, 10 missing/invalid registrations, 9 invalid/missing data objects, and 7 missing timestamps. Classification checks type, registration, data, then time; overlapping defects should not be double-counted. Repeated `null` bodies explain the difference between deliveries and identities. All 2,000 valid company IDs also have an acceptable upsert. These are input-profile expectations under explicit assumptions, not measured ingestion results.

## Candidate decisions and authority gaps

The user's requested starting design selects Python/aio-pika, FastEmbed BGE-small/384 dimensions, FastAPI, Pytest, exact cosine search, two tables, conditional version upserts, quarantine, prefetch 1, the default four-result endpoint, and F1→F2→F3. These are engineering decisions compatible with the assessment, not additional assessment mandates. An async PostgreSQL driver, full-snapshot semantics, API limits/error envelope, metadata fallbacks, and quarantine deduplication are approved as part of the M1 plan.

The assessment does not define timezone-free timestamps, numeric epoch units, struck-off semantics, partial-update semantics, equal-time chronology, a latency target, or recommendation-quality labels. The inspected integers are consistent with epoch milliseconds; default naive values to Malaysia time. Quarantine struck-off events and retain company rows. Use deterministic message-ID ties and snapshot replacement. The user approved these assumptions with M1; record them in the eventual DESIGN. Do not manufacture chronological certainty, relevance labels, elapsed-time history, latency numbers, or completed acceptance evidence.

## External technical references

Primary sources checked for feasibility; they do not prove this project runs:

- [FastEmbed documentation](https://qdrant.github.io/fastembed/) and [maintainer package metadata](https://pypi.org/project/fastembed/): CPU ONNX inference and BGE-small 384-dimensional usage; current metadata includes Python 3.14. Model initialization can download artifacts.
- [ONNX Runtime release files](https://pypi.org/project/onnxruntime/): CPython 3.14 wheels are published for several platforms. This reduces a compatibility concern but does not prove complete dependency resolution or local inference; F1 must verify both.
- [BAAI model card](https://huggingface.co/BAAI/bge-small-en-v1.5): English model identity; mixed-language quality remains unmeasured. Record the actual resolved artifact identity during F1/F3.
- [pgvector v0.8.7](https://github.com/pgvector/pgvector/blob/v0.8.7/README.md): exact search, `<=>` cosine distance, and `1 - distance` similarity. [Python adapter](https://github.com/pgvector/pgvector-python#asyncpg): vector registration with asyncpg.
- [PostgreSQL 16 INSERT](https://www.postgresql.org/docs/16/sql-insert.html): atomic `ON CONFLICT DO UPDATE` and conditional update guard. Application correctness still needs real database tests.
- [RabbitMQ 3.13 acknowledgement guide](https://www.rabbitmq.com/docs/3.13/confirms): manual acknowledgements and requeue on channel/connection loss. [aio-pika work queues](https://docs.aio-pika.com/rabbitmq-tutorial/2-work-queues.html): consumer acknowledgement/prefetch mechanics; inspect exception defaults when implementing.

SDD planning followed `sdd-router` and `architect-planner`, including lifecycle, routing matrix, handoff schema, retention policy, and implementation-quality references. Discovery/planning requires no execution handoff; the plan is approved, and implementation awaits a separate user instruction. No standalone architecture spec or speculative completed DESIGN is needed.
