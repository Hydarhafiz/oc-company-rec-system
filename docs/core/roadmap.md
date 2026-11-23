# Company recommendation system roadmap

Status: **M1 in progress; F1 complete; F2 ready**. Assessment version: **2026.11**. Requirements and inspected evidence: [planning sources](planning-sources.md). Executable contracts: [current milestone](milestone.md).

## Purpose and ownership

Power the OneCredit portal's similar-company cards using the latest valid company information from its registry feed. The assessment requires a RabbitMQ consumer, CPU embeddings stored in PostgreSQL/pgvector, recommendations searched in PostgreSQL, reproducible run instructions, and a final `DESIGN.md`. Schema, model, API, failure policy, and tests are candidate decisions.

Proposed flow:

```text
OneCredit registry producer → company-events → async Python consumer
                                             → CPU FastEmbed → PostgreSQL/pgvector
OneCredit portal → FastAPI → exact cosine-distance query ──────┘
```

OneCredit owns the producer, feed, and durable queue with no extra arguments. Our consumer owns validation, embeddings, conditional writes, quarantine, and acknowledgements. PostgreSQL owns current company state and version ordering. FastAPI owns the portal response and error contract; it reads stored vectors and does not run embedding inference.

Keep one Python application with two process entry points (consumer and API), shared typed validation/persistence code, explicit SQL, and the supplied Compose services. Proposed libraries: existing `aio-pika`, Pydantic, FastEmbed, `asyncpg` with the pgvector Python adapter, FastAPI/Uvicorn, and Pytest with async/API test support. Preserve Python 3.14; dependency resolution and real CPU inference are early feasibility gates.

## Delivery sequence and time limit

There is one active milestone, **M1 — reliable ingestion and recommendation submission**. There are no additional planned milestones or completed features.

| Order | Deliverable | Estimated allocation, including verification/docs |
| --- | --- | --- |
| Planning | Inspect starter/feed and review these contracts | 40 minutes |
| F1 | Database and reliable ingestion | 135 minutes |
| F2 | Recommendation API | 60 minutes |
| F3 | Full replay, recovery evidence, README and final DESIGN | 65 minutes |
| Shared reserve | Unexpected problems; optional quality evaluation only after core completion | 60 minutes maximum |

Core target: **5 hours including planning**, with a **hard six-hour development stop**. These are estimates, not elapsed-time claims. Count all assessment planning, coding, tests, research needed for delivery, and documentation against actual time spent; separately track personal conceptual learning. The README excludes initial infrastructure setup from assessment time, but this plan conservatively budgets development work. Do not reset the clock when implementation starts. At each feature boundary compare actual remaining time; use reserve for core gaps before optional work. Stop at the limit and document unfinished work honestly.

**F3-OPT — Recommendation quality evaluation (time permitting)** stays within F3 and begins only after F1, F2, and required F3 verification/documentation are complete. Use at most the remaining assessment time, capped at one hour, without extending the six-hour overall limit. Sample up to 10 companies covering English descriptions, Malay descriptions, and fallback embeddings; manually score their top-four recommendations (2 = strongly similar business activity, 1 = related, 0 = unrelated). Report strict Precision@4 using only score-2 recommendations as relevant, plus qualitative error examples. Record methodology, observations, and limitations in `DESIGN.md`, identifying this as a small exploratory evaluation that is not statistically representative. Do not switch embedding models or implement new features. If the reserve is needed for required work, skip F3-OPT and document that decision in `DESIGN.md`; it is not a condition of core acceptance. See the [F3-OPT contract](milestone.md#f3-opt--recommendation-quality-evaluation-time-permitting) for metric details.

## Decisions and boundaries

- Use FastEmbed `BAAI/bge-small-en-v1.5`, 384 dimensions, on CPU. Preserve incomplete-description companies through documented fallback text. The feed mixes English and Malay; the English model is a simplicity choice with unmeasured multilingual quality.
- Use exact pgvector cosine distance for 2,000 companies, with deterministic ties and no ANN index. Revisit indexing only after measured performance justifies its recall/complexity trade-off.
- Start with one consumer and prefetch 1. PostgreSQL conditional upserts establish idempotent current state despite at-least-once delivery; this does not promise exactly-once transport.
- Use `companies` and `failed_events`; quarantine invalid/unsupported input before ACK. Do not add a RabbitMQ DLQ or change queue arguments.
- Keep the supplied producer, feed, Compose infrastructure, and assessment identity. Add only necessary application/dependency/test/run support after approval. No portal UI, extra services, Redis, Kafka, Kubernetes, paid APIs, cloud deployment, push, publication, or submission action.
- Defer `DESIGN.md` until F3 can describe implemented choices and measured evidence. Keep these plans concise and update their status as work is verified.

The user approved this milestone and its explicit feed assumptions. F1 is complete; F2 is the next planned feature. F1's evidence and environment limits are recorded in the [milestone](milestone.md).
