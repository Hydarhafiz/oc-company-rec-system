# M1 — Reliable ingestion and recommendation submission

Status: **M1 complete; F1, F2, and F3 complete**. The unchanged feed was ingested and independently verified, replay and recovery behavior were exercised against the local broker and database, the live API and local latency were measured, and submission documentation is complete. The final full suite passed 66 tests with no skips. See the [roadmap](roadmap.md) for the concise completed summary and [planning sources](planning-sources.md) for assessment authority and feed observations. No next milestone is approved.

## Shared contracts

### Incoming events and normalization

- Consume the existing `company-events` queue with manual acknowledgements, one consumer, and prefetch 1. Access it without changing its durable/no-arguments definition; do not modify the upstream producer/feed or configure a DLQ.
- Require a JSON object with `type="company-upserted"`, string `registrationNumber`, supported `updatedAt`, and an object `data`. Read `message_id` from AMQP metadata, not JSON. Require a nonempty string message ID; missing IDs are quarantined. Ignore extra envelope/data fields such as `source` and `ingestedBy`.
- Normalize registration by trimming surrounding whitespace and uppercasing, then require the assessment format `SYN-[0-9]{6}`. Do not coerce numeric IDs or remove internal punctuation/whitespace. Use the same rule in the API; this synthetic-ID policy would need revision for real registry IDs.
- Support ISO datetime strings with an explicit offset or `Z`, timezone-free ISO datetimes, and integer Unix epoch **milliseconds** (reject booleans and implicit seconds/string-number coercion). Convert to aware UTC and store as `timestamptz`. **Assumption:** timezone-free values mean `Asia/Kuala_Lumpur` (UTC+08:00), inferred from the feed's Malaysian context and surrounding explicit offsets. Missing/unparseable/out-of-range times are quarantined; never substitute arrival time.
- `data` contains `companyName`, `natureOfBusiness`, `industry`, and `state`. Treat absent/null/blank metadata strings as unavailable; reject non-string non-null metadata. Preserve a valid identifiable company even when descriptions or other metadata are incomplete. Trim available strings, preserving source display casing. For absent company names, use the normalized registration number as the response display name.
- Treat accepted upserts as complete snapshots: a newer event replaces current metadata, including missing/null descriptions, and regenerates a consistent embedding. Do not silently merge older descriptions into the latest snapshot.
- **Assumption:** quarantine unsupported types, including the eight `company-struck-off` events, without deleting or deactivating companies. Their business semantics are unspecified, and retaining all 2,000 companies follows the requested no-loss target. Preserve the raw events for investigation; deletion/status support is outside M1.

### Database, embeddings, and durable processing

| Table | Planned fields and invariants |
| --- | --- |
| `companies` | Normalized `registration_number` text primary key; nullable name, description, industry, state; non-null `embedding vector(384)`, `embedding_source`, exact `embedding_text`, model identity; non-null `source_updated_at timestamptz` and `last_message_id` text with deterministic bytewise ordering. One current row per company; metadata/vector/version change atomically. |
| `failed_events` | Generated primary key; nullable AMQP message ID; raw body as bytes (malformed JSON need not be valid JSON/UTF-8); body SHA-256; reason code/detail; first-received UTC timestamp. Unique failure identity from message ID (including absence) plus body hash makes quarantine redelivery/replay idempotent. No foreign key requiring a valid company. |

Enable the vector extension explicitly with a repeatable schema-init command. Use plain SQL and an async connection pool; no ORM or migration framework is needed for the initial schema. Validate schema/vector dimensions at startup. This is new local persistence, not a migration of existing user data.

Choose the first nonblank embedding input: `natureOfBusiness` → `industry` → `companyName` → fixed text `Company business activity unspecified.` Store the selected source category (`nature_of_business`, `industry`, `company_name`, or `generic`) and actual text. Use the same document-embedding path for every company; recommendations compare stored company vectors without a query-specific prompt. Initialize the CPU model once; move synchronous inference off the asyncio event loop so AMQP heartbeats continue. Require 384 finite, nonzero values before persistence. Lock dependencies and document the resolved model artifact/revision during implementation; do not mix model versions silently.

Within a transaction, insert or update only when incoming `(source_updated_at, message_id)` is lexicographically greater than stored `(source_updated_at, last_message_id)`. Enforce this in PostgreSQL `ON CONFLICT ... DO UPDATE ... WHERE`, with bytewise (`C`) message-ID collation. All metadata and the vector come from the winning snapshot. An optional early stale check cannot replace this atomic guard. Equal versions and older events are successful no-ops. This protects against races and late deliveries without an unbounded processed-ID table. Equal-time message-ID ordering provides reproducibility, **not true chronology**; the producer's ID is a body hash. Distinct-body equal-time conflicts are absent in this fixture and require a separate regression scenario.

| Processing outcome | Required action |
| --- | --- |
| Valid insert/update or duplicate/stale no-op | Commit transaction, then ACK on the delivery's channel. |
| Invalid JSON/envelope/fields, missing message ID, unsupported type, or proven event-specific unprocessable input | Commit idempotent quarantine record, then ACK. No partial company write. |
| Database/broker outage, failed/uncertain commit, unavailable model/runtime, or unexpected programming error | Roll back when possible; do not ACK or classify infrastructure/programming errors as bad data. Close/reconnect the channel to return outstanding deliveries, with backoff to avoid a tight retry loop. |
| Crash or lost ACK after commit | Redelivery is safe through conditional company writes or quarantine deduplication. |

If quarantine storage fails, preserve the unacknowledged delivery. Graceful shutdown stops taking work and commits+ACKs completed work or closes with the delivery outstanding. Do not use a context-manager default that rejects and discards exceptions. No broker/database distributed transaction is claimed. Compose restarts must not intentionally erase data; `just down` is an explicit destructive reset. Supplied Compose has no named persistent volumes, so container replacement/reset durability is a documented infrastructure limitation.

### Portal API

`GET /companies/{registration_number}/similar?limit=4` serves the similar-company cards, not a general company CRUD interface. Validate `limit` as an integer from 1 through 4. Return `200` with `{ "registration_number": "SYN-001550", "recommendations": [...] }`. Each recommendation has `registration_number` (the company ID), `name`, nullable `industry`, nullable `state`, and `similarity` equal to `1 - cosine_distance` (a score, not a probability).

Look up the normalized source and rank all other companies in PostgreSQL by `embedding <=> source.embedding ASC`, then `registration_number` bytewise ascending. Exclude the source in SQL before applying the limit. Do not filter by industry/state or description completeness. Use one consistent database snapshot for source lookup and candidates, parameterized SQL, and bounded query timeouts. No runtime embedding generation occurs in the API.

| Situation | Planned response |
| --- | --- |
| Existing source, fewer than requested candidates | `200`; return available candidates, including `[]` if none. |
| Valid normalized ID absent from database | `404`, error code `company_not_found`. |
| Invalid identifier or invalid/out-of-range limit | `422`, error code `invalid_request`. |
| Database unavailable or query timeout | `503`, error code `database_unavailable`; do not turn this into an empty result. |
| Unexpected application error | `500`, error code `internal_error`; log details locally. |

Use a consistent error envelope `{ "error": { "code": "...", "message": "..." } }`; do not expose SQL, credentials, or tracebacks. Source lookup and ranking share a snapshot so concurrent ingestion does not create inconsistent missing-source behavior. Startup/shutdown owns the API pool lifecycle. No auth/cloud deployment is admitted for this localhost assessment.

## Feature contracts and progress

F1, F2, and F3 acceptance evidence is recorded below. Feature tests alone do not establish full broker replay, replay performance, or subjective recommendation quality; F3 adds and records that end-to-end evidence.

### F1 — Database and reliable ingestion

- **Type/status/route:** `build`; complete; `builder-executor -> tdd-loop`.
- **Outcome/scope:** repeatable two-table schema; typed Pydantic validation/normalization; CPU embedding generation; async database adapter and atomic conditional upsert; reliable RabbitMQ consumer; ingestion regressions. Admit necessary dependencies and lockfile/run-command support. Exclude API, evaluation, upstream edits, ANN, and extra services.
- **Entry gate:** satisfied. Python 3.14 dependency resolution, model download/cache, a real 384-dimensional CPU embedding, SQL contracts, and queue contracts were verified before implementation proceeded.
- **F1-01:** actual PostgreSQL/pgvector tests prove one row per normalized ID, atomic metadata/vector/version updates, duplicate/stale no-ops, equivalent timestamp instants, and deterministic equal-time winners in both arrival orders. Include competing transactions so an application-side read cannot mask a missing SQL guard.
- **F1-02:** validation covers the real nested feed, all four timestamp forms, padded/lowercase IDs, null/missing/blank descriptions and metadata, numeric IDs, malformed JSON/non-object bodies, unsupported events, and ignored extra fields. All fallback levels preserve company eligibility; real-model smoke proves finite nonzero 384-dimensional output. Lightweight deterministic vectors may isolate unit/SQL tests, but cannot replace this smoke or final replay.
- **F1-03:** transaction/broker failure scenarios prove rollback, no ACK before commit, durable quarantine before ACK, no ACK if quarantine fails, and safe redelivery after commit-before-ACK. Demonstrate a failed transaction can be followed by successful processing without corrupting the connection/row.
- **Exit/evidence:** satisfied. The full F1/F2 regression run passed **65 tests with 0 skips** against the isolated `companies_test` database on PostgreSQL 16.16/pgvector 0.8.7; it includes actual PostgreSQL tests, a full read-only parse of all 2,518 feed lines, and real BGE-small CPU inference. A live RabbitMQ 3.13.7 smoke inserted one unique company, acknowledged the message, then confirmed identical and older messages left the stored snapshot unchanged. The embedding artifact was `Qdrant/bge-small-en-v1.5-onnx-Q@aa8f8b060edb00e03bfdd08813a2949946c8ba55`; output was finite, nonzero, and 384-dimensional. Full broker replay and outage/restart recovery remain F3 acceptance.

### F2 — Recommendation API

- **Type/status/route:** `build`; complete; `builder-executor -> tdd-loop`.
- **Outcome/scope:** FastAPI entry point, pool lifecycle, exact pgvector ranking and response/error contracts above, and API tests. Exclude portal UI, auth, alternate ranking heuristics, ANN, and quality claims.
- **Entry gate:** satisfied. F1's schema, model, ingestion, and live broker smoke are verified; the similar-company portal capability and response/error contract were approved in M1.
- **F2-01:** verified by actual pgvector tests using controlled 384-dimensional vectors and independently calculated cosine scores, including deterministic ties, ordering, limits, self-exclusion, and incomplete metadata.
- **F2-02:** verified through API tests for normalization, `200` with fewer/zero candidates, `404`, validation `422`, database connection/query-timeout `503`, sanitized `500`, and a concurrent source change under repeatable-read isolation.
- **Exit/evidence:** satisfied by 16 F2 tests within the full 65-test run (0 skips) against the isolated PostgreSQL 16.16/pgvector 0.8.7 database. The FastAPI app has a lifespan-managed pool, bounded acquire/query timeouts, and the `just api` local launch command. HTTP behavior was exercised through the ASGI client; a separate Uvicorn port smoke was not required. The application database may still have fewer than 2,000 companies until F3 replay.

### F3 — End-to-end verification and submission preparation

- **Type/status/route:** `verification-only`; complete; `tdd-loop`. No application defect requiring a build or bugfix route was found.
- **Outcome/scope:** completed producer-to-database-to-API verification with the unchanged feed and model, durable independent verification/recovery scripts, README and `DESIGN.md` evidence, and truthful M1/roadmap status. No publish, push, deployment, or assessment submission was performed.
- **Entry gate:** satisfied. F1/F2 evidence and the user's current time record were checked; the existing local database and queue were inspected. Full-feed replay was authorized. Recovery scenarios used reserved IDs and one unique malformed-message identity and removed only those test records.
- **F3-01:** the original publisher delivered **2,518** messages: **2,473 accepted upsert deliveries** and **45 quarantined deliveries**, represented by **40 failure identities**. The independent `tests/feed_oracle.py` and read-only database verifier matched all 2,000 company IDs, winning timestamps/message IDs, metadata, embedding inputs/model, and raw quarantine bodies/reasons. Every stored vector was finite, nonzero, and 384-dimensional. RabbitMQ ended with 0 ready and 0 unacknowledged messages. Feed SHA-256: `3c479dbc3bf0dd3ec9a10a4a31ab52d5f43471aa5c875dcaf3d182e79962d73b`.
- **F3-02:** a second unchanged replay preserved counts and a digest over every company column (including vector text) and quarantine row: `9ddf7e0d57fbf615c674c0afa2c9ae5a12bdf71fba6adb8edf62bfd21856ee7b` before and after. RabbitMQ recorded 5,036 cumulative publishes, deliveries, and ACKs across the two full replays, with no redeliveries and zero ready/unacknowledged messages. The retained second-replay duration was unavailable because its monitor could observe existing DB counts before broker publish confirmation; the digest is the idempotency evidence. The live broker harness passed four isolated scenarios: company transaction failure before commit and retry; quarantine transaction failure and retry; process exit after DB commit but before ACK and successful redelivery; and database unavailable at consumer startup with the event retained until healthy restart. The running PostgreSQL container itself was not interrupted. The recovery harness cleaned only its test rows and left the feed dataset and queue intact.
- **F3-03:** five known source IDs were called through the running local FastAPI service, each returning four recommendations. Results excluded the source and matched PostgreSQL metadata, direct pgvector order, and similarity values; controlled-vector tests independently cover cosine ranking and tie ordering. Invalid ID/limit returned `422`, unknown company `404`, and a controlled unavailable-store instance returned sanitized `503`; the real PostgreSQL service was not stopped. Full-feed publish-and-drain took **37.656 seconds** (publisher call **6.554 seconds**), with consumer/model warm and empty application DB. For 100 sequential warmed loopback requests, p50 was **2.996 ms**, p95 **3.707 ms**; hardware and measurement conditions are documented in `DESIGN.md`. No cold-start or production-load claim is made.
- **F3-04:** README covers dependencies, local services, schema init, consumer/publisher/API commands, example response, tests, configuration/model cache, troubleshooting, and destructive reset warning. `DESIGN.md` covers architecture, data contracts, model/fallback behavior, ACK and recovery, measured evidence, trade-offs, remaining risks, 10x options, and Codex/SDD use.
- **Exit/evidence:** final `just test` passed **66 tests, 0 skipped**; the independent feed verifier and Python syntax/whitespace checks passed. Durable oracle, database verifier, broker recovery runner, and isolated consumer were retained in `tests/`; generated reports and model files remain outside Git. One detailed F3 feature commit closes M1.

#### F3-OPT — Recommendation quality evaluation (time permitting)

- **Type/status:** optional manual evaluation; complete after core F3 verification, within the six-hour assessment budget.
- **Sample and judgments:** the developer reviewed three source companies and their top-four results. English cafe source: `SYN-001550` scored `2,2,2,0`; Malay restaurant source: `SYN-000002` scored `2,2,2,2`; fallback source `SYN-000014` was skipped because business descriptions are absent and all candidates share only the generic `Hospitality` fallback. No labels were invented for that case.
- **Metric and interpretation:** strict Precision@4 is `3/4 = 0.75` for the English-source case and `4/4 = 1.00` for the Malay-source case; combined across the eight judged recommendations it is `7/8 = 0.875`. The fallback case is excluded from the judged denominator and reported as a metadata limitation. The English-source review contains two manually judged cross-language semantic matches; the Malay-source case contains four exact-description matches. The two-source/eight-recommendation sample is small and must not be generalized to the full dataset or interpreted as a broad multilingual-quality estimate. Full IDs, methodology, and notes are in `DESIGN.md`.

## Execution, retention, and review gates

Keep production responsibilities easy to trace: delivery orchestration → typed event transformation → CPU embedding → transactional persistence; API request → validated ID → database ranking → response. Avoid generic service frameworks. No formatter, linter, type checker, or CI policy is configured in the starter; ignore patterns alone do not establish one. Planned verification command is `uv run pytest` (focused capability suites first, full suite at F3). Do not silently add unrelated quality tooling; admit only dependencies/configuration necessary for the feature's implementation and tests.

The user approved M1 and authorized F1, F2, and F3. The ignored `.sdd/` handoff is removed after the F3 feature commit; detailed command output and temporary state stay out of canonical docs.

Promote durable regressions to capability-oriented tests, keep the authoritative feed unchanged, and retain only independent small fixtures needed for cases the feed lacks. Purge scratch scenarios/reports at feature completion; generated artifacts and model caches stay ignored. Include planning changes in the appropriate completed feature boundary, or one standalone planning commit only after approval. Each feature has one detailed commit with truthful verification and residual risks. Preserve the full completed M1 plan through the final feature commit; keep only a concise completed summary in the roadmap when later reusing this milestone file.

There is no existing application path to migrate or remove. F1 review verified SQL atomicity, queue compatibility, timestamp assumptions, and failure handling against its feature contract. F2 adds the single supported card-ranking endpoint; no parallel recommendation path or extra API surface remains. F3 full-feed, replay, recovery, API, performance, and submission evidence is complete with the limits stated above. Plan review is complete, including naive-time and struck-off semantics. M1 is closed; stop here for the user's review. No following milestone has been approved.
