# M1 — Reliable ingestion and recommendation submission

Status: **M1 in progress; F1 and F2 complete; F3 ready**. Feature-level ingestion and recommendation behavior are implemented and verified; full-feed replay, recovery, and submission evidence remain in F3. Scope and time allocations: [roadmap](roadmap.md). Assessment authority, feed observations, and external references: [planning sources](planning-sources.md).

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

F1 and F2 acceptance evidence is recorded below. F3 acceptance remains **planned/unverified**. Feature tests do not establish full broker replay, replay performance, or subjective recommendation quality.

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

- **Type/status/route:** `verification-only`; planned, depends on F1/F2; `tdd-loop`. This route admits verification scenarios, measured evidence, and submission documentation; application repairs return to the appropriate build/bug route inside scope.
- **Outcome/scope:** real producer-to-API verification using the unchanged feed/model/services; final README and `DESIGN.md`; documented limits and remaining work. Optional quality evaluation uses only remaining time. No publish/push/submission action and no separate release phase.
- **Entry gate:** F1/F2 verified, remaining assessment time checked. Obtain a clean isolated local assessment database/queue without deleting unrelated user data; resets must be explicit and local.
- **F3-01:** publish all **2,518 deliveries** through the original producer; wait for ready and unacknowledged queue counts to reach zero. Prove **2,000 unique company rows**, valid vectors for every row, and winning metadata/version against an independent fixture oracle using the specified normalization/time/version rules. Expected profile: 2,473 accepted upsert deliveries and 45 quarantined deliveries, represented by 40 unique failure identities. Treat these as assertions to verify against the running system.
- **F3-02:** replay the unchanged feed again; company rows, winning versions/content/vectors, and unique quarantine identities remain stable. Demonstrate an older event cannot overwrite a newer row. Interrupt a delivery before commit and after commit/before ACK, restart, and prove convergence without loss or duplicates; include database-outage/quarantine recovery. Record a reproducible broker-level recovery scenario, beyond mocks.
- **F3-03:** call the running API for known companies after replay; prove usable cards, no self IDs, ordered results, and documented errors. Check ranking correctness with controlled vectors separately from subjective real-model quality. Measure replay duration and representative API latency with stated hardware, sample size, cold/warm conditions, and method; report actual observations without inventing an assessment latency threshold.
- **F3-04:** from the final README, reproduce locked installation, supplied services, schema init, consumer/API startup, publication, verification commands and sample requests; document port overrides, model download/cache, destructive reset behavior, and persistence limits. Final `DESIGN.md` describes actual architecture, assumptions, failures, limitations, measured evidence, what remains, 10x scaling options, and AI tools (including Codex planning use). For 10x, discuss measuring CPU/search bottlenecks, batching/concurrency, and evidence-led ANN; do not build those now.
- **Exit/evidence:** run the complete admitted Pytest suite and real end-to-end checks. Retain reusable integration/recovery scenarios and compact measured findings in `DESIGN.md`; keep generated logs/reports/model weights out of Git. Optional quality work follows F3-OPT below and is not required for core acceptance. One completed F3 commit contains durable verification/docs and accurate milestone/roadmap status.

#### F3-OPT — Recommendation quality evaluation (time permitting)

- **Entry/time gate:** begin only after F1, F2, and required F3 verification/documentation are complete. Use at most the remaining assessment time, capped at one hour; the six-hour overall development limit is unchanged. If the reserve is needed to complete required work, skip F3-OPT and document that decision in `DESIGN.md`.
- **Sample and judgments:** sample up to 10 companies, including English descriptions, Malay descriptions, and fallback-embedding cases. Record the selected companies and selection method. Manually inspect each company's top-four recommendations and assign relevance scores: **2 = strongly similar business activity, 1 = related, 0 = unrelated**.
- **Metric:** report strict **Precision@4 = number of score-2 recommendations / 4** for each sampled company, plus the mean across sampled companies. Scores 0 and 1 are not relevant for this metric; any missing recommendation slots count as non-relevant. Include qualitative error examples.
- **Evidence and limits:** record methodology, judgments, observations, and limitations in `DESIGN.md`. Explain that these manual results are a small exploratory evaluation, not statistically representative. Do not claim results before evaluation. Do not switch embedding models or implement new features as part of F3-OPT; retain its write-up within F3's existing commit boundary.

## Execution, retention, and review gates

Keep production responsibilities easy to trace: delivery orchestration → typed event transformation → CPU embedding → transactional persistence; API request → validated ID → database ranking → response. Avoid generic service frameworks. No formatter, linter, type checker, or CI policy is configured in the starter; ignore patterns alone do not establish one. Planned verification command is `uv run pytest` (focused capability suites first, full suite at F3). Do not silently add unrelated quality tooling; admit only dependencies/configuration necessary for the feature's implementation and tests.

The user approved M1 and authorized F1 and F2. The ignored `.sdd/` handoff is removed after the feature commit; detailed command output and temporary state stay out of canonical docs.

Promote durable regressions to capability-oriented tests, keep the authoritative feed unchanged, and retain only independent small fixtures needed for cases the feed lacks. Purge scratch scenarios/reports at feature completion; generated artifacts and model caches stay ignored. Include planning changes in the appropriate completed feature boundary, or one standalone planning commit only after approval. Each feature has one detailed commit with truthful verification and residual risks. Preserve the full completed M1 plan through the final feature commit; keep only a concise completed summary in the roadmap when later reusing this milestone file.

There is no existing application path to migrate or remove. F1 review verified SQL atomicity, queue compatibility, timestamp assumptions, and failure handling against its feature contract. F2 adds the single supported card-ranking endpoint; no parallel recommendation path or extra API surface remains. Reopen human review only for a material architecture/authority conflict, runtime/model incompatibility requiring a changed decision, scope expansion, or a necessary manual check that cannot be established automatically. Plan review is complete, including naive-time and struck-off semantics. F3 is the remaining feature in the approved M1.
