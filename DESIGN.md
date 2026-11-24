# Company recommendation system design

## Problem and architecture

The portal needs up to four similar-company cards for a company page. The supplied registry producer remains the source
of events and publishes the original feed to the durable `company-events` RabbitMQ queue. One Python application has two
entry points:

```text
producer/publish.py → company-events → company_rec.consumer → validation → FastEmbed → PostgreSQL/pgvector
OneCredit portal → company_rec.api → exact pgvector cosine search ────────────────────┘
```

`company_rec.consumer` owns queue delivery, one-at-a-time processing, reconnection, and acknowledgement. It passes each
body and AMQP `message_id` to `company_rec.events`, which validates the event and converts the supported fields into a
normalized company snapshot. `company_rec.embeddings` selects a text fallback and runs the cached CPU model away from
the asyncio event loop. `company_rec.database` writes the snapshot and vector together or stores a quarantine record.
The PostgreSQL transaction finishes before the consumer acknowledges the delivery.

`company_rec.api` owns the portal response and its database-pool lifecycle. The route accepts a company ID and a limit;
`company_rec.database` checks the source and returns candidates from one repeatable-read snapshot. PostgreSQL ranks
stored vectors directly. The API does not generate query embeddings or expose general company CRUD. This keeps the
portal contract focused on drawing similar-company cards.

## Event and persistence decisions

The consumer declares the existing queue as durable without adding arguments, uses manual acknowledgements, and sets
prefetch to one. The original `producer/publish.py` and `data/feed.jsonl` are unchanged. The producer's AMQP message ID
is the SHA-256 of the exact feed line bytes; it is not part of the event JSON.

The event parser accepts only `company-upserted` objects with a supported registration number, timestamp, and data
object. Registration numbers are trimmed, uppercased, and checked against `SYN-[0-9]{6}`. Metadata strings are trimmed;
missing, null, and blank values become unavailable, while non-string values are rejected. A valid company with an
incomplete description remains eligible for recommendations. Extra source fields are ignored.

Supported timestamps are ISO datetimes with an offset or `Z`, timezone-free ISO datetimes, and integer Unix epoch
milliseconds. They are stored as UTC `timestamptz`. The feed's timezone-free values are interpreted as
`Asia/Kuala_Lumpur`; this is an assumption based on the Malaysian feed context. Snapshot events replace all metadata,
including missing values, rather than merging stale fields. Equal-time events are ordered by message ID bytes for
determinism; the message ID is a body hash and does not establish real chronology.

The schema in `company_rec/schema.sql` has two tables:

- `companies` stores one current snapshot per normalized ID, the 384-value vector, the selected embedding text and
  source, the model identity, and the source version.
- `failed_events` keeps the raw bytes, body hash, message ID, reason, and first-received time for invalid or unsupported
  deliveries. A unique identity over message ID and body hash makes replayed failures idempotent.

For a valid event, `company_rec.database` uses a transaction and `INSERT ... ON CONFLICT ... DO UPDATE ... WHERE` to
replace a row only when `(source_updated_at, message_id)` is greater than the stored version. The comparison uses
bytewise message-ID ordering. Older and duplicate versions are successful no-ops. A vector and its metadata therefore
come from the same winning snapshot without an unbounded processed-message table.

The consumer acknowledges a valid update only after the transaction returns. It first persists an invalid event to
`failed_events`, then acknowledges it. Database, broker, model, and unexpected failures are not converted to bad data
and are not acknowledged. Closing the consumer channel returns an in-flight delivery for at-least-once redelivery; the
database guard makes redelivery safe. This is not a distributed transaction and does not promise exactly-once
transport.

## Embeddings and recommendation API

The model is FastEmbed `BAAI/bge-small-en-v1.5` on CPU. The resolved artifact was
`Qdrant/bge-small-en-v1.5-onnx-Q@aa8f8b060edb00e03bfdd08813a2949946c8ba55` with FastEmbed 0.9.1. The database accepts
only finite, nonzero 384-dimensional vectors.

Embedding input uses the first available value in this order: `natureOfBusiness`, `industry`, `companyName`, then the
fixed text `Company business activity unspecified.` The database records which source and exact text were used. The
model is English-oriented while the feed contains English and Malay descriptions. This keeps model choice and runtime
simple. The required verification did not evaluate semantic relevance. A small optional manual review below includes
one English-source query with Malay candidates and one Malay-source query; it does not establish language-wide quality.

The portal route is `GET /companies/{registration_number}/similar?limit=4`. It excludes the source, orders by
`candidate.embedding <=> source.embedding` ascending, then by registration number in bytewise order, and returns
`similarity = 1 - cosine distance`. The score is a similarity value, not a probability. Responses include company ID,
display name, industry, state, and similarity. Invalid IDs or limits return `422`; a valid unknown company returns
`404`; database connection/query failures return `503`; unexpected failures return a sanitized `500` envelope.

## Full-feed verification

The read-only source oracle is `tests/feed_oracle.py`; it computes normalized winners and failure identities without
calling the production parser. `tests/verify_feed_ingestion.py` compares every persisted company snapshot and
quarantine body against that oracle. The authoritative feed SHA-256 was
`3c479dbc3bf0dd3ec9a10a4a31ab52d5f43471aa5c875dcaf3d182e79962d73b`.

The first live run used the unchanged `just publish` target while the actual consumer was running. It published
**2,518 deliveries**: **2,473 valid upsert deliveries** and **45 invalid/unsupported deliveries**. The independent
oracle found **2,000 unique winning companies** and **40 unique quarantine identities**. Six identical null bodies
account for the difference between six invalid-envelope deliveries and one stored failure identity. RabbitMQ ended
with **0 ready and 0 unacknowledged messages**. Every company had a finite, nonzero 384-dimensional embedding using
the same recorded model artifact. The normalized winning timestamp, message ID, name, business description, industry,
state, embedding source, and embedding text matched the independent latest-event oracle for all 2,000 rows.

The feed was then replayed unchanged. A digest over every company column (including the complete vector text) and every
quarantine row was identical before and after:

```text
companies: 2,000
quarantine identities: 40
SHA-256 before replay 2: 9ddf7e0d57fbf615c674c0afa2c9ae5a12bdf71fba6adb8edf62bfd21856ee7b
SHA-256 after replay 2:  9ddf7e0d57fbf615c674c0afa2c9ae5a12bdf71fba6adb8edf62bfd21856ee7b
```

After the two full-feed runs, RabbitMQ reported 5,036 publishes, deliveries, and acknowledgements, no redeliveries,
and zero ready or unacknowledged messages. The second replay's duration was not retained because its monitor could
match the already-populated database counts before it had confirmed the broker publish counter; the state digest and
final broker totals provide the idempotency evidence instead.

## Recovery verification

`tests/live_broker_recovery.py` is a repeatable, isolated local scenario. It requires the expected full-feed database,
an empty `company-events` queue, no active consumer, and three unused synthetic company IDs. It installs triggers that
fail only the first company transaction and the first quarantine transaction for their isolated inputs, kills a
consumer immediately after commit for another ID, and points a fourth startup attempt at an unreachable local
database port. It removes the triggers/functions/sequences, acknowledges or drains only its own messages, deletes only
its three company rows and one failure identity, and checks the queue is empty before it exits.

All four scenarios passed:

1. **Transaction failure before commit:** the PostgreSQL trigger raised once and the sequence recorded two attempts.
   The original transaction rolled back; RabbitMQ recorded two deliveries, one redelivery, and one acknowledgement
   after the successful retry. The recovered company row was present once.
2. **Quarantine transaction failure:** a trigger failed the first insert for one malformed test message. The message
   was redelivered; the retry stored the exact raw body and `invalid_json` reason, then ACKed it. The failure identity
   was present once.
3. **Process exit after commit, before ACK:** an injected consumer exited with code 86 immediately after the database
   transaction returned. PostgreSQL retained the row; RabbitMQ redelivered the unacknowledged message; a healthy
   consumer treated it as an unchanged version and acknowledged it without changing the row snapshot.
4. **Database unavailable at consumer startup:** the consumer exited with code 1 against an unreachable database
   port. Its queued event remained ready with zero unacknowledged deliveries. Restarting the consumer with the healthy database
   persisted and acknowledged the event. A live interruption of the running PostgreSQL container was not induced.

## Live API and performance observations

The `just api` Uvicorn service was called over loopback after ingestion. Five known source companies each returned four
recommendations. Responses excluded the source and matched direct PostgreSQL metadata, pgvector ordering, and
similarity scores; the controlled-vector API tests independently verify cosine ranking and deterministic ties. Invalid
ID and limit requests returned `422`, an unknown company returned `404`, and a loopback request through a controlled
unavailable-store instance returned the sanitized `503` response. PostgreSQL itself was not stopped to test that
response.

On the local WSL2/Docker Desktop environment, the host reported an AMD Ryzen 5 9600X, 8 logical CPUs available, and
27 GiB memory. PostgreSQL 16/pgvector 0.8.7 and RabbitMQ 3.13.7 ran in the supplied containers. The first ingestion
measurement began when `just publish` started, with the consumer and model already loaded and the database empty:

| Measurement | Result | Method and conditions |
| --- | ---: | --- |
| Full-feed publish and drain | 37.656 s | Original publisher; stop when all 2,000 companies and 40 quarantine identities were persisted and the queue had 0 ready/0 unacknowledged messages. Publisher call itself took 6.554 s. |
| API p50 | 2.996 ms | 100 sequential successful loopback GETs, persistent HTTP client, 5 warm-up requests, 5 source IDs. |
| API p95 | 3.707 ms | Nearest-rank p95 over the same 100 request samples. |

The model files and consumer were warm for ingestion; the application database started empty. API requests used a
running Uvicorn process and warmed local PostgreSQL/OS caches. No cold-start latency, concurrent load, target SLA, or
production-scale performance is claimed. These are local observations, not performance targets.

The developer reported **120 minutes** of assessment time before F3 closeout began at approximately 2026-11-24
07:03 UTC. At the final verification checkpoint around 07:44 UTC, approximately **41 additional minutes** had elapsed,
for an estimated cumulative total of **161 minutes of the 360-minute limit** and about **199 minutes remaining**. This
is an approximate wall-clock accounting from the supplied time record.

## Optional recommendation-quality review

The developer selected three live API cases: the assessment's English cafe example (`SYN-001550`), the first
C-collated registration whose description matched a fixed Malay business-vocabulary query (`SYN-000002`), and the
first C-collated company without a business description and with an industry fallback (`SYN-000014`). The Malay
query used `perkhidmatan|makanan|pembekal|pembekalan|menjual|jualan|perniagaan|restoran|katering|pengangkutan|membekal|pertanian|syarikat|perusahaan`.
Their top-four IDs were confirmed through the running HTTP API. The developer manually supplied all eight judged
scores; score 2 is the only relevant class for strict Precision@4. The four fallback recommendations were explicitly
left unscored because their only shared input was the generic `Hospitality` industry label.

| Query | Candidate scores in returned order | Strict Precision@4 | Judgment notes |
| --- | --- | ---: | --- |
| `SYN-001550` — “Operating cafes and coffee shops in the Klang Valley” | `SYN-000539`: 2; `SYN-000263`: 2; `SYN-001650`: 2; `SYN-000700`: 0 | 3/4 = 0.75 | The first candidate has the exact English description. The next two use Malay descriptions the reviewer judged semantically equivalent. The parcel-delivery business was unrelated. |
| `SYN-000002` — “Restoran masakan Melayu dan Cina” | `SYN-000039`: 2; `SYN-000068`: 2; `SYN-000108`: 2; `SYN-000368`: 2 | 4/4 = 1.00 | All four have the exact same business-description text as the source. These are exact-description matches, not evidence of cross-language retrieval. |
| `SYN-000014` — no business description; embedding fallback `Hospitality` | `SYN-000320`: unscored; `SYN-000728`: unscored; `SYN-000936`: unscored; `SYN-000964`: unscored | Excluded | The source and candidates have only an industry fallback and no business descriptions. The reviewer skipped this case because the available evidence cannot establish activity similarity. |

Across the **eight judged recommendations** from two source companies, strict Precision@4 is **7/8 = 0.875**. The
mean of the two per-company Precision@4 values is also **0.875**. The fallback case is reported separately and is not
included in either denominator. This is a tiny, manually selected synthetic sample, not a representative quality
estimate. In the English-source case, one candidate has an exact English description and two Malay descriptions were
judged semantically equivalent to it. Those two reviewer judgments are cross-language semantic matches, not exact
description matches. All four recommendations in the Malay-source case have exact duplicate descriptions. The
fallback case provides no activity-level evidence. These cases do not support a general claim about multilingual
ranking quality.

## Trade-offs, limits, and a 10x path

The baseline favors exactness and simple operations for 2,000 companies: one consumer, prefetch one, CPU inference,
explicit SQL, and exact cosine search without an ANN index. At ten times the volume, first measure queue age, embedding
time, write throughput, database CPU, query latency, and result quality. If embedding is the bottleneck, evaluate
bounded batching and additional consumers while retaining the atomic source-version guard. If exact search becomes a
measured bottleneck, benchmark pgvector ANN options against exact rankings and a recall set before adding an index.
Do not add a second model or infrastructure based only on the volume estimate.

Remaining limitations are the synthetic registration-number policy, the documented timezone assumption, unspecified
struck-off event semantics (these events are quarantined without deleting a company), English-oriented embeddings for
some Malay descriptions, the unscored description-fallback case, no production authentication/deployment/monitoring,
and Compose's lack of named persistent volumes. The optional relevance labels cover only eight recommendations and
must not be generalized to the feed or to Malay quality overall.

## AI tool use

OpenAI Codex was used to inspect and implement the approved milestone, run the full-feed and failure-recovery
verification, and prepare these run instructions and design notes. The `$sdd-router` and `$tdd-loop` skills guided
milestone continuation, evidence capture, and verification. The code and design should be reviewed and understood by
the submitting developer before an interview walkthrough.
