# MLE Take-Home: Company Recommendation System

Welcome to the **Machine Learning Engineer** take-home from the OneCredit Data & AI team. It's a simplified version of
a system we run in production, where it powers company recommendations and lead generation.

**Who it's for:** people early in their career, with 0 to 2 years of experience. Fresh graduates are welcome.

If you applied, thanks for your time. If you didn't, we hope this helps you learn something new, such as RabbitMQ or
pgvector.

Assessment version: **2026.11** (`mle-v2026.11`). What changed between versions is in [`CHANGELOG.md`](CHANGELOG.md).

## The scenario

When someone opens a company's page on the OneCredit portal, we want to show a **"Similar companies"** panel next to
it. Someone looking at a coffee shop chain in Selangor should see other companies that do similar things.

The registry team already publishes every company change to a RabbitMQ queue. Your job is to build the service that
powers that panel from the feed.

### What the user sees

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  OneCredit Portal                                         Search companies…  │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  Sinar Teguh Bakery Sdn. Bhd.                                   SYN-001550   │
│  Food & Beverage · Selangor                                                  │
│  Operating cafes and coffee shops in the Klang Valley                        │
│                                                                              │
│  Similar companies                                                           │
│  ┌────────────────────────────────────┐  ┌────────────────────────────────┐  │
│  │ Seri Sejahtera Food Sdn. Bhd.      │  │ Everbright Pacific Food        │  │
│  │ Food & Beverage · Johor            │  │ Sdn. Bhd.                      │  │
│  │                                    │  │ Food & Beverage · Kuala Lumpur │  │
│  └────────────────────────────────────┘  └────────────────────────────────┘  │
│  ┌────────────────────────────────────┐  ┌────────────────────────────────┐  │
│  │ Gemilang Delima Food Sdn. Bhd.     │  │ Pacific Everbright Logistik    │  │
│  │ Food & Beverage · Johor            │  │ PLT                            │  │
│  │                                    │  │ Logistics · Kuala Lumpur       │  │
│  └────────────────────────────────────┘  └────────────────────────────────┘  │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

Clicking a card opens that company's page, with its own "Similar companies" panel.

### How it works for the user

```mermaid
sequenceDiagram
    actor User as Portal user
    participant Portal as OneCredit portal
    participant Service as Your service
    participant DB as Postgres + pgvector
    User->>Portal: Opens the page for SYN-001550
    Portal->>Service: Similar companies for SYN-001550
    Service->>DB: Nearest neighbours of SYN-001550
    DB-->>Service: Closest companies
    Service-->>Portal: Similar companies, most similar first
    Portal-->>User: Page with the "Similar companies" panel
    User->>Portal: Clicks Seri Sejahtera Food Sdn. Bhd.
    Portal->>Service: Similar companies for SYN-000539
```

### How the data gets there

```mermaid
flowchart LR
    P[Registry feed<br/>producer] -->|company-events| Q[(RabbitMQ)]
    Q --> C[Your consumer]
    C -->|embed descriptions| E[Embedding model]
    C --> D[(Postgres + pgvector<br/>similarity search)]
    A[Your service] --> D
    W[OneCredit portal] --> A
```

### Notes from the registry team

> The feed merges a few upstream sources and we don't validate what they send. Delivery is at-least-once, and every
> message has an AMQP `message_id`. Each event carries the time the change happened at the source in `updatedAt`. We own
> the `company-events` queue and declare it as durable with no extra arguments. We can't change the format before your
> launch, so please work with what's there.

### What the portal needs

- Recommendations that reflect the latest information we've received about each company.
- No lost companies. If a company is in the feed, users expect to see it on the portal and in other companies'
  recommendations.
- Fast enough to render while the company page loads.

## The task

1. Build a **consumer** that reads the `company-events` queue and stores each company in Postgres, with an embedding
   of its business description in pgvector.
2. Build the **service** the portal calls: given a company, return its most similar companies, with what the portal
   needs to draw each card in the mock above. Run the similarity search in Postgres with pgvector. Matching companies
   on their nature of business is enough for a first version.
3. Write a short `DESIGN.md` covering the decisions you made, the assumptions you made where the scenario was unclear,
   what you would change to handle 10x the volume, and what's left.
4. Optional, up to one hour of your six: go deeper in one area you're strong in. For example: how good the
   recommendations are, latency at a realistic scale, how you'd monitor this in production, or a design for explaining
   a recommendation with an LLM (a design only; no paid APIs). Write it up in `DESIGN.md`.

The rest is your call: the schema, the embedding model, your tests and tooling, and the API, including what the portal
gets back when something goes wrong. Tell us in your own README how to start your consumer and service, and we'll
follow it.

Where the scenario leaves something open, make a reasonable call and write it down. There isn't one right answer.

## Embeddings

Use an open-source embedding model that runs on a laptop CPU. Don't call a paid API. Some options:

- `BAAI/bge-small-en-v1.5`, a small CPU model (384 dimensions), through
  [fastembed](https://qdrant.github.io/fastembed/) or sentence-transformers. The panel in the mock above comes from it.
- A placeholder while you build, such as a hashed bag-of-words vector, as long as you say so in `DESIGN.md`.

## Getting started

You need Docker, [uv](https://docs.astral.sh/uv/) (it installs Python 3.14 for you) and
[just](https://github.com/casey/just). Setup takes about 5 minutes and doesn't count towards your time.

```bash
just setup      # install the locked application and test dependencies
just up         # start Postgres (with pgvector) and RabbitMQ
just db-init    # create the schema and pgvector extension
```

Postgres is at `postgresql://postgres:postgres@localhost:5432/companies`, and the RabbitMQ management UI is at
http://localhost:15672 (user `guest`, password `guest`). If those ports are taken on your machine, export
`POSTGRES_PORT`, `RABBITMQ_PORT` or `RABBITMQ_UI_PORT` in your shell before `just up`; `just publish` uses the same
`RABBITMQ_PORT`.

`just publish` sends the whole feed every time you run it. In this implementation, repeated delivery is safe and leaves
the latest company snapshots unchanged. `just down` runs `docker compose down -v` and deletes the local database and
broker state. Do not use it as a routine restart or replay step; inspect and back up anything you need before using it.

## What's already here

```
.
├── company_rec/           # consumer, event validation, embeddings, persistence, and FastAPI service
├── data/feed.jsonl        # 2,518 events about 2,000 synthetic Malaysian companies
├── producer/publish.py    # unchanged OneCredit feed publisher
├── tests/                 # unit, PostgreSQL, API, feed-oracle, and live recovery checks
├── docker-compose.yml     # Postgres with pgvector, and RabbitMQ
└── Justfile               # setup, up, db-init, consume, publish, api, test, down
```

## Run this implementation

Prerequisites are Docker, `uv` with Python 3.14, and `just`. The first model load needs network access to download the
FastEmbed model unless its files are already cached. Start the services and initialize the schema:

```bash
just setup
just up
just db-init
```

Run each long-lived process in its own terminal. Start the consumer before publishing the feed:

```bash
# Terminal 1: validate PostgreSQL and consume company-events.
just consume

# Terminal 2: publish all 2,518 unchanged feed lines.
just publish

# Terminal 3: serve the recommendation endpoint on 127.0.0.1:8000.
just api
```

The consumer uses manual acknowledgements and PostgreSQL conditional upserts. It acknowledges an invalid event only
after storing its quarantine record. A database, broker, or model failure leaves the delivery unacknowledged; restart
the consumer after the underlying service is available. Replaying the feed is safe and does not replace a newer
snapshot with an older one.

### Request and response

```bash
curl 'http://127.0.0.1:8000/companies/SYN-001550/similar'
```

An observed response after full-feed ingestion:

```json
{
  "registration_number": "SYN-001550",
  "recommendations": [
    {
      "registration_number": "SYN-000539",
      "name": "Seri Sejahtera Food Sdn. Bhd.",
      "industry": "Food & Beverage",
      "state": "Johor",
      "similarity": 1.0
    },
    {
      "registration_number": "SYN-000263",
      "name": "Gemilang Delima Food Sdn. Bhd.",
      "industry": "Food & Beverage",
      "state": "Johor",
      "similarity": 0.8336164951324463
    },
    {
      "registration_number": "SYN-001650",
      "name": "Everbright Pacific Food Sdn. Bhd.",
      "industry": "Food & Beverage",
      "state": "Kuala Lumpur",
      "similarity": 0.8336164951324463
    },
    {
      "registration_number": "SYN-000700",
      "name": "Teguh Mega Logistik Trading",
      "industry": "Logistics",
      "state": "Selangor",
      "similarity": 0.830081840743805
    }
  ]
}
```

`similarity` is `1 - cosine distance`, not a probability. The optional `limit` query parameter must be from 1 through 4.
Errors use `{ "error": { "code": "...", "message": "..." } }`: invalid input returns `422`, an unknown company
returns `404`, and an unavailable database returns `503`.

### Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5432/companies` | Application database used by schema initialization, consumer, and API. |
| `COMPANY_TEST_DATABASE_URL` | unset | Separate database for PostgreSQL/pgvector integration tests. |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:${RABBITMQ_PORT}/` | Consumer and publisher connection URL. |
| `RABBITMQ_PORT` | `5672` | Local AMQP port used by Compose and the publisher fallback URL. |
| `POSTGRES_PORT` | `5432` | Local PostgreSQL port published by Compose. |
| `RABBITMQ_UI_PORT` | `15672` | Local RabbitMQ management UI/API port. |
| `FASTEMBED_CACHE_PATH` | FastEmbed default cache | Optional model cache directory used by the consumer and real-model test. |

Set port overrides in the shell before `just up`; use matching `DATABASE_URL` and `RABBITMQ_URL` values for app
processes if you changed the ports. For example, create a separate test database once and use it for the full suite:

```bash
docker compose exec -T postgres psql -U postgres -d postgres -c 'CREATE DATABASE companies_test;'
COMPANY_TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/companies_test just test
```

The integration fixture creates a temporary schema inside `companies_test` and drops that schema after each test. It
does not use or reset the application database. If the test database already exists, skip the `CREATE DATABASE`
command. Without `COMPANY_TEST_DATABASE_URL`, PostgreSQL integration tests are skipped.

FastEmbed downloads `BAAI/bge-small-en-v1.5` on the first load if it is not cached. To use a prepared cache, set
`FASTEMBED_CACHE_PATH` to that cache directory before starting the consumer. The accepted artifact is
`Qdrant/bge-small-en-v1.5-onnx-Q` revision
`aa8f8b060edb00e03bfdd08813a2949946c8ba55`; embeddings have 384 dimensions and run on CPU.

### Verification commands

Run the full suite and inspect a completed live-feed replay with the independent oracle:

```bash
COMPANY_TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/companies_test just test
uv run python -m tests.verify_feed_ingestion --dsn postgresql://postgres:postgres@localhost:5432/companies
```

The live recovery harness verifies company rollback/redelivery, quarantine-write recovery, process exit after commit
but before ACK, and recovery after an unavailable database at consumer startup. It requires the normal
2,000-company/40-quarantine dataset, an empty `company-events` queue, and no active consumer. It uses three
checked-unused synthetic company IDs and one unique malformed-message identity, deleting only those test records. It
temporarily installs and removes database triggers, functions, and sequences. Run it only against the local
assessment services:

```bash
uv run python -m tests.live_broker_recovery \
  --dsn postgresql://postgres:postgres@localhost:5432/companies \
  --rabbitmq-url amqp://guest:guest@localhost:5672/ \
  --management-url http://localhost:15672
```

The harness uses `FASTEMBED_CACHE_PATH` when set. Otherwise, FastEmbed's default cache must already contain the model;
you can provide a prepared local cache with `--cache-dir /path/to/model/cache`.

### Troubleshooting and reset warning

- Check `docker compose ps` for healthy PostgreSQL and RabbitMQ services. If a local port is occupied, choose a port
  override and update the matching application URL.
- If the app reports a missing schema, run `just db-init`; this is repeatable and does not clear existing rows.
- If publishing appears stuck, confirm `just consume` is running and inspect ready/unacknowledged counts in the
  RabbitMQ management UI at `http://localhost:15672`.
- If consumer startup fails while loading FastEmbed, confirm the model cache path is writable and contains the
  accepted model artifact, or allow the first run to download it.
- If `uv` reports that its cache is not writable, point its cache at a writable directory, for example
  `UV_CACHE_DIR=/tmp/oc-company-rec-system-uv-cache just test`.
- `just down` deletes PostgreSQL and RabbitMQ state because it runs `docker compose down -v`. Do not run it to resolve
  an ordinary startup, schema, or replay problem. Inspect the services and data first.

## How to submit

1. Click **Use this template** → **Create a new repository**, and make it **public**. Any name is fine.
2. Build your solution in your copy, with `DESIGN.md` at the top level.
3. When you're done, fill in the [submission form](https://docs.google.com/forms/d/e/1FAIpQLSdjCsXP5jFrKrIi4krLVS8Sq0rhM4mLlhycpsAo34sSj4h3FA/viewform) with a link to your repo and your résumé.

If the setup itself is broken (Docker, the services or the data won't start), open a
[Setup problem](https://github.com/OneCreditMY/mle-take-home/issues/new?template=setup-problem.yml) issue. Issues are public, so don't put your
solution or personal details in one.

## Time

Plan for **4 to 6 hours**, within 7 days of creating your copy, and stop at 6 hours. If you run out of time,
write down what you'd do next. A clear, honest "here's what's left" counts in your favour.

## What we look at

- Does it work, and does it keep working?
- How you tested it.
- How you reasoned about your choices and assumptions in `DESIGN.md`.
- How readable your code is.

We don't expect a perfect or complete solution.

## Using AI tools

Go ahead. Tell us in `DESIGN.md` which tools you used and for what. In the interview we'll ask you
to walk us through your code, so make sure you understand everything you submit.

## After you submit

- We review your work as soon as we can.
- If we move forward, the next interview is a walkthrough of your code with us.
- Either way, you'll get written feedback by email.
- Your repo stays yours. Keep it public as a portfolio piece if you like.

## About the data

Everything in this repo is synthetic. We generated it for this assessment.

- The companies, names, registration numbers, descriptions and events are made up. Any resemblance to a real company
  or person is a coincidence.
- No OneCredit customer data, credit data or production data is in this repo, and none was used to create it.
- Registration numbers use a made-up `SYN-` format, so they can't be mistaken for real Companies Commission of
  Malaysia (SSM) numbers.
- Your copy is public, so keep secrets, credentials and personal data out of it.
