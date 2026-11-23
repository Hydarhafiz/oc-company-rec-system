CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS companies (
    registration_number text COLLATE "C" PRIMARY KEY,
    company_name text,
    nature_of_business text,
    industry text,
    state text,
    embedding vector(384) NOT NULL,
    embedding_source text NOT NULL CHECK (
        embedding_source IN ('nature_of_business', 'industry', 'company_name', 'generic')
    ),
    embedding_text text NOT NULL,
    embedding_model text NOT NULL,
    source_updated_at timestamptz NOT NULL,
    last_message_id text COLLATE "C" NOT NULL
);

CREATE TABLE IF NOT EXISTS failed_events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    message_id text COLLATE "C",
    body_sha256 bytea NOT NULL CHECK (octet_length(body_sha256) = 32),
    raw_body bytea NOT NULL,
    reason_code text NOT NULL,
    detail text NOT NULL,
    first_received_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS failed_events_identity_idx
    ON failed_events (
        (message_id IS NULL),
        (COALESCE(message_id, '' COLLATE "C")),
        body_sha256
    );
