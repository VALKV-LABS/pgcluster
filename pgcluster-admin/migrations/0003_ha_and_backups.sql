ALTER TABLE hosts
    -- The hash of the CA bundle the host last reported trusting.
    ADD COLUMN ca_fingerprint text,
    -- The hash of the CA that signed the host's client certificate.
    ADD COLUMN certificate_ca text;

ALTER TABLE databases
    -- What the cluster's Raft leader last reported. It owns these facts.
    ADD COLUMN observed jsonb NOT NULL DEFAULT '{}',
    -- Clients get no access until the first backup exists.
    ADD COLUMN access_open boolean NOT NULL DEFAULT false;

ALTER TABLE nodes
    -- How this node gets its data: initdb, seed or restore.
    ADD COLUMN bootstrap text NOT NULL DEFAULT 'initdb',
    -- Every commit before this is in the WAL archive, as the primary reports it.
    ADD COLUMN archived_until timestamptz,
    ADD COLUMN offline_since timestamptz,
    ADD COLUMN replacement_flagged boolean NOT NULL DEFAULT false;

ALTER TABLE operations
    ADD COLUMN params jsonb NOT NULL DEFAULT '{}';

-- One-off work for a database. Every node gets it, and it runs where it can.
CREATE TABLE tasks (
    id           text PRIMARY KEY,
    operation_id text NOT NULL REFERENCES operations (id) ON DELETE CASCADE,
    database_id  text NOT NULL REFERENCES databases (id) ON DELETE CASCADE,
    kind         text NOT NULL,
    -- `leader` or `primary`.
    run_on       text NOT NULL,
    params       jsonb NOT NULL DEFAULT '{}',
    status       text NOT NULL DEFAULT 'pending',
    error        text,
    output       jsonb,
    created_at   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz
);

CREATE INDEX tasks_pending ON tasks (database_id) WHERE status = 'pending';
CREATE UNIQUE INDEX tasks_one_per_step ON tasks (operation_id, kind);

CREATE TABLE events (
    id          bigserial PRIMARY KEY,
    type        text NOT NULL,
    database_id text,
    node        text,
    detail      jsonb NOT NULL DEFAULT '{}',
    at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE backup_repositories (
    id            text PRIMARY KEY,
    name          text NOT NULL UNIQUE,
    type          text NOT NULL,
    config        jsonb NOT NULL,
    -- Encrypted: see secrets.rs.
    credentials   bytea,
    -- `verifying`, `verified` or `failed`.
    status        text NOT NULL DEFAULT 'verifying',
    error         text,
    -- The host proving it can be written to.
    check_host_id text REFERENCES hosts (id) ON DELETE SET NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE backups (
    database_id  text NOT NULL REFERENCES databases (id) ON DELETE CASCADE,
    id           text NOT NULL,
    label        text NOT NULL,
    completed_at timestamptz NOT NULL,
    size_bytes   bigint NOT NULL,
    PRIMARY KEY (database_id, id)
);

-- A rotation of the admin CA in progress. At most one row.
CREATE TABLE admin_ca_rotation (
    id            integer PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    -- `trust`, `issue` or `trim`.
    phase         text NOT NULL,
    old_cert_pem  text NOT NULL,
    new_cert_pem  text NOT NULL,
    -- Encrypted: see secrets.rs.
    new_key       bytea NOT NULL,
    started_at    timestamptz NOT NULL DEFAULT now()
);
