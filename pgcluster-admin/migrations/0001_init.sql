CREATE TABLE hosts (
    id                  text PRIMARY KEY,
    name                text NOT NULL UNIQUE,
    failure_domain      text,
    address             text,
    version             text,
    capacity            jsonb,
    -- Rises with any change to this host's instances.
    generation          bigint NOT NULL DEFAULT 1,
    observed_generation bigint NOT NULL DEFAULT 0,
    enrolled_at         timestamptz,
    last_seen_at        timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE enrollment_tokens (
    token_hash text PRIMARY KEY,
    host_id    text NOT NULL REFERENCES hosts (id) ON DELETE CASCADE,
    expires_at timestamptz NOT NULL,
    used_at    timestamptz
);

CREATE TABLE databases (
    id            text PRIMARY KEY,
    name          text NOT NULL,
    workspace     text,
    status        text NOT NULL,
    status_reason text,
    -- Rises with any change to `spec`.
    generation    bigint NOT NULL DEFAULT 1,
    spec          jsonb NOT NULL,
    ca_cert_pem   text,
    -- Encrypted: see secrets.rs.
    secrets       bytea,
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX databases_live_name ON databases (name) WHERE status <> 'deleted';

CREATE TABLE nodes (
    database_id         text NOT NULL REFERENCES databases (id) ON DELETE CASCADE,
    node                text NOT NULL,
    host_id             text NOT NULL REFERENCES hosts (id),
    port_base           integer NOT NULL,
    secrets             bytea NOT NULL,
    -- Taken out of desired state, and waiting for the host to remove it.
    retracted           boolean NOT NULL DEFAULT false,
    -- Whether the host's last report listed this instance.
    present             boolean NOT NULL DEFAULT false,
    observed_generation bigint NOT NULL DEFAULT 0,
    state               text NOT NULL DEFAULT 'pending',
    error               text,
    PRIMARY KEY (database_id, node),
    UNIQUE (host_id, port_base)
);

CREATE TABLE operations (
    id                text PRIMARY KEY,
    -- Orders a database's operations, which run one at a time.
    seq               bigserial NOT NULL,
    database_id       text NOT NULL REFERENCES databases (id),
    kind              text NOT NULL,
    status            text NOT NULL DEFAULT 'pending',
    target_generation bigint NOT NULL,
    deadline          timestamptz NOT NULL,
    error             text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    finished_at       timestamptz
);

CREATE INDEX operations_active ON operations (database_id, seq)
    WHERE status IN ('pending', 'running');

CREATE TABLE operation_steps (
    operation_id text NOT NULL REFERENCES operations (id) ON DELETE CASCADE,
    seq          integer NOT NULL,
    name         text NOT NULL,
    status       text NOT NULL DEFAULT 'pending',
    attempts     integer NOT NULL DEFAULT 0,
    not_before   timestamptz NOT NULL DEFAULT now(),
    finished_at  timestamptz,
    PRIMARY KEY (operation_id, seq)
);

CREATE INDEX operation_steps_pending ON operation_steps (not_before) WHERE status = 'pending';

CREATE TABLE operation_logs (
    id           bigserial PRIMARY KEY,
    operation_id text NOT NULL REFERENCES operations (id) ON DELETE CASCADE,
    source       text NOT NULL,
    message      text NOT NULL,
    at           timestamptz NOT NULL DEFAULT now()
);
