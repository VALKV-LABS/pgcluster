ALTER TABLE hosts
    -- No new nodes land here, and the reconciler moves the ones that are.
    ADD COLUMN draining boolean NOT NULL DEFAULT false,
    -- Why the drain cannot move a node, while it cannot.
    ADD COLUMN drain_error text,
    -- When the last node left a draining host.
    ADD COLUMN drained_at timestamptz,
    -- Each Postgres major the host has, and its full version, such as
    -- {"18": "18.1"}. A minor upgrade moves to what every host of a database has.
    ADD COLUMN postgres_versions jsonb NOT NULL DEFAULT '{}';

ALTER TABLE nodes
    -- The version the running server reports.
    ADD COLUMN server_version text;

-- Every change made through the admin API, and the key that made it.
CREATE TABLE audit_log (
    id          bigserial PRIMARY KEY,
    key_name    text NOT NULL,
    method      text NOT NULL,
    path        text NOT NULL,
    status      integer NOT NULL,
    database_id text,
    at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX audit_log_key ON audit_log (key_name, id);
