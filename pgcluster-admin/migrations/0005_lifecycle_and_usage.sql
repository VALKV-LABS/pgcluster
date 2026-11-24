ALTER TABLE databases
    -- Set while a suspension keeps clients out. Nodes, data and backups stay.
    ADD COLUMN suspended_at timestamptz,
    -- Every commit before this was in the WAL archive when the database was
    -- deleted. Its nodes are gone, so they can no longer say.
    ADD COLUMN archived_until timestamptz,
    -- A deleted database's backups stay restorable until this, then go.
    ADD COLUMN purge_after timestamptz,
    -- The host removing them from the repository.
    ADD COLUMN purge_host_id text REFERENCES hosts (id) ON DELETE SET NULL,
    ADD COLUMN purge_error text,
    ADD COLUMN purged_at timestamptz;

CREATE INDEX databases_purge ON databases (purge_after)
    WHERE purge_after IS NOT NULL AND purged_at IS NULL;

-- What each node was given, and for how long. Usage is read from here, so
-- it is kept by triggers: no code path that adds, resizes or removes a node
-- can forget it. A row is closed when its node is removed or resized, and a
-- resize opens one with the new resources at the same instant.
CREATE TABLE usage_intervals (
    id          bigserial PRIMARY KEY,
    database_id text NOT NULL,
    node        text NOT NULL,
    host_id     text NOT NULL,
    vcpu        double precision NOT NULL,
    memory_gb   double precision NOT NULL,
    storage_gb  double precision NOT NULL,
    started_at  timestamptz NOT NULL DEFAULT now(),
    ended_at    timestamptz
);

CREATE INDEX usage_intervals_open ON usage_intervals (database_id, node) WHERE ended_at IS NULL;
CREATE INDEX usage_intervals_span ON usage_intervals (started_at);

CREATE FUNCTION usage_open_node() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO usage_intervals (database_id, node, host_id, vcpu, memory_gb, storage_gb)
    SELECT NEW.database_id, NEW.node, NEW.host_id,
           (d.spec->'resources'->>'vcpu')::double precision,
           (d.spec->'resources'->>'memory_gb')::double precision,
           (d.spec->'resources'->>'storage_gb')::double precision
    FROM databases d WHERE d.id = NEW.database_id;
    RETURN NEW;
END
$$;

CREATE FUNCTION usage_close_node() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    UPDATE usage_intervals SET ended_at = now()
    WHERE database_id = OLD.database_id AND node = OLD.node AND ended_at IS NULL;
    RETURN OLD;
END
$$;

CREATE FUNCTION usage_resize() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.spec->'resources' IS DISTINCT FROM OLD.spec->'resources' THEN
        WITH closed AS (
            UPDATE usage_intervals SET ended_at = now()
            WHERE database_id = NEW.id AND ended_at IS NULL
            RETURNING node, host_id
        )
        INSERT INTO usage_intervals (database_id, node, host_id, vcpu, memory_gb, storage_gb)
        SELECT NEW.id, closed.node, closed.host_id,
               (NEW.spec->'resources'->>'vcpu')::double precision,
               (NEW.spec->'resources'->>'memory_gb')::double precision,
               (NEW.spec->'resources'->>'storage_gb')::double precision
        FROM closed;
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER nodes_usage_open AFTER INSERT ON nodes
    FOR EACH ROW EXECUTE FUNCTION usage_open_node();
CREATE TRIGGER nodes_usage_close AFTER DELETE ON nodes
    FOR EACH ROW EXECUTE FUNCTION usage_close_node();
CREATE TRIGGER databases_usage_resize AFTER UPDATE OF spec ON databases
    FOR EACH ROW EXECUTE FUNCTION usage_resize();

-- Nodes that exist already are counted from now: what came before was not
-- recorded.
INSERT INTO usage_intervals (database_id, node, host_id, vcpu, memory_gb, storage_gb)
SELECT n.database_id, n.node, n.host_id,
       (d.spec->'resources'->>'vcpu')::double precision,
       (d.spec->'resources'->>'memory_gb')::double precision,
       (d.spec->'resources'->>'storage_gb')::double precision
FROM nodes n JOIN databases d ON d.id = n.database_id;
