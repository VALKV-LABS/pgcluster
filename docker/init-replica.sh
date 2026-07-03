#!/usr/bin/env bash
# init-replica.sh — run pg_basebackup from the primary then start in standby mode.
#
# Usage (entrypoint for a replica container):
#   /docker-entrypoint-initdb.d/init-replica.sh <primary-host> [primary-port]
#
# Environment:
#   PGDATA           — replica data directory (default: /var/lib/postgresql/data)
#   POSTGRES_USER    — replication user (default: postgres)
#   POSTGRES_PASSWORD — password (optional; used in .pgpass if set)
#
# The script is idempotent: if PGDATA/PG_VERSION already exists the backup step
# is skipped and Postgres is started directly.

set -euo pipefail

PRIMARY_HOST="${1:?primary host required as first argument}"
PRIMARY_PORT="${2:-5432}"
PGDATA="${PGDATA:-/var/lib/postgresql/data}"
PGUSER="${POSTGRES_USER:-postgres}"

echo "[init-replica] PGDATA=${PGDATA} PRIMARY=${PRIMARY_HOST}:${PRIMARY_PORT}"

if [ -f "${PGDATA}/PG_VERSION" ]; then
  echo "[init-replica] Data directory already initialised — skipping pg_basebackup."
else
  echo "[init-replica] Running pg_basebackup from ${PRIMARY_HOST}:${PRIMARY_PORT} ..."

  # Write a temporary .pgpass so pg_basebackup can authenticate
  if [ -n "${POSTGRES_PASSWORD:-}" ]; then
    PGPASSFILE=$(mktemp)
    chmod 600 "${PGPASSFILE}"
    echo "${PRIMARY_HOST}:${PRIMARY_PORT}:replication:${PGUSER}:${POSTGRES_PASSWORD}" \
      > "${PGPASSFILE}"
    export PGPASSFILE
  fi

  # Retry pg_basebackup up to 5 times: two replicas starting simultaneously can
  # race and hit "WAL segment already removed" if the other replica's stream
  # causes a checkpoint between slot creation and WAL streaming.
  for attempt in 1 2 3 4 5; do
    if pg_basebackup \
        --host="${PRIMARY_HOST}" \
        --port="${PRIMARY_PORT}" \
        --username="${PGUSER}" \
        --pgdata="${PGDATA}" \
        --wal-method=stream \
        --checkpoint=fast \
        --no-password \
        --progress \
        --verbose; then
      break
    fi
    echo "[init-replica] pg_basebackup failed (attempt ${attempt}/5), retrying in 3s ..."
    rm -rf "${PGDATA:?}"/*
    sleep 3
  done

  # Write standby.signal so Postgres starts as a hot standby
  touch "${PGDATA}/standby.signal"

  # Append primary_conninfo to postgresql.auto.conf
  cat >> "${PGDATA}/postgresql.auto.conf" <<EOF

# Added by init-replica.sh
primary_conninfo = 'host=${PRIMARY_HOST} port=${PRIMARY_PORT} user=${PGUSER} password=${POSTGRES_PASSWORD}'
hot_standby = on
EOF

  echo "[init-replica] pg_basebackup complete. Standby mode configured."
fi

# Ensure data dir is owned by the postgres user with correct permissions.
chown -R postgres:postgres "${PGDATA}"
chmod 700 "${PGDATA}"

echo "[init-replica] Starting Postgres in standby mode ..."
# Do NOT pass "$@" — script args are <primary-host> [port], not postgres flags.
# Use the same hba_file as pg-primary so that after promotion replication is allowed from all nodes.
HBA_FILE="/etc/postgresql/pg_hba.conf"
if [ -f "${HBA_FILE}" ]; then
  exec gosu postgres postgres -D "${PGDATA}" \
    -c hba_file="${HBA_FILE}" \
    -c wal_level=replica \
    -c max_wal_senders=10 \
    -c max_replication_slots=10 \
    -c hot_standby=on \
    -c listen_addresses='*'
else
  exec gosu postgres postgres -D "${PGDATA}"
fi
