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
#   PG_NODE_ID       — pgcluster node ID, used as application_name (optional)
#   PG_TLS_NAME      — serve TLS with /certs/<name>.crt and .key (optional)
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

  # pg_basebackup needs an empty PGDATA, so the password file stays outside it
  # until the backup finishes. It then moves in for primary_conninfo (passfile=).
  if [ -n "${POSTGRES_PASSWORD:-}" ]; then
    PGPASSFILE="$(mktemp)"
    echo "${PRIMARY_HOST}:${PRIMARY_PORT}:replication:${PGUSER}:${POSTGRES_PASSWORD}" \
      > "${PGPASSFILE}"
    chmod 600 "${PGPASSFILE}"
    export PGPASSFILE
  fi

  # A slot holds WAL for this standby while it is away, up to
  # max_slot_wal_keep_size. It must exist before the standby first connects.
  if [ -n "${PG_NODE_ID:-}" ]; then
    SLOT_NAME="pgcluster_${PG_NODE_ID}"
    PGPASSWORD="${POSTGRES_PASSWORD:-}" psql \
      --host="${PRIMARY_HOST}" --port="${PRIMARY_PORT}" --username="${PGUSER}" \
      --dbname=postgres --no-password --quiet \
      -c "SELECT pg_create_physical_replication_slot('${SLOT_NAME}')
          WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = '${SLOT_NAME}')"
  fi

  # Retry pg_basebackup up to 5 times: two replicas starting simultaneously can
  # race and hit "WAL segment already removed" if the other replica's stream
  # causes a checkpoint between slot creation and WAL streaming.
  BACKUP_DONE=0
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
      BACKUP_DONE=1
      break
    fi
    echo "[init-replica] pg_basebackup failed (attempt ${attempt}/5), retrying in 3s ..."
    find "${PGDATA}" -mindepth 1 -delete
    sleep 3
  done

  if [ "${BACKUP_DONE}" -ne 1 ]; then
    echo "[init-replica] pg_basebackup failed after 5 attempts." >&2
    exit 1
  fi

  # Write standby.signal so Postgres starts as a hot standby
  touch "${PGDATA}/standby.signal"

  # Append primary_conninfo to postgresql.auto.conf.
  # Use passfile= so the password is not stored in plaintext in the config.
  PASSFILE_ARG=""
  if [ -n "${POSTGRES_PASSWORD:-}" ]; then
    mv "${PGPASSFILE}" "${PGDATA}/.pgpass"
    chmod 600 "${PGDATA}/.pgpass"
    PASSFILE_ARG=" passfile=${PGDATA}/.pgpass"
  fi
  cat >> "${PGDATA}/postgresql.auto.conf" <<EOF

# Added by init-replica.sh
primary_conninfo = 'host=${PRIMARY_HOST} port=${PRIMARY_PORT} user=${PGUSER}${PASSFILE_ARG}${PG_NODE_ID:+ application_name=${PG_NODE_ID}}'
hot_standby = on
EOF

  if [ -n "${SLOT_NAME:-}" ]; then
    echo "primary_slot_name = '${SLOT_NAME}'" >> "${PGDATA}/postgresql.auto.conf"
  fi

  echo "[init-replica] pg_basebackup complete. Standby mode configured."
fi

# Ensure data dir is owned by the postgres user with correct permissions.
chown -R postgres:postgres "${PGDATA}"
chmod 700 "${PGDATA}"

echo "[init-replica] Starting Postgres in standby mode ..."
# Do NOT pass "$@" — script args are <primary-host> [port], not postgres flags.
# Use the same hba_file as pg-primary so that after promotion replication is allowed from all nodes.
HBA_FILE="/etc/postgresql/pg_hba.conf"
TLS_ARGS=()
if [ -n "${PG_TLS_NAME:-}" ]; then
  TLS_ARGS=(-c ssl=on
    -c "ssl_cert_file=/certs/${PG_TLS_NAME}.crt"
    -c "ssl_key_file=/certs/${PG_TLS_NAME}.key")
fi
if [ -f "${HBA_FILE}" ]; then
  exec gosu postgres postgres -D "${PGDATA}" \
    -c hba_file="${HBA_FILE}" \
    -c wal_level=replica \
    -c max_wal_senders=10 \
    -c max_replication_slots=10 \
    -c hot_standby=on \
    -c listen_addresses='*' \
    "${TLS_ARGS[@]}"
else
  exec gosu postgres postgres -D "${PGDATA}" "${TLS_ARGS[@]}"
fi
