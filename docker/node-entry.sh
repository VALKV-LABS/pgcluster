#!/bin/sh
# Entry point for a node of the e2e stack: one container, with vk-agent owning
# Postgres. With PG_BOOTSTRAP set it creates the first primary. Other nodes start
# empty, and the Raft leader has their agents seed them.
set -eu

PGDATA=/var/lib/postgresql/data

# Every node reads the same config, copied by seeding, so the certificate lives
# at a fixed path.
mkdir -p /tmp/pg-tls
cp "/certs/${PG_TLS_NAME}.crt" /tmp/pg-tls/node.crt
cp "/certs/${PG_TLS_NAME}.key" /tmp/pg-tls/node.key
chmod 600 /tmp/pg-tls/node.key

if [ -n "${PG_BOOTSTRAP:-}" ] && [ ! -f "${PGDATA}/PG_VERSION" ]; then
  PWFILE="$(mktemp)"
  printf '%s' "${POSTGRES_PASSWORD}" > "${PWFILE}"
  initdb -D "${PGDATA}" --username=postgres --pwfile="${PWFILE}" --data-checksums
  rm -f "${PWFILE}"

  cat >> "${PGDATA}/postgresql.conf" <<CONF
listen_addresses = '*'
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
ssl = on
ssl_cert_file = '/tmp/pg-tls/node.crt'
ssl_key_file = '/tmp/pg-tls/node.key'
CONF
  cp /etc/postgresql/pg_hba.conf "${PGDATA}/pg_hba.conf"
fi

exec vk-agent /etc/vk-agent/config.toml
