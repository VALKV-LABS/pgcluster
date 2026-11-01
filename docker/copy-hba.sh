#!/bin/sh
# Runs when the primary is first initialised. Nodes seeded with pg_basebackup get
# PGDATA's copy of pg_hba.conf, so it must allow what the shared file allows.
cp /etc/postgresql/pg_hba.conf "${PGDATA}/pg_hba.conf"
