#!/data/data/com.termux/files/usr/bin/bash
# Creates the demo's roles and database on the shared PostgreSQL. Idempotent.
#   DEMO_DB_PASSWORD=... DEMO_RO_PASSWORD=... ./setup-db.sh
#
# pgqueue_demo     owns the database; runs migrations and the workload. At most 40 connections.
# pgqueue_demo_ro  can only SELECT (and is read-only by default); give its DSN to the public
#                  dashboard as PGQUEUE_READONLY_DSN. Skipped when DEMO_RO_PASSWORD is unset.
set -euo pipefail

: "${DEMO_DB_PASSWORD:?set DEMO_DB_PASSWORD to the password for the pgqueue_demo role}"
DEMO_RO_PASSWORD="${DEMO_RO_PASSWORD:-}"
DEMO_CONNECTION_LIMIT="${DEMO_CONNECTION_LIMIT:-40}"
DEMO_RO_CONNECTION_LIMIT="${DEMO_RO_CONNECTION_LIMIT:-10}"
PSQL="${PSQL:-psql -v ON_ERROR_STOP=1 -d postgres}"

$PSQL -v pw="$DEMO_DB_PASSWORD" -v conn_limit="$DEMO_CONNECTION_LIMIT" <<'SQL'
SELECT format('CREATE ROLE pgqueue_demo LOGIN PASSWORD %L', :'pw')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pgqueue_demo') \gexec
SELECT format('ALTER ROLE pgqueue_demo PASSWORD %L CONNECTION LIMIT %s', :'pw', :'conn_limit') \gexec
SELECT 'CREATE DATABASE pgqueue_demo OWNER pgqueue_demo'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'pgqueue_demo') \gexec
-- By default every role may connect to every database. Only the demo roles need this one.
REVOKE CONNECT ON DATABASE pgqueue_demo FROM PUBLIC;
SQL

if [ -n "$DEMO_RO_PASSWORD" ]; then
  $PSQL -v pw="$DEMO_RO_PASSWORD" -v conn_limit="$DEMO_RO_CONNECTION_LIMIT" <<'SQL'
SELECT format('CREATE ROLE pgqueue_demo_ro LOGIN PASSWORD %L', :'pw')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pgqueue_demo_ro') \gexec
SELECT format('ALTER ROLE pgqueue_demo_ro PASSWORD %L CONNECTION LIMIT %s', :'pw', :'conn_limit') \gexec
ALTER ROLE pgqueue_demo_ro SET default_transaction_read_only = on;
GRANT CONNECT ON DATABASE pgqueue_demo TO pgqueue_demo_ro;
\connect pgqueue_demo
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO pgqueue_demo_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO pgqueue_demo_ro;
-- Tables that the migrations create later, as pgqueue_demo, are readable too.
ALTER DEFAULT PRIVILEGES FOR ROLE pgqueue_demo IN SCHEMA public GRANT SELECT ON TABLES TO pgqueue_demo_ro;
SQL
  echo "read-only role pgqueue_demo_ro is ready: set PGQUEUE_READONLY_DSN to use it"
else
  echo "DEMO_RO_PASSWORD not set: skipped the read-only role (the dashboard then uses PGQUEUE_DSN)"
fi
echo "role and database pgqueue_demo are ready (connection limit $DEMO_CONNECTION_LIMIT)"
