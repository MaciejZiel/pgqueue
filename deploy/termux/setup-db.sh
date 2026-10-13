#!/data/data/com.termux/files/usr/bin/bash
# Creates the demo's own role and database on the shared PostgreSQL. Idempotent.
#   DEMO_DB_PASSWORD=... ./setup-db.sh
set -euo pipefail

: "${DEMO_DB_PASSWORD:?set DEMO_DB_PASSWORD to the password for the pgqueue_demo role}"
PSQL="${PSQL:-psql -v ON_ERROR_STOP=1 -d postgres}"

$PSQL -v pw="$DEMO_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE pgqueue_demo LOGIN PASSWORD %L', :'pw')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pgqueue_demo') \gexec
SELECT format('ALTER ROLE pgqueue_demo PASSWORD %L', :'pw') \gexec
SELECT 'CREATE DATABASE pgqueue_demo OWNER pgqueue_demo'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'pgqueue_demo') \gexec
SQL
echo "role and database pgqueue_demo are ready"
