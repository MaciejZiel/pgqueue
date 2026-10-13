#!/data/data/com.termux/files/usr/bin/bash
# Runs the migrations, then the dashboard plus the whole synthetic workload in one
# supervised process tree (python -m demo). Configuration comes from an env file.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ENV_FILE="${PGQUEUE_DEMO_ENV:-$HOME/.config/pgqueue-demo/env}"

if [ ! -f "$ENV_FILE" ]; then
  echo "missing $ENV_FILE: copy deploy/termux/env.example there and edit it" >&2
  exit 1
fi
set -a
# shellcheck disable=SC1090
. "$ENV_FILE"
set +a

cd "$ROOT"
exec "$ROOT/.venv/bin/python" -m demo
