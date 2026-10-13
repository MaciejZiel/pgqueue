#!/data/data/com.termux/files/usr/bin/bash
# Builds the virtual environment next to the checkout. Safe to run again after an update.
# Needs: pkg install python postgresql clang make rust libffi openssl
set -euo pipefail
cd "$(dirname "$0")/../.."

python -m venv .venv
.venv/bin/python -m pip install --upgrade pip
# asyncpg and greenlet have no Android wheels: pip compiles them with clang (a few minutes).
.venv/bin/python -m pip install ".[admin]"
echo "installed: $(.venv/bin/python -c 'import pgqueue; print(pgqueue.__version__)')"
