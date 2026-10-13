# Third-party licenses

Every direct dependency of pgqueue (runtime, optional `admin` extra, development
tooling, build backend, CI actions and container images). All of them use
permissive licenses. One transitive development-only dependency with a weak copyleft
license is listed separately below.

| source | name | license type | license link |
|---|---|---|---|
| https://pypi.org/project/asyncpg/ | asyncpg | Apache-2.0 | https://github.com/MagicStack/asyncpg/blob/master/LICENSE |
| https://pypi.org/project/SQLAlchemy/ | SQLAlchemy | MIT | https://github.com/sqlalchemy/sqlalchemy/blob/main/LICENSE |
| https://pypi.org/project/greenlet/ | greenlet (via `sqlalchemy[asyncio]`) | MIT AND PSF-2.0 | https://github.com/python-greenlet/greenlet/blob/master/LICENSE |
| https://pypi.org/project/alembic/ | alembic | MIT | https://github.com/sqlalchemy/alembic/blob/main/LICENSE |
| https://pypi.org/project/pydantic/ | pydantic | MIT | https://github.com/pydantic/pydantic/blob/main/LICENSE |
| https://pypi.org/project/fastapi/ | fastapi | MIT | https://github.com/fastapi/fastapi/blob/master/LICENSE |
| https://pypi.org/project/uvicorn/ | uvicorn | BSD-3-Clause | https://github.com/Kludex/uvicorn/blob/main/LICENSE.md |
| https://pypi.org/project/Jinja2/ | Jinja2 | BSD-3-Clause | https://github.com/pallets/jinja/blob/main/LICENSE.txt |
| https://pypi.org/project/prometheus-client/ | prometheus-client | Apache-2.0 AND BSD-2-Clause | https://github.com/prometheus/client_python/blob/master/LICENSE |
| https://pypi.org/project/pytest/ | pytest | MIT | https://github.com/pytest-dev/pytest/blob/main/LICENSE |
| https://pypi.org/project/pytest-asyncio/ | pytest-asyncio | Apache-2.0 | https://github.com/pytest-dev/pytest-asyncio/blob/main/LICENSE |
| https://pypi.org/project/ruff/ | ruff | MIT | https://github.com/astral-sh/ruff/blob/main/LICENSE |
| https://pypi.org/project/mypy/ | mypy | MIT | https://github.com/python/mypy/blob/master/LICENSE |
| https://pypi.org/project/hatchling/ | hatchling | MIT | https://github.com/pypa/hatch/blob/master/LICENSE.txt |
| https://github.com/actions/checkout | actions/checkout | MIT | https://github.com/actions/checkout/blob/main/LICENSE |
| https://github.com/actions/setup-python | actions/setup-python | MIT | https://github.com/actions/setup-python/blob/main/LICENSE |
| https://github.com/astral-sh/setup-uv | astral-sh/setup-uv | MIT | https://github.com/astral-sh/setup-uv/blob/main/LICENSE |
| https://hub.docker.com/_/postgres | postgres (container image) | PostgreSQL | https://www.postgresql.org/about/licence/ |
| https://hub.docker.com/_/python | python (container image, demo only) | PSF-2.0 | https://docs.python.org/3/license.html |
| https://github.com/astral-sh/uv | uv (container image, build stage of the demo) | MIT OR Apache-2.0 | https://github.com/astral-sh/uv/blob/main/LICENSE-MIT |
| https://pypi.org/project/celery/ | celery (benchmark comparison only, `bench` group) | BSD-3-Clause | https://github.com/celery/celery/blob/main/LICENSE |
| https://pypi.org/project/redis/ | redis-py (benchmark comparison only, `bench` group) | MIT | https://github.com/redis/redis-py/blob/master/LICENSE |
| https://hub.docker.com/r/valkey/valkey | valkey (container image, benchmark comparison only) | BSD-3-Clause | https://github.com/valkey-io/valkey/blob/unstable/COPYING |

## Development-only transitive dependencies

The `pathspec` row is installed only by `uv sync --all-extras` for local development and
CI. The rest belong to the optional `bench` dependency group (`uv sync --group bench`),
which only `make bench` uses for the Celery comparison. None of them are runtime
dependencies, none are imported by pgqueue and none are distributed with it (neither in
the package nor in the demo image).

| source | name | license type | license link |
|---|---|---|---|
| https://pypi.org/project/pathspec/ | pathspec 1.1.1 (transitive, via mypy; dev/CI only, not distributed) | MPL-2.0 | https://github.com/cpburnz/python-pathspec/blob/master/LICENSE |
| https://pypi.org/project/kombu/ | kombu (transitive, via celery; benchmark only) | BSD-3-Clause | https://github.com/celery/kombu/blob/main/LICENSE |
| https://pypi.org/project/amqp/ | amqp (transitive, via kombu; benchmark only) | BSD-3-Clause | https://github.com/celery/py-amqp/blob/main/LICENSE |
| https://pypi.org/project/billiard/ | billiard (transitive, via celery; benchmark only) | BSD-3-Clause | https://github.com/celery/billiard/blob/main/LICENSE.txt |
| https://pypi.org/project/vine/ | vine (transitive, via celery; benchmark only) | BSD-3-Clause | https://github.com/celery/vine/blob/main/LICENSE |
| https://pypi.org/project/click-didyoumean/ | click-didyoumean (transitive, via celery; benchmark only) | MIT | https://github.com/click-contrib/click-didyoumean/blob/master/LICENSE |
| https://pypi.org/project/click-plugins/ | click-plugins (transitive, via celery; benchmark only) | BSD-3-Clause | https://github.com/click-contrib/click-plugins/blob/master/LICENSE.txt |
| https://pypi.org/project/click-repl/ | click-repl (transitive, via celery; benchmark only) | MIT | https://github.com/click-contrib/click-repl/blob/main/LICENSE |
| https://pypi.org/project/prompt-toolkit/ | prompt-toolkit (transitive, via click-repl; benchmark only) | BSD-3-Clause | https://github.com/prompt-toolkit/python-prompt-toolkit/blob/main/LICENSE |
| https://pypi.org/project/wcwidth/ | wcwidth (transitive, via prompt-toolkit; benchmark only) | MIT | https://github.com/jquast/wcwidth/blob/master/LICENSE |
| https://pypi.org/project/python-dateutil/ | python-dateutil (transitive, via celery; benchmark only) | Apache-2.0 AND BSD-3-Clause | https://github.com/dateutil/dateutil/blob/master/LICENSE |
| https://pypi.org/project/six/ | six (transitive, via python-dateutil; benchmark only) | MIT | https://github.com/benjaminp/six/blob/main/LICENSE |
| https://pypi.org/project/tzlocal/ | tzlocal (transitive, via celery; benchmark only) | MIT | https://github.com/regebro/tzlocal/blob/master/LICENSE.txt |
| https://pypi.org/project/tzdata/ | tzdata (transitive, via celery and kombu; benchmark only) | Apache-2.0 | https://github.com/python/tzdata/blob/master/LICENSE |

## Notes

- The PostgreSQL driver is asyncpg; psycopg (LGPL-3.0) is deliberately not used.
- Transitive dependencies were reviewed as well. Two are MPL-2.0 and stay out of
  anything pgqueue ships: `certifi` (pulled in by httpx, which is therefore not used --
  the admin tests use a small built-in ASGI client) and `pathspec`, a dependency of the
  mypy type checker used only in development and CI, never distributed with pgqueue.
