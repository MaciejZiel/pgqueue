# Third-party licenses

Every direct dependency of pgqueue (runtime, optional `admin` extra, development
tooling, build backend, CI actions and container images). All of them use
permissive licenses.

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

## Notes

- The PostgreSQL driver is asyncpg; psycopg (LGPL-3.0) is deliberately not used.
- Transitive dependencies were reviewed as well. Two are MPL-2.0 and stay out of
  anything pgqueue ships: `certifi` (pulled in by httpx, which is therefore not used --
  the admin tests use a small built-in ASGI client) and `pathspec`, a dependency of the
  mypy type checker used only in development and CI, never distributed with pgqueue.
