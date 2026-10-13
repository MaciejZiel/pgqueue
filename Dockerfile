# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.11.33 AS uv

FROM python:3.12-slim AS app
COPY --from=uv /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    PYTHONUNBUFFERED=1
WORKDIR /app

# Dependencies first, so code changes do not invalidate this layer.
COPY pyproject.toml uv.lock README.md LICENSE ./
RUN uv sync --locked --no-dev --extra admin --no-install-project

COPY src ./src
COPY examples ./examples
COPY demo ./demo
RUN uv sync --locked --no-dev --extra admin

RUN useradd --system --uid 10001 pgqueue
USER pgqueue
CMD ["pgqueue", "--help"]
