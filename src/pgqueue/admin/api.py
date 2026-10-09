"""The admin API: queue overview, job inspection and requeueing."""

from __future__ import annotations

import os
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from pgqueue import __version__
from pgqueue.admin import queries
from pgqueue.admin.schemas import JobOut, JobPage, Overview, RequeueResult
from pgqueue.dsn import sqlalchemy_url
from pgqueue.schema import JobStatus

# Requests that change state must carry this header. Browsers do not add
# custom headers to cross-site form posts, so this blocks CSRF against a
# dashboard protected by (automatically resent) basic-auth credentials.
CSRF_HEADER = "X-PgQueue-Action"

_basic = HTTPBasic(auto_error=False)

HERE = Path(__file__).parent
templates = Jinja2Templates(directory=HERE / "templates")


def _engine(request: Request) -> AsyncEngine:
    engine: AsyncEngine = request.app.state.engine
    return engine


def _authenticate(
    request: Request,
    credentials: Annotated[HTTPBasicCredentials | None, Depends(_basic)],
) -> None:
    expected: tuple[str, str] | None = request.app.state.credentials
    if expected is None:
        return
    ok = credentials is not None and (
        secrets.compare_digest(credentials.username.encode(), expected[0].encode())
        & secrets.compare_digest(credentials.password.encode(), expected[1].encode())
    )
    if not ok:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "authentication required",
            headers={"WWW-Authenticate": 'Basic realm="pgqueue"'},
        )


def _require_action_header(request: Request) -> None:
    if request.headers.get(CSRF_HEADER) != "1":
        raise HTTPException(status.HTTP_403_FORBIDDEN, f"missing {CSRF_HEADER} header")


def _parse_statuses(raw: str | None) -> list[JobStatus]:
    if not raw:
        return []
    try:
        return [JobStatus(part.strip()) for part in raw.split(",") if part.strip()]
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc


Engine = Annotated[AsyncEngine, Depends(_engine)]

api = APIRouter(prefix="/api", dependencies=[Depends(_authenticate)])


@api.get("/overview")
async def get_overview(
    engine: Engine, minutes: Annotated[int, Query(ge=1, le=24 * 60)] = 60
) -> Overview:
    """Counts per queue and status, queue lag, throughput, workers and schedules."""
    async with engine.connect() as conn:
        return await queries.overview(conn, minutes=minutes)


@api.get("/jobs")
async def list_jobs(
    engine: Engine,
    *,
    status_: Annotated[str | None, Query(alias="status", description="comma-separated")] = None,
    queue: str | None = None,
    task: str | None = None,
    before_id: int | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
) -> JobPage:
    async with engine.connect() as conn:
        return await queries.list_jobs(
            conn,
            statuses=_parse_statuses(status_),
            queue=queue,
            task=task,
            before_id=before_id,
            limit=limit,
        )


@api.get("/jobs/{job_id}")
async def get_job(engine: Engine, job_id: int) -> JobOut:
    async with engine.connect() as conn:
        job = await queries.get_job(conn, job_id)
    if job is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    return job


@api.post("/jobs/{job_id}/requeue", dependencies=[Depends(_require_action_header)])
async def requeue_job(engine: Engine, job_id: int) -> JobOut:
    """Run a failed, dead or retrying job again now."""
    async with engine.begin() as conn:
        job = await queries.requeue_job(conn, job_id)
        if job is None:
            current = await queries.get_job(conn, job_id)
            if current is None:
                raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
            raise HTTPException(
                status.HTTP_409_CONFLICT, f"a {current.status.value} job cannot be requeued"
            )
    return job


@api.post("/queues/{queue}/requeue", dependencies=[Depends(_require_action_header)])
async def requeue_queue(
    engine: Engine, queue: str, status_: Annotated[JobStatus, Query(alias="status")]
) -> RequeueResult:
    """Requeue every failed, dead or retrying job of a queue, e.g. to drain the dead letters."""
    try:
        async with engine.begin() as conn:
            count = await queries.requeue_all(conn, queue=queue, status=status_)
    except ValueError as exc:
        raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
    return RequeueResult(requeued=count)


def create_app(
    dsn: str,
    *,
    username: str | None = None,
    password: str | None = None,
) -> FastAPI:
    """Build the admin application.

    Credentials default to ``PGQUEUE_ADMIN_USERNAME``/``PGQUEUE_ADMIN_PASSWORD``;
    when neither is set the admin is unauthenticated (fine behind a VPN or
    in the local demo, not on the open internet).
    """
    username = username or os.environ.get("PGQUEUE_ADMIN_USERNAME")
    password = password or os.environ.get("PGQUEUE_ADMIN_PASSWORD")
    engine = create_async_engine(sqlalchemy_url(dsn), pool_size=5, pool_pre_ping=True)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        await engine.dispose()

    app = FastAPI(title="pgqueue admin", version=__version__, lifespan=lifespan)
    app.state.engine = engine
    app.state.credentials = (username, password) if username and password else None
    app.include_router(api)
    app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")

    @app.get("/", include_in_schema=False, dependencies=[Depends(_authenticate)])
    async def dashboard(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(request, "dashboard.html", {"version": __version__})

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
