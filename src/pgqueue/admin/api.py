"""The admin API: queue overview, job inspection and requeueing."""

from __future__ import annotations

import os
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

from fastapi import (
    APIRouter,
    Depends,
    FastAPI,
    HTTPException,
    Query,
    Request,
    status,
)
from fastapi import (
    Path as PathParam,
)
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    PlainTextResponse,
    Response,
    StreamingResponse,
)
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from prometheus_client import CONTENT_TYPE_LATEST
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from pgqueue import __version__
from pgqueue.admin import queries
from pgqueue.admin.live import LiveFeed, TooManySubscribers
from pgqueue.admin.metrics import render as render_metrics
from pgqueue.admin.schemas import JobOut, JobPage, Overview, RequeueResult, Snapshot
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

# Ids are bigint columns: anything larger would reach the driver and come back as a 500.
MAX_ID = 2**63 - 1
JobId = Annotated[int, PathParam(ge=1, le=MAX_ID)]


def _public(request: Request) -> bool:
    public: bool = request.app.state.read_only
    return public


def _feed(request: Request) -> LiveFeed:
    feed: LiveFeed = request.app.state.feed
    return feed


Feed = Annotated[LiveFeed, Depends(_feed)]

api = APIRouter(prefix="/api", dependencies=[Depends(_authenticate)])
# Everything that changes state lives on its own router, so a read-only
# deployment simply never mounts it: the endpoints do not exist at all.
actions = APIRouter(prefix="/api", dependencies=[Depends(_authenticate)])


@api.get("/overview")
async def get_overview(
    request: Request, engine: Engine, minutes: Annotated[int, Query(ge=1, le=24 * 60)] = 60
) -> Overview:
    """Counts per queue and status, queue lag, throughput, workers and schedules."""
    async with engine.connect() as conn:
        return await queries.overview(conn, minutes=minutes, redact=_public(request))


@api.get("/jobs")
async def list_jobs(
    request: Request,
    engine: Engine,
    *,
    status_: Annotated[str | None, Query(alias="status", description="comma-separated")] = None,
    queue: str | None = None,
    task: str | None = None,
    before_id: Annotated[int | None, Query(ge=1, le=MAX_ID)] = None,
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
            redact=_public(request),
        )


@api.get("/jobs/{job_id}")
async def get_job(request: Request, engine: Engine, job_id: JobId) -> JobOut:
    async with engine.connect() as conn:
        job = await queries.get_job(conn, job_id, redact=_public(request))
    if job is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "job not found")
    return job


@api.get("/live", response_model=Snapshot)
async def get_live(feed: Feed) -> Response:
    """The dashboard snapshot: counts, workers, leases, latency, throughput and a job feed.

    Computed at most once a second however many clients ask.
    """
    return Response(await feed.current(), media_type="application/json")


@api.get("/stream")
async def stream(request: Request, feed: Feed) -> StreamingResponse:
    """The same snapshots pushed as server-sent events, once a second."""

    async def events() -> AsyncIterator[bytes]:
        subscription = feed.subscribe()
        try:
            try:
                first = await anext(subscription)
            except TooManySubscribers:
                # Lost a race for the last slot after the check below: ask the browser to
                # come back later (it then gets the 503 and falls back to polling).
                yield b"retry: 15000\n\n"
                return
            yield b": keep-alive\n\n" if first is None else b"data: " + first + b"\n\n"
            async for payload in subscription:
                yield b": keep-alive\n\n" if payload is None else b"data: " + payload + b"\n\n"
        finally:
            await subscription.aclose()

    if feed.subscribers >= feed.max_subscribers:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "too many live viewers")
    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@actions.post("/jobs/{job_id}/requeue", dependencies=[Depends(_require_action_header)])
async def requeue_job(engine: Engine, job_id: JobId) -> JobOut:
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


@actions.post("/queues/{queue}/requeue", dependencies=[Depends(_require_action_header)])
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
    read_only: bool | None = None,
    banner: str | None = None,
    public: bool | None = None,
    read_only_dsn: str | None = None,
) -> FastAPI:
    """Build the admin application.

    Credentials default to ``PGQUEUE_ADMIN_USERNAME``/``PGQUEUE_ADMIN_PASSWORD``;
    when neither is set the admin is unauthenticated (fine behind a VPN or
    in the local demo, not on the open internet).

    ``read_only`` (default: ``PGQUEUE_ADMIN_READ_ONLY``) leaves out every
    endpoint that changes state, so the dashboard can be shown to the public.
    A read-only app also hides what only an operator needs: job errors are cut to
    their last line with absolute paths removed, and workers are shown under a
    short stable label instead of hostname and pid.
    ``read_only_dsn`` (default: ``PGQUEUE_READONLY_DSN``) is used instead of ``dsn``
    for a read-only app, so the public engine can connect as a role that can only SELECT.
    ``banner`` is an optional line of text shown at the top of the dashboard.

    ``public`` (default: ``PGQUEUE_ADMIN_PUBLIC``) is for an instance shown to anyone on
    the internet: ``/metrics`` (worker ids, queue names, failure counts) and the API docs
    (``/docs``, ``/redoc``, ``/openapi.json``) are not served at all. The dashboard and its
    ``/api`` endpoints, which the page itself needs, stay.
    """
    username = username or os.environ.get("PGQUEUE_ADMIN_USERNAME")
    password = password or os.environ.get("PGQUEUE_ADMIN_PASSWORD")
    if read_only is None:
        read_only = os.environ.get("PGQUEUE_ADMIN_READ_ONLY", "").lower() in ("1", "true", "yes")
    if public is None:
        public = os.environ.get("PGQUEUE_ADMIN_PUBLIC", "").lower() in ("1", "true", "yes")
    if read_only:
        dsn = read_only_dsn or os.environ.get("PGQUEUE_READONLY_DSN") or dsn
    engine = create_async_engine(sqlalchemy_url(dsn), pool_size=5, pool_pre_ping=True)
    feed = LiveFeed(engine, read_only=read_only)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        await feed.close()
        await engine.dispose()

    app = FastAPI(
        title="pgqueue admin",
        version=__version__,
        lifespan=lifespan,
        docs_url=None if public else "/docs",
        redoc_url=None if public else "/redoc",
        openapi_url=None if public else "/openapi.json",
    )
    app.state.engine = engine
    app.state.feed = feed
    app.state.read_only = read_only
    app.state.credentials = (username, password) if username and password else None
    app.include_router(api)
    if not read_only:
        app.include_router(actions)
    app.mount("/static", StaticFiles(directory=HERE / "static"), name="static")

    @app.get("/favicon.ico", include_in_schema=False)
    async def favicon() -> FileResponse:
        # Browsers request this on their own, without credentials and before reading the page.
        return FileResponse(
            HERE / "static" / "favicon.ico",
            media_type="image/x-icon",
            headers={"Cache-Control": "public, max-age=86400"},
        )

    @app.get("/", include_in_schema=False, dependencies=[Depends(_authenticate)])
    async def dashboard(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request,
            "dashboard.html",
            {"version": __version__, "read_only": read_only, "banner": banner},
        )

    if not public:

        @app.get(
            "/metrics",
            include_in_schema=False,
            dependencies=[Depends(_authenticate)],
            response_class=PlainTextResponse,
        )
        async def metrics() -> PlainTextResponse:
            async with engine.connect() as conn:
                body = await render_metrics(conn, redact=read_only)
            return PlainTextResponse(body, media_type=CONTENT_TYPE_LATEST)

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    return app
