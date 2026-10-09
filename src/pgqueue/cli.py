"""Command line interface: ``pgqueue migrate`` and ``pgqueue worker``."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
import os
import sys
from collections.abc import Sequence

from pgqueue.app import PgQueue
from pgqueue.migrate import upgrade
from pgqueue.worker import Worker

log = logging.getLogger("pgqueue.cli")


def load_app(path: str) -> PgQueue:
    """Import ``package.module:attribute`` and return the PgQueue it names."""
    module_name, _, attribute = path.partition(":")
    if not module_name or not attribute:
        raise SystemExit(f"expected MODULE:ATTRIBUTE, got {path!r}")
    if os.getcwd() not in sys.path:
        sys.path.insert(0, os.getcwd())
    module = importlib.import_module(module_name)
    app = getattr(module, attribute, None)
    if not isinstance(app, PgQueue):
        raise SystemExit(f"{path} is not a PgQueue instance")
    return app


def _dsn(value: str | None) -> str:
    dsn = value or os.environ.get("PGQUEUE_DSN")
    if not dsn:
        raise SystemExit("no database: pass --dsn or set PGQUEUE_DSN")
    return dsn


async def run_worker(worker: Worker) -> None:
    """Run a worker with SIGTERM/SIGINT wired to a graceful drain."""
    worker.install_signal_handlers()
    try:
        await worker.run()
    finally:
        await worker.app.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pgqueue", description=__doc__)
    parser.add_argument("--log-level", default=os.environ.get("PGQUEUE_LOG_LEVEL", "INFO"))
    commands = parser.add_subparsers(dest="command", required=True)

    migrate = commands.add_parser("migrate", help="create or upgrade the pgqueue tables")
    migrate.add_argument("--dsn", help="database URL (default: $PGQUEUE_DSN)")
    migrate.add_argument("--revision", default="head")

    worker = commands.add_parser("worker", help="process jobs")
    worker.add_argument("app", help="the PgQueue application, e.g. myproject.jobs:app")
    worker.add_argument("-q", "--queues", default="default", help="comma-separated queues")
    worker.add_argument("-c", "--concurrency", type=int, default=10)
    worker.add_argument("--lease", type=float, default=30.0, help="lease length in seconds")
    worker.add_argument("--poll-interval", type=float, default=1.0)
    worker.add_argument("--shutdown-timeout", type=float, default=30.0)
    worker.add_argument("--no-listen", action="store_true", help="poll only, no LISTEN/NOTIFY")
    worker.add_argument("--no-periodic", action="store_true", help="do not fire periodic schedules")
    worker.add_argument(
        "--metrics-port", type=int, help="export Prometheus worker metrics on this port"
    )

    admin = commands.add_parser("admin", help="serve the admin API, dashboard and /metrics")
    admin.add_argument("--dsn", help="database URL (default: $PGQUEUE_DSN)")
    admin.add_argument("--host", default="127.0.0.1")
    admin.add_argument("--port", type=int, default=8080)
    return parser


def serve_admin(dsn: str, host: str, port: int) -> None:
    try:
        import uvicorn  # noqa: PLC0415 - optional dependency

        from pgqueue.admin import create_app  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover
        raise SystemExit("the admin needs the extra: pip install 'pgqueue[admin]'") from exc
    uvicorn.run(create_app(dsn), host=host, port=port, log_level="info", access_log=False)


def main(argv: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    if args.command == "migrate":
        upgrade(_dsn(args.dsn), args.revision)
        log.info("database is at revision %s", args.revision)
    elif args.command == "worker":
        app = load_app(args.app)
        worker = Worker(
            app,
            queues=[q.strip() for q in args.queues.split(",") if q.strip()],
            concurrency=args.concurrency,
            lease_seconds=args.lease,
            poll_interval=args.poll_interval,
            shutdown_timeout=args.shutdown_timeout,
            listen=not args.no_listen,
            run_periodic=not args.no_periodic,
        )
        if args.metrics_port:
            from pgqueue.prometheus import WorkerMetrics  # noqa: PLC0415 - optional extra

            WorkerMetrics(worker).serve(args.metrics_port)
        asyncio.run(run_worker(worker))
    elif args.command == "admin":
        serve_admin(_dsn(args.dsn), args.host, args.port)
