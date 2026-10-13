"""``python -m demo``: migrate, then serve the dashboard with the workload attached."""

from __future__ import annotations

import logging

import uvicorn

from demo.config import Settings
from demo.server import build_app
from pgqueue.migrate import upgrade


def main() -> None:
    settings = Settings.from_env()
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s"
    )
    upgrade(settings.dsn)
    uvicorn.run(
        build_app(settings),
        host=settings.host,
        port=settings.port,
        root_path=settings.root_path,
        log_level="warning",
        access_log=False,
        timeout_graceful_shutdown=3,  # open event streams must not block shutdown
        proxy_headers=True,
        forwarded_allow_ips="127.0.0.1",
    )


if __name__ == "__main__":
    main()
