"""Admin API, dashboard and Prometheus metrics (install the ``admin`` extra)."""

from pgqueue.admin.api import create_app

__all__ = ["create_app"]
