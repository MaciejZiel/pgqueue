"""Exceptions that are part of the public API."""

from __future__ import annotations


class PgQueueError(Exception):
    """Base class for every error raised by pgqueue itself."""


class UnknownTaskError(PgQueueError):
    """A job references a task name that is not registered in this process."""


class PayloadError(PgQueueError):
    """A payload does not match the task's declared pydantic model."""


class Retry(Exception):
    """Raise from a handler to request a retry, optionally after a specific delay.

    Use it when the handler knows better than the backoff policy, e.g. a
    ``429 Too Many Requests`` with a ``Retry-After`` header. The attempt still
    counts towards ``max_attempts``.
    """

    def __init__(self, reason: str = "retry requested", *, delay: float | None = None) -> None:
        super().__init__(reason)
        self.delay = delay


class PermanentError(Exception):
    """Raise from a handler for failures that retrying cannot fix.

    The job goes straight to ``failed`` without consuming further attempts.
    """
