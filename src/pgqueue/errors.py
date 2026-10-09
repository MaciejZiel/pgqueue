"""Exceptions that are part of the public API."""

from __future__ import annotations


class PgQueueError(Exception):
    """Base class for every error raised by pgqueue itself."""


class UnknownTaskError(PgQueueError):
    """A job references a task name that is not registered in this process."""


class PayloadError(PgQueueError):
    """A payload does not match the task's declared pydantic model."""
