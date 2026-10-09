"""What happens to a job after its handler raised."""

from __future__ import annotations

import traceback
from dataclasses import dataclass

from pydantic import ValidationError

from pgqueue.backoff import RetryPolicy
from pgqueue.errors import PayloadError, PermanentError, Retry
from pgqueue.models import Job
from pgqueue.schema import JobStatus

MAX_ERROR_LENGTH = 8_000

# Errors that a retry cannot possibly fix.
NON_RETRYABLE: tuple[type[BaseException], ...] = (PermanentError, PayloadError, ValidationError)


@dataclass(frozen=True, slots=True)
class FailureOutcome:
    status: JobStatus
    retry_in: float = 0.0


def decide_failure(job: Job, exc: BaseException, policy: RetryPolicy) -> FailureOutcome:
    """Map an exception from attempt ``job.attempts`` to the job's next state.

    * non-retryable errors (``PermanentError``, invalid payload) -> ``failed``
    * attempts exhausted -> ``dead`` (the dead-letter state)
    * otherwise -> ``retrying`` after the policy's backoff, or after the delay
      carried by an explicit :class:`Retry`.
    """
    if isinstance(exc, NON_RETRYABLE):
        return FailureOutcome(JobStatus.FAILED)
    if job.attempts >= job.max_attempts:
        return FailureOutcome(JobStatus.DEAD)
    if isinstance(exc, Retry) and exc.delay is not None:
        return FailureOutcome(JobStatus.RETRYING, max(0.0, exc.delay))
    return FailureOutcome(JobStatus.RETRYING, policy.delay(job.attempts))


def format_error(exc: BaseException) -> str:
    """A traceback for ``last_error``, keeping the end (the useful part) if long."""
    text = "".join(traceback.format_exception(exc)).rstrip()
    if len(text) > MAX_ERROR_LENGTH:
        text = "...\n" + text[-MAX_ERROR_LENGTH:]
    return text
