"""pgqueue: a background job queue built on PostgreSQL."""

from pgqueue.app import PgQueue, Task
from pgqueue.backoff import RetryPolicy
from pgqueue.errors import PayloadError, PermanentError, PgQueueError, Retry, UnknownTaskError
from pgqueue.models import Job, JobContext
from pgqueue.schema import JobStatus

__version__ = "0.1.0"

__all__ = [
    "Job",
    "JobContext",
    "JobStatus",
    "PayloadError",
    "PermanentError",
    "PgQueue",
    "PgQueueError",
    "Retry",
    "RetryPolicy",
    "Task",
    "UnknownTaskError",
    "__version__",
]
