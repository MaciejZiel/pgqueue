"""What the public, read-only dashboard may show about errors and workers.

The database keeps the full traceback and the real worker id (``host:pid:random``);
an operator needs both. A public viewer needs neither, and both leak the machine:
file system layout, interpreter version, host name and process ids.
"""

from __future__ import annotations

import hashlib
import re

# An absolute POSIX path (two or more segments) or a Windows drive path, at a word start.
_PATH = re.compile(r"(?<![\w.:/-])(?:/[\w.+@~%=-]+){2,}/?|\b[A-Za-z]:\\[^\s'\"]+")
# host:pid:random, the shape of a worker id (also embedded in "lease expired: worker ...").
_WORKER_ID = re.compile(r"[\w.-]+:\d+:[0-9a-f]{6}\b")
MAX_PUBLIC_ERROR = 300


def worker_label(worker_id: str) -> str:
    """A short label that is stable for a worker id but reveals nothing about the host."""
    return "w-" + hashlib.sha256(worker_id.encode()).hexdigest()[:6]


def public_error(error: str | None) -> str | None:
    """The last line of ``error`` (exception type and message) without paths or worker ids."""
    if not error:
        return None
    lines = error.strip().splitlines()
    line = lines[-1].strip() if lines else ""
    line = _WORKER_ID.sub(lambda m: worker_label(m.group()), line)
    line = _PATH.sub("<path>", line)
    if len(line) > MAX_PUBLIC_ERROR:
        line = line[: MAX_PUBLIC_ERROR - 3] + "..."
    return line or None
