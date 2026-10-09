"""Retry delays: capped exponential backoff with jitter."""

from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """How long to wait before attempt ``n + 1`` after attempt ``n`` failed.

    The un-jittered delay is ``base * factor ** (attempt - 1)``, capped at
    ``max_delay``. Jitter then removes a random fraction (up to ``jitter``) of
    that delay. Without jitter, a burst of jobs that failed together (say,
    because a downstream API was down for a minute) would all retry at the
    same instant and knock it over again; spreading them out avoids that
    thundering herd. Keeping a floor of ``(1 - jitter) * delay`` -- rather
    than AWS-style "full jitter" -- guarantees that backoff still grows.
    """

    base: float = 1.0
    factor: float = 2.0
    max_delay: float = 600.0
    jitter: float = 0.5

    def __post_init__(self) -> None:
        if self.base < 0 or self.max_delay < 0:
            raise ValueError("delays must be non-negative")
        if self.factor < 1:
            raise ValueError("factor must be >= 1")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be within [0, 1]")

    def ceiling(self, attempt: int) -> float:
        """The delay before jitter for the retry after ``attempt`` (1-based)."""
        if attempt < 1:
            raise ValueError("attempt is 1-based")
        # Cap the exponent too, so attempt=10_000 does not overflow a float.
        exponent = min(attempt - 1, 64)
        return float(min(self.max_delay, self.base * self.factor**exponent))

    def delay(self, attempt: int, rng: random.Random | None = None) -> float:
        ceiling = self.ceiling(attempt)
        draw = (rng or random).random()
        return ceiling * (1 - self.jitter * draw)


DEFAULT_RETRY_POLICY = RetryPolicy()
