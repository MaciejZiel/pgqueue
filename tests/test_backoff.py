from __future__ import annotations

import random

import pytest

from pgqueue.backoff import RetryPolicy


def test_ceiling_grows_exponentially_and_is_capped() -> None:
    policy = RetryPolicy(base=2, factor=3, max_delay=100, jitter=0)
    assert [policy.ceiling(n) for n in range(1, 6)] == [2, 6, 18, 54, 100]
    assert policy.ceiling(10_000) == 100


def test_jitter_stays_within_bounds() -> None:
    policy = RetryPolicy(base=1, factor=2, max_delay=600, jitter=0.5)
    rng = random.Random(7)
    for attempt in range(1, 12):
        ceiling = policy.ceiling(attempt)
        for _ in range(200):
            delay = policy.delay(attempt, rng)
            assert ceiling * 0.5 <= delay <= ceiling


def test_jitter_actually_spreads_retries() -> None:
    policy = RetryPolicy(base=10, jitter=1.0)
    rng = random.Random(1)
    delays = {round(policy.delay(1, rng), 3) for _ in range(100)}
    assert len(delays) > 90


def test_no_jitter_is_deterministic() -> None:
    policy = RetryPolicy(base=1.5, jitter=0)
    assert policy.delay(3) == 6.0


@pytest.mark.parametrize(
    "kwargs",
    [{"base": -1}, {"factor": 0.5}, {"jitter": 1.5}, {"jitter": -0.1}, {"max_delay": -1}],
)
def test_invalid_policies_are_rejected(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match="must be"):
        RetryPolicy(**kwargs)


def test_attempt_is_one_based() -> None:
    with pytest.raises(ValueError, match="1-based"):
        RetryPolicy().ceiling(0)
