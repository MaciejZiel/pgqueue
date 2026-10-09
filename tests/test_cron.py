from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from pgqueue.cron import CronError, CronExpression


def at(text: str) -> datetime:
    return datetime.fromisoformat(text).replace(tzinfo=UTC)


@pytest.mark.parametrize(
    ("expression", "after", "expected"),
    [
        ("* * * * *", "2026-10-10 12:00:30", "2026-10-10 12:01"),
        ("*/15 * * * *", "2026-10-10 12:01", "2026-10-10 12:15"),
        ("*/15 * * * *", "2026-10-10 12:45", "2026-10-10 13:00"),
        ("0 * * * *", "2026-10-10 23:30", "2026-10-11 00:00"),
        ("30 2 * * *", "2026-10-10 02:30", "2026-10-11 02:30"),
        ("0 9-17/4 * * *", "2026-10-10 10:00", "2026-10-10 13:00"),
        ("0 0 1 * *", "2026-12-15 00:00", "2027-01-01 00:00"),
        ("0 0 29 2 *", "2026-03-01 00:00", "2028-02-29 00:00"),  # next leap day
        ("0 12 * * mon-fri", "2026-10-10 13:00", "2026-10-12 12:00"),  # Sat -> Mon
        ("0 0 * * 7", "2026-10-10 00:00", "2026-10-11 00:00"),  # 7 is Sunday too
        ("0 0 * jan,jul *", "2026-10-10 00:00", "2027-01-01 00:00"),
        ("5,10 0 * * *", "2026-10-10 00:05", "2026-10-10 00:10"),
        ("0 0 13 * fri", "2026-10-10 00:00", "2026-10-13 00:00"),  # either matches: 13th
        ("0 0 13 * fri", "2026-10-13 00:00", "2026-10-16 00:00"),  # ...or a Friday
        ("10/20 * * * *", "2026-10-10 12:31", "2026-10-10 12:50"),
        ("@hourly", "2026-10-10 12:00", "2026-10-10 13:00"),
        ("@daily", "2026-10-10 12:00", "2026-10-11 00:00"),
        ("@weekly", "2026-10-10 12:00", "2026-10-11 00:00"),
        ("@monthly", "2026-10-10 12:00", "2026-11-01 00:00"),
        ("@yearly", "2026-10-10 12:00", "2027-01-01 00:00"),
    ],
)
def test_next_after(expression: str, after: str, expected: str) -> None:
    assert CronExpression.parse(expression).next_after(at(after)) == at(expected)


def test_next_after_is_strictly_after_and_normalises_timezones() -> None:
    cron = CronExpression.parse("0 12 * * *")
    warsaw = timezone(timedelta(hours=2))
    noon_utc_in_warsaw = datetime(2026, 10, 10, 14, 0, tzinfo=warsaw)
    assert cron.next_after(noon_utc_in_warsaw) == at("2026-10-11 12:00")


def test_matches() -> None:
    cron = CronExpression.parse("*/5 8 * * *")
    assert cron.matches(at("2026-10-10 08:05"))
    assert not cron.matches(at("2026-10-10 08:06"))
    assert not cron.matches(at("2026-10-10 09:05"))


def test_sequence_of_ticks_is_monotonic_and_matching() -> None:
    cron = CronExpression.parse("7,37 */3 * * 1-5")
    moment = at("2026-01-01 00:00")
    for _ in range(500):
        nxt = cron.next_after(moment)
        assert nxt > moment
        assert cron.matches(nxt)
        moment = nxt


@pytest.mark.parametrize(
    "expression",
    [
        "* * * *",
        "60 * * * *",
        "* 24 * * *",
        "* * 0 * *",
        "* * * 13 *",
        "* * * * 8",
        "*/0 * * * *",
        "5-1 * * * *",
        "a * * * *",
        "1,,2 * * * *",
        "* * * foo *",
    ],
)
def test_invalid_expressions(expression: str) -> None:
    with pytest.raises(CronError):
        CronExpression.parse(expression)


def test_impossible_expression_is_reported() -> None:
    with pytest.raises(CronError, match="never matches"):
        CronExpression.parse("0 0 30 2 *").next_after(at("2026-01-01 00:00"))


def test_naive_datetimes_are_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        CronExpression.parse("* * * * *").next_after(datetime(2026, 1, 1))
