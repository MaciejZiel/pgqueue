"""A small, dependency-free parser for standard five-field cron expressions.

Supported syntax (all times are UTC):

* fields: ``minute hour day-of-month month day-of-week``
* ``*``, single values, ranges ``a-b``, lists ``a,b,c`` and steps ``*/n``,
  ``a-b/n`` and ``a/n`` (from ``a`` to the end of the range)
* month names ``jan``..``dec`` and weekday names ``sun``..``sat``;
  day-of-week ``0`` and ``7`` both mean Sunday
* macros ``@yearly`` (``@annually``), ``@monthly``, ``@weekly``, ``@daily``
  (``@midnight``) and ``@hourly``

Like Vixie cron, when both day-of-month and day-of-week are restricted a day
matches if *either* matches (``0 0 13 * fri`` is "the 13th and every Friday").
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

MACROS = {
    "@yearly": "0 0 1 1 *",
    "@annually": "0 0 1 1 *",
    "@monthly": "0 0 1 * *",
    "@weekly": "0 0 * * 0",
    "@daily": "0 0 * * *",
    "@midnight": "0 0 * * *",
    "@hourly": "0 * * * *",
}

MONTHS = {
    name: i
    for i, name in enumerate(
        ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"],
        start=1,
    )
}
WEEKDAYS = {name: i for i, name in enumerate(["sun", "mon", "tue", "wed", "thu", "fri", "sat"])}

# Searching further than this without a match means the expression can never
# fire (e.g. "0 0 30 2 *", February 30th).
_SEARCH_LIMIT = timedelta(days=366 * 5)


class CronError(ValueError):
    """The expression is not valid cron syntax."""


def _parse_value(token: str, lo: int, hi: int, names: dict[str, int]) -> int:
    value = names.get(token.lower()) if names else None
    if value is None:
        if not token.isdigit():
            raise CronError(f"invalid value {token!r}")
        value = int(token)
    if not lo <= value <= hi:
        raise CronError(f"value {value} out of range {lo}-{hi}")
    return value


def _parse_field(
    field: str, lo: int, hi: int, names: dict[str, int] | None = None
) -> frozenset[int]:
    names = names or {}
    values: set[int] = set()
    for part in field.split(","):
        if not part:
            raise CronError(f"empty list item in {field!r}")
        base, _, step_text = part.partition("/")
        step = 1
        if step_text:
            if not step_text.isdigit() or int(step_text) == 0:
                raise CronError(f"invalid step {step_text!r}")
            step = int(step_text)
        if base == "*":
            start, end = lo, hi
        elif "-" in base:
            a, _, b = base.partition("-")
            start, end = _parse_value(a, lo, hi, names), _parse_value(b, lo, hi, names)
            if start > end:
                raise CronError(f"descending range {base!r}")
        else:
            start = _parse_value(base, lo, hi, names)
            end = hi if step_text else start
        values.update(range(start, end + 1, step))
    return frozenset(values)


@dataclass(frozen=True, slots=True)
class CronExpression:
    expression: str
    minutes: frozenset[int]
    hours: frozenset[int]
    days: frozenset[int]
    months: frozenset[int]
    weekdays: frozenset[int]  # 0 = Sunday
    day_restricted: bool
    weekday_restricted: bool

    @classmethod
    def parse(cls, expression: str) -> CronExpression:
        text = MACROS.get(expression.strip().lower(), expression)
        fields = text.split()
        if len(fields) != 5:
            raise CronError(f"expected 5 fields, got {len(fields)}: {expression!r}")
        minute, hour, day, month, weekday = fields
        weekdays = _parse_field(weekday, 0, 7, WEEKDAYS)
        if 7 in weekdays:
            weekdays = (weekdays - {7}) | {0}
        return cls(
            expression=expression,
            minutes=_parse_field(minute, 0, 59),
            hours=_parse_field(hour, 0, 23),
            days=_parse_field(day, 1, 31),
            months=_parse_field(month, 1, 12, MONTHS),
            weekdays=weekdays,
            day_restricted=not day.startswith("*"),
            weekday_restricted=not weekday.startswith("*"),
        )

    def _day_matches(self, moment: datetime) -> bool:
        day_ok = moment.day in self.days
        weekday_ok = (moment.isoweekday() % 7) in self.weekdays
        if self.day_restricted and self.weekday_restricted:
            return day_ok or weekday_ok
        return day_ok and weekday_ok

    def matches(self, moment: datetime) -> bool:
        moment = moment.astimezone(UTC)
        return (
            moment.minute in self.minutes
            and moment.hour in self.hours
            and moment.month in self.months
            and self._day_matches(moment)
        )

    def next_after(self, moment: datetime) -> datetime:
        """The first matching minute strictly after ``moment`` (returned in UTC)."""
        if moment.tzinfo is None:
            raise ValueError("moment must be timezone-aware")
        current = moment.astimezone(UTC).replace(second=0, microsecond=0) + timedelta(minutes=1)
        deadline = current + _SEARCH_LIMIT
        # Skip whole months, days and hours that cannot match instead of
        # walking minute by minute: at most a few hundred iterations.
        while current < deadline:
            if current.month not in self.months:
                year, month = divmod(current.month, 12)
                current = current.replace(
                    year=current.year + year, month=month + 1, day=1, hour=0, minute=0
                )
                continue
            if not self._day_matches(current):
                current = (current + timedelta(days=1)).replace(hour=0, minute=0)
                continue
            if current.hour not in self.hours:
                current = (current + timedelta(hours=1)).replace(minute=0)
                continue
            if current.minute not in self.minutes:
                current += timedelta(minutes=1)
                continue
            return current
        raise CronError(f"{self.expression!r} never matches")
