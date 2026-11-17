"""Date parsing and the day rules of `history()` and the time-axis filters.

Every time bound a user passes is a `DateLike`: an ISO-8601 string, a `date`, a timezone-aware
`datetime` (a `pandas.Timestamp` among them), one of the Java time values Deephaven hands its
users (`Instant`, `ZonedDateTime`, `LocalDate`), or a `numpy.datetime64` (a day, or an instant
in UTC). The API layer turns Deephaven's own date and time text into its Java value first
(`_tables/time_text.py`), since only Deephaven's parser reads it. `parse_date_like` turns each
into a `date` or an aware `datetime`, the only two forms the rest of the library sees. A bound
with a time of day but no timezone is ambiguous across the three day zones Massive uses, so it
is rejected with a `ValueError` that names the parameter, the value, and the fix
(`require_aware`).

A Java value is recognized by its class name, so the dataset layer imports neither Deephaven
nor jpy, and reading its fields calls only that immutable `java.time` object, which takes no
Deephaven lock. An instant from Java, numpy, or pandas can hold nanoseconds a `datetime`
cannot; it is rounded outward to a microsecond, down for a lower bound and up for an upper
one, so no row is lost.

A range has two kinds of bounds, and one range never mixes them (`check_order`):

- **Dates** are whole days, inclusive at both ends, so `history("2025-03-03", "2025-03-03")`
  is the whole of March 3 (`DaySpan`). A day is a calendar day in the asset class's *day
  zone*: America/New_York for stocks, options, and indices, UTC for forex and crypto. A
  futures date names the trading session whose `session_end_date` equals it, since a session
  is the futures trading day.
- **Aware datetimes** are exact instants, a half-open range `[start, end)` (`TimeRange`).
  Equal instants are an empty range, not an error.

A date and a datetime cannot be mixed: a date means a whole day or session, a datetime one
instant, and a futures session's first instant moves with the holiday calendar, which the
library does not have. An `end` before `start` raises. An `end` of None means "through now":
the current instant for a datetime `start`, and for a date `start` the current day in the
day zone (for futures, the session the current instant belongs to). `bounds` applies these
rules to a dataset's `history()` arguments, and `reaches_now` tells whether a window reaches
the present.

`day_start` and `instant_bound` turn a date sent to a REST parameter that compares instants
into its instant, and `day_end` gives the instant a day is over by. Like `midnight`, they
return their instants in UTC, whatever the zone whose day they start or end.

A futures date names a trading session, whose 17:00 Chicago rule is `futures_sessions`; the
bars of a timespan that fit a range are `bar_periods`.

Every function is thread-safe and pure (the current time is a parameter), apart from the
DEBUG lines `parse_date_like` logs when it rounds an instant from Java, numpy, or pandas, or
reads a numpy time as UTC.
"""

from __future__ import annotations

import calendar
import contextlib
import logging
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta, timezone, tzinfo
from typing import TYPE_CHECKING, Any, Final, Protocol, TypeAlias, cast, runtime_checkable
from zoneinfo import ZoneInfo

import numpy as np

from deephaven_massive._datasets.epoch import EPOCH, epoch_ns
from deephaven_massive._datasets.futures_sessions import (
    CHICAGO,
    futures_session,
    futures_session_end,
    futures_session_open,
)
from deephaven_massive._enums import AssetClass, ClassFromPrefix
from deephaven_massive._massive import WireUnit

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from deephaven_massive._datasets.catalog import DatasetSpec

__all__ = [
    "DAY_ZONES",
    "DAY_ZONE_NAMES",
    "MIXED_BOUNDS",
    "NEW_YORK",
    "DateLike",
    "DaySpan",
    "TimeRange",
    "TimeTextError",
    "bounds",
    "calendar_day",
    "check_order",
    "day_class",
    "day_end",
    "day_start",
    "day_zone",
    "instant_bound",
    "is_trading_day",
    "midnight",
    "parse_date_like",
    "reaches_now",
    "require_aware",
    "trades_at_weekends",
]

DateLike: TypeAlias = str | date | datetime | np.datetime64
"""A time bound: `history()`'s `start` and `end`, a list's `start`, `end`, and `as_of`, a day.

Every argument that names a date or an instant takes any of these forms, so a caller can pass
text, a Python or Java time, or a numpy or pandas one, whichever their code holds. Annotate your
own code with it, as `dm.DateLike`, when you pass a time through to a dataset method. One of:

- text, read by one of two parsers:
  - ISO-8601: a date in any ISO form (`"2025-03-03"`, `"20250303"`, `"2025-W10-1"`), or a
    date and time with an offset (`"2025-03-03T09:30:00-05:00"`), whose
    digits below the microsecond are rounded outward like a Java instant's;
  - otherwise Deephaven's own date text, as `deephaven.time.to_j_local_date` reads it
    (`"2025-3-3"`), or its date and time text with a zone, as `to_j_zdt` reads it
    (`"2025-03-03T09:30 America/New_York"`, `"2025-03-03T09:30:00 ET"`). Deephaven text that
    names a day and a zone but no time (`"2025-03-03 ET"`) is refused, since Deephaven reads it
    as that midnight. Deephaven's instant parser is never used: it reads bare digits as an epoch
    count;
- a `date`, or a timezone-aware `datetime`, a `pandas.Timestamp` among them (its nanoseconds
  are kept and moved outward to a microsecond like a Java instant's);
- a Java `java.time.LocalDate` (a date), or a Java `Instant` or `ZonedDateTime` (an instant,
  moved outward to a microsecond: a start down, an end up), the values Deephaven's own time
  functions return, such as `deephaven.time.dh_now()` and `to_j_local_date("2025-03-03")`, and
  `Instant` and `LocalDate` cells read as Java values (through `iter_dict`, for one).
  `dh_today()` returns ISO text, the first form;
- a `numpy.datetime64`: a date at day precision (`numpy.datetime64("2025-03-03")`), otherwise
  an instant in UTC, as Deephaven reads it (`to_numpy` gives an `Instant` column so), moved
  outward to a microsecond like a Java instant. A week, month, or year is refused: as an `end`
  it would shrink the range to its first day, and a numpy week starts on a Thursday.

These are the inputs Deephaven's own `deephaven.time` converters take, except a bare integer
(epoch nanoseconds there, which would silently misread a date written as `20250303`) and text
Deephaven reads only as such a count.

Type checkers see Deephaven's Java values as `Any`, so the alias does not name them. A naive
`datetime` or `pandas.Timestamp`, or an ISO string with a time but no UTC offset, raises a
`ValueError` that names the parameter and the fix ("needs a timezone: pass an aware datetime or
a date"), since a time without a zone cannot be placed. A date-only value is a whole day in the
asset class's day zone.
"""


logger = logging.getLogger(__name__)

NEW_YORK: Final = ZoneInfo("America/New_York")
"""The day zone of stocks, options, and indices."""

DAY_ZONES: Final[Mapping[AssetClass, tzinfo]] = {
    AssetClass.STOCKS: NEW_YORK,
    AssetClass.OPTIONS: NEW_YORK,
    AssetClass.INDICES: NEW_YORK,
    AssetClass.FOREX: UTC,
    AssetClass.CRYPTO: UTC,
}
"""The zone whose midnights bound a date-only `start` or `end`, per asset class.

Futures are absent: a futures date names a trading session, not a calendar day (see `bounds`).
Datasets without an asset class use New York."""

DAY_ZONE_NAMES: Final[Mapping[tzinfo, str]] = {NEW_YORK: "New York time", UTC: "UTC"}
"""How user-facing text names each zone of `DAY_ZONES`."""

MIXED_BOUNDS: Final = "Pass two dates for whole days, or two datetimes for exact instants."
"""The fix that ends the message of a range that mixes a date and a datetime."""

# A fraction of a second finer than the microsecond, which `datetime.fromisoformat` truncates,
# in the extended (`09:30:00.1234567`) and the basic (`093000.1234567`) time form alike. Only a
# fraction of a second can hold a `.` or `,` in ISO-8601 text, so the separator marks it.
_ISO_DATE: Final = re.compile(r"^(\d{4}-.+|\d{4}W\d{3}|\d{8})$")
"""The shapes of an ISO-8601 calendar date: extended (with dashes), or basic in exactly eight
characters. `date.fromisoformat` alone also reads longer runs of digits, such as the epoch
count `"1741012200"` as 1741-01-22, which this keeps out."""
_FRACTION: Final = re.compile(r"[.,](\d+)")
"""The fraction of a second in ISO-8601 text; only a fraction can hold a `.` or `,`."""
_OUTSIDE_DATETIME: Final = "is not a time from year 1 to 9999. Pass a time in those years."
_JAVA_INSTANTS: Final = frozenset({"java.time.Instant", "java.time.ZonedDateTime"})
"""The Java classes whose values are instants, by their jpy class name."""
_NANOS: Final = 10**9
_NUMPY_DAY: Final = "D"
"""The `numpy.datetime64` unit of a calendar day, the one calendar unit a bound takes."""
_NUMPY_PERIODS: Final = frozenset({"W", "M", "Y"})
"""The `numpy.datetime64` units longer than a day, which a bound refuses: as an `end` they would
shrink the range to the period's first day, and a numpy week starts on a Thursday."""
_ATTOSECONDS: Final[Mapping[str, int]] = {
    "h": 3_600 * 10**18,
    "m": 60 * 10**18,
    "s": 10**18,
    "ms": 10**15,
    "us": 10**12,
    "ns": 10**9,
    "ps": 10**6,
    "fs": 10**3,
    "as": 1,
}
"""The length of each `numpy.datetime64` time unit in attoseconds, the finest unit numpy has."""
_ONE_DAY: Final = timedelta(days=1)
_INDEX_DAY_END: Final = timedelta(hours=1)
"""How much later an index day ends than the New York day: 01:00 New York time, midnight in
Chicago, since whether an index day bar covers the New York or the Chicago day is not
verified."""
_ONE_MICROSECOND: Final = timedelta(microseconds=1)
_WEEKEND_TRADING: Final = frozenset({AssetClass.FOREX, AssetClass.CRYPTO})
"""The classes that trade at weekends, whose bar periods keep their Saturdays and Sundays."""


@runtime_checkable
class _NanosecondTime(Protocol):
    """A `datetime` that also holds nanoseconds, as a `pandas.Timestamp` does."""

    def to_datetime64(self) -> np.datetime64:
        """Return the time as a `numpy.datetime64` in UTC, in its own unit."""
        ...


class TimeTextError(ValueError):
    """Text that is not ISO-8601, raised by `parse_date_like`.

    A `ValueError`, so callers that catch one keep working; its own class lets the API layer,
    which also reads Deephaven's date and time text, tell this failure from the others (a
    time without a timezone) and name both forms in one message.
    """


def parse_date_like(value: object, name: str, *, upper: bool = False) -> date | datetime:
    """Parse one time bound into a `date` or an aware `datetime`.

    Args:
        value: An ISO-8601 string (a date in any ISO form, `"2025-03-03"`, `"20250303"`, or
            `"2025-W10-1"`, or a date and time with an offset, `"2025-03-03T09:30:00-05:00"`), a
            `date`, a timezone-aware `datetime` (a `pandas.Timestamp` among them), a Java
            `LocalDate` (a date), a Java `Instant` or `ZonedDateTime` (an aware `datetime` at
            the same instant), or a `numpy.datetime64` (a date at day precision; otherwise an
            instant in UTC, since numpy times have no timezone). An instant finer than a
            microsecond, from ISO text, Java, numpy, or pandas, is rounded to a microsecond
            (`_outward`). A bare integer is no bound: Deephaven reads one as epoch nanoseconds,
            which would silently misread a date written as `20250303`.
        name: The parameter's name, used in error messages (`"start"`, `"end"`, `"date"`).
        upper: True for the upper bound of a range (`end`), which an instant between two
            microseconds is rounded up for, so the half-open range never loses a row; False,
            the default, rounds it down, so a lower bound never does.

    Returns:
        A `date` for a date-only value; otherwise an aware `datetime`.

    Raises:
        TypeError: If `value` is none of the accepted types.
        TimeTextError: If a string is not ISO-8601.
        ValueError: If a string has surrounding whitespace, which is never trimmed; the
            message names the trimmed text.

    Other values raise a `ValueError`: a naive `datetime` or `pandas.Timestamp`, or a string with
    a time but no offset (the message names the parameter, the value, and the fix;
    `require_aware`); a `pandas.NaT` or a `numpy.datetime64` that is `NaT`; a
    `numpy.datetime64` of a week, month, or year; or a Java, numpy, or pandas time outside
    years 1 to 9999.
    """
    if isinstance(value, datetime):
        if isinstance(value, _NanosecondTime):  # a pandas.Timestamp
            return _pandas_bound(value, value.to_datetime64(), name, upper=upper)
        parsed = value
    elif isinstance(value, date):
        return value
    elif isinstance(value, str):
        text = value
        if text != text.strip():
            raise ValueError(f"{name}={value!r} has surrounding whitespace. Pass {text.strip()!r}.")
        if _ISO_DATE.match(text):  # a calendar date: `2025-03-03`, `20250303`, `2025-W10-1`
            with contextlib.suppress(ValueError):
                return date.fromisoformat(text)
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            raise TimeTextError(
                f"{name}={value!r} is not an ISO-8601 date or datetime. Pass a date such as "
                "'2025-03-03', or a time with its offset such as '2025-03-03T09:30:00-05:00'."
            ) from None
        require_aware(name, value, parsed)
        return _outward(parsed, _cut_digits(text), name=name, value=value, upper=upper)
    else:
        found = _foreign_bound(value, name, upper=upper)
        if found is None:
            raise TypeError(
                f"{name} must be a str, date, datetime, numpy.datetime64, pandas.Timestamp, or "
                f"Java Instant, ZonedDateTime, or LocalDate, not {type(value).__name__}"
            )
        return found
    require_aware(name, value, parsed)
    return parsed


def _pandas_bound(value: datetime, utc: np.datetime64, name: str, *, upper: bool) -> datetime:
    """Parse a `pandas.Timestamp` bound, keeping the nanoseconds a `datetime` would drop.

    Args:
        value: The timestamp, a `datetime` subclass.
        utc: The timestamp's `to_datetime64()`: in UTC, in its own unit.
        name: The parameter's name, for messages.
        upper: True for an upper bound, rounded up; False for any other, rounded down.

    Returns:
        The aware UTC `datetime`, rounded to a microsecond (`_rounded`).

    Raises:
        ValueError: If `value` is `pandas.NaT`, or has no timezone (`require_aware`).

    A time outside years 1 to 9999 raises the `ValueError` of `_rounded`.
    """
    if np.isnat(utc):
        raise ValueError(f"{name}={value!r} is not a time. Pass a date or an aware datetime.")
    require_aware(name, value, value)
    unit, count = np.datetime_data(utc.dtype)
    return _rounded(
        int(utc.astype(np.int64)) * count * _ATTOSECONDS[unit], name, value, upper=upper
    )


def _foreign_bound(value: object, name: str, *, upper: bool) -> date | datetime | None:
    """Parse a time bound that is a Java time value or a `numpy.datetime64`.

    A Java value is recognized by its class name, which jpy sets as `jclassname` on every Java
    type it wraps, so jpy is not imported. An `Instant` becomes a UTC `datetime` and a
    `ZonedDateTime` one at its own offset; only the instant matters to a bound, so the zone's
    later rules are not needed. A `numpy.datetime64` of day precision is a date, and one of a
    time unit an instant in UTC, as Deephaven reads it (`to_numpy` gives an `Instant` column
    so), which logs at DEBUG since the value names no zone. An instant between two
    microseconds is rounded outward (`_rounded`).

    Args:
        value: The bound, which is not a `str`, `date`, or `datetime`.
        name: The parameter's name, for messages.
        upper: True for an upper bound, rounded up; False for any other, rounded down.

    Returns:
        A `date` for a `LocalDate` or a day-precision `numpy.datetime64`; an aware `datetime`
        for an `Instant`, a `ZonedDateTime`, or a `numpy.datetime64` of a time unit; None for
        a value of any other type.

    Raises:
        ValueError: If a `numpy.datetime64` is `NaT` or of a week, month, or year, or a Java
            or numpy value lies outside years 1 to 9999.
    """
    java_class = getattr(type(value), "jclassname", None)
    java = cast("Any", value)  # a Java object through jpy, which has no type information
    if java_class == "java.time.LocalDate":
        try:
            return date(java.getYear(), java.getMonthValue(), java.getDayOfMonth())
        except ValueError:  # a year a `date` cannot hold, as LocalDate.MAX
            raise ValueError(f"{name}={value} {_OUTSIDE_DATETIME}") from None
    if java_class in _JAVA_INSTANTS:
        zoned = java_class == "java.time.ZonedDateTime"
        instant = java.toInstant() if zoned else java
        seconds, nanos = int(instant.getEpochSecond()), int(instant.getNano())
        moment = _rounded((seconds * _NANOS + nanos) * _NANOS, name, value, upper=upper)
        if not zoned:
            return moment
        offset = timedelta(seconds=int(java.getOffset().getTotalSeconds()))
        return moment.astimezone(timezone(offset))
    if isinstance(value, np.datetime64):
        return _numpy_bound(value, name, upper=upper)
    return None


def _numpy_bound(value: np.datetime64, name: str, *, upper: bool) -> date | datetime:
    """Parse a `numpy.datetime64` bound: a day is a date, a finer time an instant in UTC.

    Args:
        value: The bound.
        name: The parameter's name, for messages.
        upper: True for an upper bound, rounded up; False for any other, rounded down.

    Returns:
        A `date` for day precision; otherwise the aware UTC `datetime` (`_rounded`).

    Raises:
        ValueError: If `value` is `NaT`, of a week, month, or year, or a day outside years 1
            to 9999.

    A time outside years 1 to 9999 raises the `ValueError` of `_rounded`.
    """
    if np.isnat(value):
        raise ValueError(f"{name}={value!r} is not a time. Pass a date or an aware datetime.")
    unit, count = np.datetime_data(value.dtype)
    if unit in _NUMPY_PERIODS:
        raise ValueError(
            f"{name}={value!r} is a week, month, or year, not a day or a time. Pass a day, "
            "such as numpy.datetime64('2025-03-03')."
        )
    if unit == _NUMPY_DAY:
        try:
            return date.fromisoformat(str(value))
        except ValueError:  # a year a `date` cannot hold
            raise ValueError(f"{name}={value!r} {_OUTSIDE_DATETIME}") from None
    logger.debug("%s=%r has no timezone; read as UTC", name, value)
    ticks = int(value.astype(np.int64)) * count * _ATTOSECONDS[unit]
    return _rounded(ticks, name, value, upper=upper)


def _rounded(attoseconds: int, name: str, value: object, *, upper: bool) -> datetime:
    """Return a UTC instant given in attoseconds since the epoch, moved outward to a microsecond.

    A `datetime` holds microseconds, so an instant between two is moved to one of them: down
    for a lower bound, so the row it came from is still included, and up for an upper bound,
    so a half-open range never loses a row. Each move logs at DEBUG with the result and the
    amount moved. An upper bound past the last microsecond of year 9999 becomes
    `datetime.max`, which no row reaches.

    Args:
        attoseconds: The instant, in attoseconds (10**-18 s) since the Unix epoch, so every
            unit a Java or numpy time counts in divides it exactly.
        name: The parameter's name, for messages.
        value: The bound as the caller passed it, for messages.
        upper: True to round up; False to round down.

    Returns:
        The aware UTC `datetime`.

    Raises:
        ValueError: If the instant lies outside years 1 to 9999.
    """
    micros, rest = divmod(attoseconds, _ATTOSECONDS["us"])
    try:
        moment = EPOCH + timedelta(microseconds=micros)
    except OverflowError:
        raise ValueError(f"{name}={value} {_OUTSIDE_DATETIME}") from None
    return _outward(moment, rest, name=name, value=value, upper=upper)


def _outward(moment: datetime, rest: int, *, name: str, value: object, upper: bool) -> datetime:
    """Move a microsecond instant up for an upper bound when time below the microsecond was cut.

    Args:
        moment: The instant with everything below the microsecond cut (rounded down).
        rest: The attoseconds that were cut; 0 when nothing was.
        name: The parameter's name, for the log line.
        value: The bound as the caller passed it, for the log line.
        upper: True for an upper bound, which moves up a microsecond; False to stay down.

    Returns:
        `moment`, a microsecond later for an upper bound with a `rest`; `datetime.max` for an
        upper bound past the last microsecond of year 9999, which no row reaches.
    """
    if not rest:
        return moment
    direction, moved = "down", rest
    if upper:
        try:
            moment += _ONE_MICROSECOND
            direction, moved = "up", _ATTOSECONDS["us"] - rest
        except OverflowError:  # the end of year 9999: stays at the largest datetime
            direction = "down (clamped)"
    logger.debug(
        "%s=%s rounded %s to %s, by %s ns",
        name,
        value,
        direction,
        moment.isoformat(),
        moved / _NANOS,
    )
    return moment


def _cut_digits(text: str) -> int:
    """Return the attoseconds `datetime.fromisoformat` cut from ISO-8601 text.

    It keeps six fractional digits of a second and drops the rest, which this reads back.

    Args:
        text: ISO-8601 text that parsed.

    Returns:
        The cut part of the second in attoseconds, at least 1 when any cut digit is nonzero
        (digits past the eighteenth count only as nonzero); 0 when nothing was cut.
    """
    found = _FRACTION.search(text)
    digits = found.group(1)[6:] if found else ""
    attoseconds = int(digits[:12].ljust(12, "0") or "0")
    return attoseconds or int(any(digit != "0" for digit in digits))


def require_aware(name: str, raw: object, moment: datetime) -> None:
    """Reject a time without a timezone.

    Massive's day boundaries depend on the asset class's zone, so a naive time cannot be
    placed on the time axis. Shared by the time bounds and the `filters` and parameter checks,
    so every naive time gets the same message.

    Args:
        name: The parameter's name, such as `"start"` or a filter name.
        raw: The value as the caller passed it, for the message.
        moment: The value as a `datetime`.

    Raises:
        ValueError: If `moment` has no timezone, with a message naming the parameter, the
            value, and the fix, such as `start=datetime.datetime(2025, 1, 2, 9, 30) has no
            timezone. Pass an aware datetime or a date.`
    """
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError(f"{name}={raw!r} has no timezone. Pass an aware datetime or a date.")


def check_order(start: date | datetime, end: date | datetime) -> None:
    """Reject a range whose bounds are of two kinds, or whose end comes before its start.

    Shared by `history()` and the time axis of a `…T` dataset. Two dates are whole days, so
    `end` may equal `start` (that one day); two datetimes are a half-open range, so equal
    instants are an empty range, which is allowed. Datetimes are compared in UTC, so the
    repeated hour of a daylight-saving fall-back is ordered correctly.

    A reversed range is refused for two reasons. Massive answers it with no error where it is
    sent as filters: the dividends, sent `ex_dividend_date.gte` a month ago and `.lte` a year
    ago, answered HTTP 200 with no rows. And `history()` cannot send it at all: it turns the
    range into the bars or days that fit inside it (`bar_periods.bar_starts`, the flat files'
    days), a reversed range fits none, and so nothing would be requested, although the
    aggregates range answers a reversed `{from}` and `{to}` with HTTP 400.

    Args:
        start: The parsed lower bound.
        end: The parsed upper bound.

    Raises:
        ValueError: If one bound is a date and the other a datetime, ending with
            `MIXED_BOUNDS`, or if the range is reversed.
    """
    if isinstance(start, datetime) is not isinstance(end, datetime):
        raise ValueError(
            f"start={start!r} and end={end!r} mix a date and a datetime. {MIXED_BOUNDS}"
        )
    if isinstance(start, datetime) and isinstance(end, datetime):
        # In UTC: two datetimes sharing one tzinfo compare by wall clock and ignore `fold`.
        reversed_range = start.astimezone(UTC) > end.astimezone(UTC)
    else:
        reversed_range = start > end
    if reversed_range:
        raise ValueError(
            f"start={start!r} is after end={end!r}. Pass a start no later than the end."
        )


def midnight(day: date, zone: tzinfo) -> datetime:
    """Return the instant a calendar day starts in a zone, in UTC.

    Args:
        day: The calendar day.
        zone: The zone whose midnight is meant.

    Returns:
        An aware UTC `datetime`.
    """
    return datetime.combine(day, time(0), tzinfo=zone).astimezone(UTC)


@dataclass(frozen=True, slots=True)
class TimeRange:
    """A half-open interval of instants, `[start, end)`, in UTC; empty when `end <= start`.

    Frozen and hashable, so it is safe to share between threads.

    Attributes:
        start: The first instant included.
        end: The first instant excluded.
    """

    start: datetime
    end: datetime

    @property
    def start_ns(self) -> int:
        """`start` as nanoseconds since the Unix epoch."""
        return epoch_ns(self.start)

    @property
    def end_ns(self) -> int:
        """`end` as nanoseconds since the Unix epoch."""
        return epoch_ns(self.end)

    @property
    def empty(self) -> bool:
        """True when no instant lies in the range."""
        return self.end <= self.start

    def partition_dates(self, zone: tzinfo) -> tuple[date, ...]:
        """Return the calendar dates the range touches in a zone, oldest first.

        These are the flat-file partitions to read: every date from the date of `start` to the
        date of `end` less one microsecond, inclusive; none for an empty range.

        Args:
            zone: The asset class's day zone.

        Returns:
            The dates, oldest first.
        """
        if self.empty:
            return ()
        first = self.start.astimezone(zone).date()
        last = (self.end - _ONE_MICROSECOND).astimezone(zone).date()
        return DaySpan(first, last).days()


@dataclass(frozen=True, slots=True)
class DaySpan:
    """Whole days, or whole futures sessions, from `first` to `last`, both inclusive.

    Empty when `last` is before `first`. Made by `bounds` and the bar rules; frozen and hashable,
    so it is safe to share between threads.

    Attributes:
        first: The first day (for futures, the first `session_end_date`) included.
        last: The last day included.
    """

    first: date
    last: date

    @property
    def empty(self) -> bool:
        """True when the span holds no day."""
        return self.last < self.first

    def days(self) -> tuple[date, ...]:
        """Return every day of the span, oldest first.

        Returns:
            The days; none for an empty span.
        """
        count = (self.last - self.first).days + 1
        return tuple(self.first + timedelta(days=n) for n in range(count))

    def instants(self, zone: tzinfo) -> TimeRange:
        """Return the span's calendar days as instants: midnight of `first` to that after `last`.

        Args:
            zone: The day zone whose midnights bound the days.

        Returns:
            The half-open range, in UTC.
        """
        return TimeRange(midnight(self.first, zone), midnight(self.last + _ONE_DAY, zone))


def bounds(
    spec: DatasetSpec,
    start: DateLike,
    end: DateLike | None,
    *,
    now: datetime,
    classes: Iterable[AssetClass] = (),
) -> TimeRange | DaySpan:
    """Normalize the bounds of a dataset's `history()` call by their kind and day rules.

    Two dates give the `DaySpan` of the whole days (for futures, sessions) from `start` to
    `end`; two datetimes give the half-open `TimeRange` between the instants. An `end` of None
    reads through now: the current instant after a datetime `start`; after a date `start` the
    current day in the day zone of the class `day_class` picks (New York without one), or for
    futures the session the current instant belongs to.

    Args:
        spec: The dataset.
        start: The first day or instant, as passed or already parsed by `parse_date_like`
            (parsed bounds pass through unchanged).
        end: The last day, the first instant excluded, or None for now.
        now: The current instant, aware.
        classes: The classes of the request's symbols, for `day_class`; empty by default.

    Returns:
        The days or the instants.

    Raises:
        ValueError: If a bound cannot be parsed or is naive (`parse_date_like`), the bounds
            mix a date and a datetime, or `start` is after `end` (or after now, for an `end`
            of None).
    """
    first = parse_date_like(start, "start")
    asset_class = day_class(spec, classes)
    last: date | datetime
    if end is not None:
        last = parse_date_like(end, "end", upper=True)
    elif isinstance(first, datetime):
        last = now
    elif asset_class is AssetClass.FUTURES:
        last = futures_session(now)
    else:
        last = now.astimezone(day_zone(asset_class)).date()
    try:
        check_order(first, last)
    except ValueError:
        if end is None:  # `last` is now, in `first`'s kind, so only the order can fail
            # Refused because Massive answers it with no error: AAPL's day bars from 10 to 20
            # days ahead answered HTTP 200 with no rows, which would read as no trading.
            raise ValueError(
                f"start={first!r} is after now, {last}, and end=None reads up to now. Pass a "
                "start no later than now."
            ) from None
        raise
    if isinstance(first, datetime) and isinstance(last, datetime):
        return TimeRange(first.astimezone(UTC), last.astimezone(UTC))
    return DaySpan(first, last)


def reaches_now(
    end: date | datetime | None, asset_class: AssetClass | ClassFromPrefix, *, now: datetime
) -> bool:
    """Tell whether a request's window reaches the present, so its latest rows are current.

    Args:
        end: The request's upper bound: an aware instant (excluded), a last day (included),
            or None for an open window, which reads through now.
        asset_class: The class whose day zone dates `now` for a day bound (`day_zone`: New
            York without one, which is never later than a UTC day).
        now: The current instant, aware.

    Returns:
        True for an open window, an instant after `now`, or a day on or after the current
        day in the class's day zone; False for a window that ends in the past.
    """
    if end is None:
        return True
    if isinstance(end, datetime):
        return end > now
    return end >= now.astimezone(day_zone(asset_class)).date()


def is_trading_day(asset_class: AssetClass | ClassFromPrefix, day: date) -> bool:
    """Tell whether a day can hold a class's trading, without a holiday calendar.

    Every weekday can. Crypto trades every day, and forex from Sunday evening to Friday
    evening UTC, so its Sunday holds trading but its Saturday none. Stocks, options, indices,
    and futures sessions trade on weekdays only. The one test the transports use to skip a
    weekend request or a weekend's missing file.

    Args:
        asset_class: The class, or `ClassFromPrefix.ANY` for data of no one class.
        day: The day, in the class's day zone; for futures, a session date.

    Returns:
        True for a weekday, a forex Sunday, or any crypto day.
    """
    if asset_class is AssetClass.CRYPTO:
        return True
    if asset_class is AssetClass.FOREX:
        return day.weekday() != calendar.SATURDAY
    return day.weekday() < calendar.SATURDAY


def trades_at_weekends(asset_class: AssetClass | ClassFromPrefix) -> bool:
    """Tell whether an asset class trades at weekends, so its Saturdays and Sundays hold data.

    Forex trades from Sunday evening UTC and crypto every day; stocks, options, indices,
    futures sessions, and data of no class do not.

    Args:
        asset_class: The class, or `ClassFromPrefix.ANY` for data of no one class.

    Returns:
        True for forex and crypto.
    """
    return asset_class in _WEEKEND_TRADING


def day_end(asset_class: AssetClass | ClassFromPrefix, day: date) -> datetime:
    """Return the instant by which a class's trading day is over: a day bar's `TimestampEnd`.

    Every row that covers whole days ends here, and a `history()` range of dates ends at its
    last day's end, so a bar is kept by the same instant a join on its end would use. The
    next midnight is the calendar's next midnight in the class's day zone, never 24 hours
    later, so a day across a daylight-saving change keeps its length.

    Args:
        asset_class: The class (`day_class`); `ClassFromPrefix.ANY` reads New York days.
        day: The day; for futures, the session's `session_end_date`.

    Returns:
        The instant, in UTC as `day_start` and `midnight` return theirs: 17:00 Chicago time
        on a futures session's date (`futures_session_end`); 01:00 New York time after an
        index day; otherwise midnight after the day in the class's day zone.
    """
    if asset_class is AssetClass.FUTURES:
        return futures_session_end(day).astimezone(UTC)
    after = day_start(asset_class, day + _ONE_DAY)
    return after + _INDEX_DAY_END if asset_class is AssetClass.INDICES else after


def day_zone(asset_class: AssetClass | ClassFromPrefix) -> tzinfo:
    """Return the zone whose calendar days an asset class's dates and instants are read in.

    New York for stocks, options, indices, and data of no class; UTC for forex and crypto;
    Chicago for futures, whose sessions roll there.

    Args:
        asset_class: The class, or `ClassFromPrefix.ANY` for data of no one class.

    Returns:
        The zone.
    """
    if asset_class is AssetClass.FUTURES:
        return CHICAGO
    return NEW_YORK if asset_class is ClassFromPrefix.ANY else DAY_ZONES[asset_class]


def day_class(
    spec: DatasetSpec, classes: Iterable[AssetClass] = ()
) -> AssetClass | ClassFromPrefix:
    """Return the asset class whose day rules a request of a dataset follows.

    A dataset with an asset class follows its class. A dataset without one reads its days in
    New York, with one exception: a dataset that declares a path per symbol class
    (`DatasetSpec.fetch_paths_by_class`, the indicators, whose crypto bars have UTC days)
    serves class-specific data, so a request follows the class its symbols share. A filter on
    a publication time does not depend on which tickers are listed, so every other dataset
    without a class keeps one window per call.

    Args:
        spec: The dataset.
        classes: The classes of the request's symbols (`symbols.symbol_class` of each);
            empty when the request has none.

    Returns:
        The class, or `ClassFromPrefix.ANY` for New York days.
    """
    if spec.asset_class is not ClassFromPrefix.ANY:
        return spec.asset_class
    shared = set(classes)
    if not spec.fetch_paths_by_class or len(shared) != 1:
        return ClassFromPrefix.ANY
    return shared.pop()


def calendar_day(spec: DatasetSpec, value: object, name: str, *, given: object = None) -> date:
    """Turn the single day a dataset's path or parameter takes into a calendar date.

    Endpoints such as the daily open-close summary take one calendar day (the day of the
    dataset's day zone: New York for stocks, options, and indices, UTC for forex and crypto). A
    date or a `"YYYY-MM-DD"` string is that day as given. A calendar date is a whole day, so a
    `datetime` is refused, as on a calendar-date time axis: which day an instant names would
    depend on a zone the caller did not state.

    Args:
        spec: The dataset, for the message.
        value: The day, as ISO-8601 date text, a `date`, a Java `LocalDate`, or a
            day-precision `numpy.datetime64`.
        name: The parameter's name, used in error messages (`"date"`).
        given: The argument as the caller passed it, for the message, when `value` is its
            conversion (Deephaven text read as a Java value); None when `value` is it.

    Returns:
        The calendar date to put in the request.

    Raises:
        ValueError: If `value` is a time rather than a day: a `datetime`, ISO-8601 text with a
            time, or an instant; the message shows the value as the caller passed it.

    Text that is not ISO-8601, or a value of another type, raises the `ValueError` or
    `TypeError` of `parse_date_like`.
    """
    parsed = parse_date_like(value, name)
    if not isinstance(parsed, datetime):
        return parsed
    shown = value if given is None else given
    raise ValueError(
        f"{spec.name}: {name}={shown!r} is a time, and {name} takes a calendar date. Pass a "
        "date such as '2025-03-03'."
    )


_UNIT_STEPS: Final[Mapping[WireUnit, timedelta]] = {
    WireUnit.S: timedelta(seconds=1),
    WireUnit.MS: timedelta(milliseconds=1),
    WireUnit.US: timedelta(microseconds=1),
}
"""One step of each timestamp unit a `datetime` can express exactly; nanoseconds cannot be."""


def day_start(asset_class: AssetClass | ClassFromPrefix, day: date) -> datetime:
    """Return the instant a date starts under an asset class's day rules.

    A calendar day starts at midnight in the class's day zone (`day_zone`; New York without
    a class). A futures date names a trading session, which opens at 17:00 Chicago time on the
    day before its `session_end_date` (`futures_session_open`).

    Args:
        asset_class: The class whose rules apply (`day_class`), or `ClassFromPrefix.ANY`.
        day: The date.

    Returns:
        The instant, in UTC as `midnight` and `day_end` return theirs, whatever the zone
        whose day it starts.
    """
    if asset_class is AssetClass.FUTURES:
        return futures_session_open(day).astimezone(UTC)
    return midnight(day, day_zone(asset_class))


def instant_bound(
    asset_class: AssetClass | ClassFromPrefix,
    name: str,
    value: date,
    *,
    unit: WireUnit | None,
    upper: bool,
) -> datetime:
    """Return the instant a REST parameter that compares instants is sent for a bound.

    Such a parameter is a timestamp parameter, or a date parameter whose row field is an
    instant (`published_utc`). A `date` bound is a whole day, inclusive at either end; an
    aware `datetime` is the first instant included at the lower end and the first excluded at
    the upper end. A date becomes the instant its day starts (`day_start`), the next day's
    start for an upper bound. An exclusive lower `.gt` or an inclusive upper `.lte` then moves
    the instant back one unit of the parameter, so the range is exactly the same.

    Args:
        asset_class: The class whose day rules apply (`day_class`), or `ClassFromPrefix.ANY` for New
            York days.
        name: The parameter's wire name, such as `"timestamp.gte"`.
        value: The bound.
        unit: The unit a timestamp parameter counts in; None for a date parameter, which
            takes the instant as ISO-8601 text.
        upper: True for the upper bound.

    Returns:
        The instant to send.

    Raises:
        ValueError: If a `.gt` or `.lte` bound has no step a `datetime` can take exactly: a
            parameter in nanoseconds, or a date parameter that takes the instant as ISO-8601
            text (`unit` None), whose precision Massive does not document. Use the `.gte` and
            `.lt` forms instead.
    """
    if isinstance(value, datetime):
        instant = value
    else:
        instant = day_start(asset_class, value + _ONE_DAY if upper else value)
    if name.rpartition(".")[2] not in ("gt", "lte"):
        return instant
    # Nanoseconds have no `datetime` step; every other unit has its step in `_UNIT_STEPS`.
    step = None if unit is None or unit is WireUnit.NS else _UNIT_STEPS[unit]
    if step is None:
        sent = "ISO-8601 instants of no documented precision" if unit is None else "nanoseconds"
        raise ValueError(
            f"{name} takes {sent}, so no day can end on it exactly. Use the .gte and .lt forms."
        )
    return instant - step
