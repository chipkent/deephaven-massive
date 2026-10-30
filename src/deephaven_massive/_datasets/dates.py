"""Date parsing and the day rules of `history()` and the time-axis filters.

Every time bound a user passes is a `DateLike`: an ISO-8601 string, a `date`, a
timezone-aware `datetime`, one of the Java time values Deephaven hands its users (`Instant`,
`ZonedDateTime`, `LocalDate`), or a day-precision `numpy.datetime64`. `parse_date_like` turns
each into a `date` or an aware `datetime`, the only two forms the rest of the library sees. A
bound with a time of day but no timezone is ambiguous across the three day zones Massive uses,
so it is rejected with a `ValueError` that names the parameter, the value, and the fix
(`require_aware`).

A Java value is recognized by its class name, so the dataset layer imports neither Deephaven
nor jpy, and reading its fields calls only that immutable `java.time` object, which takes no
Deephaven lock.

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
rules to a dataset's `history()` arguments.

`day_start` and `instant_bound` turn a date sent to a REST parameter that compares instants
into its instant, and `day_end` gives the instant a day is over by.

A futures date names a trading session, whose 17:00 Chicago rule is `futures_sessions`; the
bars of a timespan that fit a range are `bar_periods`; and whether a day's flat file can exist
yet is `history_source.flat_file_published`.

Every function is pure (the current time is a parameter) and thread-safe.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta, timezone, tzinfo
from typing import TYPE_CHECKING, Any, Final, cast
from zoneinfo import ZoneInfo

import numpy as np

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
    from deephaven_massive._types import DateLike

__all__ = [
    "DAY_ZONES",
    "DAY_ZONE_NAMES",
    "MIXED_BOUNDS",
    "NEW_YORK",
    "DaySpan",
    "TimeRange",
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
    "require_aware",
    "trades_at_weekends",
]

logger = logging.getLogger(__name__)

NEW_YORK: Final = ZoneInfo("America/New_York")
"""The day zone of stocks, options, and indices, and the zone of the flat-file cutoff."""

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

MIXED_BOUNDS: Final = (
    "start and end must both be dates or both be datetimes: a date is a whole day (for "
    "futures, a trading session) and a datetime an exact instant"
)
"""The message of a range that mixes a date and a datetime."""

_DATE_ONLY: Final = re.compile(r"^\d{4}-\d{2}-\d{2}$")
# A fraction of a second finer than the microsecond, which `datetime.fromisoformat` truncates,
# in the extended (`09:30:00.1234567`) and the basic (`093000.1234567`) time form alike. Only a
# fraction of a second can hold a `.` or `,` in ISO-8601 text, so the separator marks it.
_BELOW_MICROSECOND: Final = re.compile(r"[.,]\d{7,}")
_BELOW_MICROSECOND_MESSAGE: Final = (
    "has more than six fractional digits of a second, which a datetime cannot hold; cutting "
    "them would move the time, so give at most microseconds"
)
_OUTSIDE_DATETIME: Final = "is not a time from year 1 to 9999, which a datetime holds"
_JAVA_INSTANTS: Final = frozenset({"java.time.Instant", "java.time.ZonedDateTime"})
"""The Java classes whose values are instants, by their jpy class name."""
_NUMPY_DAY: Final = ("D", 1)
"""The unit of a day-precision `numpy.datetime64` (`numpy.datetime_data`), the only precision
a bound takes."""
_ONE_DAY: Final = timedelta(days=1)
_INDEX_DAY_END: Final = timedelta(hours=1)
"""How much later an index day ends than the New York day: 01:00 New York time, midnight in
Chicago, since whether an index day bar covers the New York or the Chicago day is not
verified."""
_ONE_MICROSECOND: Final = timedelta(microseconds=1)
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_SATURDAY: Final = 5
_WEEKEND_TRADING: Final = frozenset({AssetClass.FOREX, AssetClass.CRYPTO})
"""The classes that trade at weekends, whose bar periods keep their Saturdays and Sundays."""


def parse_date_like(value: DateLike, name: str) -> date | datetime:
    """Parse one time bound into a `date` or an aware `datetime`.

    Args:
        value: An ISO-8601 string (`"2025-03-03"` or `"2025-03-03T09:30:00-05:00"`), a `date`,
            a timezone-aware `datetime`, a Java `LocalDate` (a date), a Java `Instant` or
            `ZonedDateTime` (an aware `datetime` at the same instant, cut to the
            microsecond), or a `numpy.datetime64` of day precision (a date).
        name: The parameter's name, used in error messages (`"start"`, `"end"`, `"date"`).

    Returns:
        A `date` for a date-only value; otherwise an aware `datetime`.

    Raises:
        ValueError: For a naive `datetime` or a string with a time but no offset (the message
            names the parameter, the value, and the fix; `require_aware`), a string that is
            not ISO-8601, or a string with more than six fractional digits of a second, which
            a `datetime` would silently cut to microseconds.

    A Java or numpy value raises the `ValueError` of `_foreign_bound` when it cannot be a
    bound, and a value of none of the accepted types its `TypeError`.
    """
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        return value
    elif isinstance(value, str):
        text = value.strip()
        if _BELOW_MICROSECOND.search(text):
            raise ValueError(f"{name}={value!r} {_BELOW_MICROSECOND_MESSAGE}")
        try:
            if _DATE_ONLY.match(text):
                return date.fromisoformat(text)
            parsed = datetime.fromisoformat(text)
        except ValueError:
            raise ValueError(f"{name}={value!r} is not an ISO-8601 date or datetime") from None
    else:
        return _foreign_bound(value, name)
    require_aware(name, value, parsed)
    return parsed


def _foreign_bound(value: object, name: str) -> date | datetime:
    """Parse a time bound that is a Java time value or a `numpy.datetime64`.

    A Java value is recognized by its class name, which jpy sets as `jclassname` on every Java
    type it wraps, so jpy is not imported. An `Instant` becomes a UTC `datetime` and a
    `ZonedDateTime` one at its own offset; only the instant matters to a bound, so the zone's
    later rules are not needed. A Java instant's nanoseconds below the microsecond are cut,
    moving the bound less than a microsecond earlier: unlike typed text, Deephaven's instants
    carry nanoseconds the user never chose (`dh_now()` on Linux, a SIP `Timestamp`), and an
    earlier start still includes the row a `Timestamp` cell came from.

    Args:
        value: The bound, which is not a `str`, `date`, or `datetime`.
        name: The parameter's name, for messages.

    Returns:
        A `date` for a `LocalDate` or a day-precision `numpy.datetime64`; an aware `datetime`
        for an `Instant` or a `ZonedDateTime`.

    Raises:
        TypeError: If `value` is none of the accepted types.
        ValueError: If a `numpy.datetime64` is `NaT` or has a precision other than a day,
            since it has no timezone; or if a Java or numpy value lies outside years 1 to
            9999.
    """
    java_class = getattr(type(value), "jclassname", None)
    java = cast("Any", value)  # a Java object through jpy, which has no type information
    try:
        if java_class == "java.time.LocalDate":
            return date(java.getYear(), java.getMonthValue(), java.getDayOfMonth())
        if java_class in _JAVA_INSTANTS:
            zoned = java_class == "java.time.ZonedDateTime"
            instant = java.toInstant() if zoned else java
            # Deephaven's own instants carry nanoseconds (`dh_now()` on Linux, a SIP
            # timestamp), which the user cannot choose, so they are cut to the microsecond.
            nanos = int(instant.getNano())
            if nanos % 1_000:
                logger.debug(
                    "%s=%s cut to the microsecond: %d ns dropped", name, value, nanos % 1_000
                )
            seconds = int(instant.getEpochSecond())
            moment = _EPOCH + timedelta(seconds=seconds, microseconds=nanos // 1_000)
            if not zoned:
                return moment
            offset = timedelta(seconds=int(java.getOffset().getTotalSeconds()))
            return moment.astimezone(timezone(offset))
    except (OverflowError, ValueError):  # a time a `datetime` cannot hold, as Instant.MAX
        raise ValueError(f"{name}={value} {_OUTSIDE_DATETIME}") from None
    if isinstance(value, np.datetime64):
        if np.isnat(value):
            raise ValueError(f"{name}={value!r} is NaT, not a date")
        if np.datetime_data(value.dtype) != _NUMPY_DAY:
            raise ValueError(
                f"{name}={value!r} is a numpy.datetime64 of {value.dtype}, which has no "
                "timezone; pass one of day precision, an aware datetime, or a Java Instant "
                "(deephaven.time.to_j_instant converts a UTC numpy time)"
            )
        try:
            return date.fromisoformat(str(value))
        except ValueError:  # a year a `date` cannot hold
            raise ValueError(f"{name}={value!r} {_OUTSIDE_DATETIME}") from None
    raise TypeError(
        f"{name} must be a str, date, datetime, Java Instant, ZonedDateTime, or LocalDate, or "
        f"a numpy.datetime64 of day precision, not {type(value).__name__}"
    )


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
            value, and the fix, such as `start=datetime.datetime(2025, 1, 2, 9, 30) needs a
            timezone: pass an aware datetime or a date`.
    """
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError(f"{name}={raw!r} needs a timezone: pass an aware datetime or a date")


def check_order(start: date | datetime, end: date | datetime) -> None:
    """Reject a range whose bounds are of two kinds, or whose end comes before its start.

    Shared by `history()` and the time axis of a `…T` dataset. Two dates are whole days, so
    `end` may equal `start` (that one day); two datetimes are a half-open range, so equal
    instants are an empty range, which is allowed. Datetimes are compared in UTC, so the
    repeated hour of a daylight-saving fall-back is ordered correctly.

    Args:
        start: The parsed lower bound.
        end: The parsed upper bound.

    Raises:
        ValueError: `MIXED_BOUNDS` if one bound is a date and the other a datetime, or
            `start (...) must not be after end (...)` if the range is reversed.
    """
    if isinstance(start, datetime) is not isinstance(end, datetime):
        raise ValueError(f"{MIXED_BOUNDS}; got start={start!r}, end={end!r}")
    if isinstance(start, datetime) and isinstance(end, datetime):
        # In UTC: two datetimes sharing one tzinfo compare by wall clock and ignore `fold`.
        reversed_range = start.astimezone(UTC) > end.astimezone(UTC)
    else:
        reversed_range = start > end
    if reversed_range:
        raise ValueError(f"start ({start!r}) must not be after end ({end!r})")


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
        return _epoch_ns(self.start)

    @property
    def end_ns(self) -> int:
        """`end` as nanoseconds since the Unix epoch."""
        return _epoch_ns(self.end)

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


def _epoch_ns(moment: datetime) -> int:
    """Convert an aware `datetime` to nanoseconds since the Unix epoch, exactly.

    Args:
        moment: An aware `datetime`.

    Returns:
        The epoch nanoseconds.
    """
    delta = moment - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * 10**9 + delta.microseconds * 1_000


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
        last = parse_date_like(end, "end")
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
            raise ValueError(
                f"start ({first!r}) must not be after now ({last!r}); end=None reads through now"
            ) from None
        raise
    if isinstance(first, datetime) and isinstance(last, datetime):
        return TimeRange(first.astimezone(UTC), last.astimezone(UTC))
    return DaySpan(first, last)


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
        return day.weekday() != _SATURDAY
    return day.weekday() < _SATURDAY


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
        The aware instant: 17:00 Chicago time on a futures session's date
        (`futures_session_end`); 01:00 New York time after an index day; otherwise midnight
        after the day in the class's day zone.
    """
    if asset_class is AssetClass.FUTURES:
        return futures_session_end(day)
    zone = day_zone(asset_class)
    after = day + _ONE_DAY
    midnight = datetime(after.year, after.month, after.day, tzinfo=zone)
    return midnight + _INDEX_DAY_END if asset_class is AssetClass.INDICES else midnight


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


def calendar_day(spec: DatasetSpec, value: DateLike, name: str) -> date:
    """Turn the single day a dataset's path or parameter takes into a calendar date.

    Endpoints such as the daily open-close summary take one calendar day (the day of the
    dataset's day zone: New York for stocks, options, and indices, UTC for forex and crypto). A
    date or a `"YYYY-MM-DD"` string is that day as given. A calendar date is a whole day, so a
    `datetime` is refused, as on a calendar-date time axis: which day an instant names would
    depend on a zone the caller did not state.

    Args:
        spec: The dataset, for the message.
        value: The day, as an ISO-8601 date string, a `date`, a Java `LocalDate`, or a
            day-precision `numpy.datetime64`.
        name: The parameter's name, used in error messages (`"date"`).

    Returns:
        The calendar date to put in the request.

    Raises:
        ValueError: If `value` is a `datetime`, or ISO-8601 text with a time
            (`"<dataset>: <name> is a calendar date; pass a date"`).

    Text that is not ISO-8601, or a value of another type, raises the `ValueError` or
    `TypeError` of `parse_date_like`.
    """
    parsed = parse_date_like(value, name)
    if not isinstance(parsed, datetime):
        return parsed
    raise ValueError(f"{spec.name}: {name} is a calendar date; pass a date")


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
        The aware instant, in the day zone.
    """
    if asset_class is AssetClass.FUTURES:
        return futures_session_open(day)
    return datetime(day.year, day.month, day.day, tzinfo=day_zone(asset_class))


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
    step = None if unit is None else _UNIT_STEPS.get(unit)
    if step is None:
        sent = "ISO-8601 instants of no documented precision" if unit is None else "nanoseconds"
        raise ValueError(
            f"{name} takes {sent}, so it cannot bound a half-open range exactly; use the .gte "
            "and .lt forms"
        )
    return instant - step
