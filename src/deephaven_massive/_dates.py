"""Date parsing and the day rules of `history()` and the time-axis filters (DESIGN § 12).

Every time bound a user passes is a `DateLike`: an ISO-8601 string, a `date`, or a
timezone-aware `datetime`. A bound with a time of day but no timezone is ambiguous across the
three day zones Massive uses, so it is rejected with a `ValueError` that names the
parameter, the value, and the fix (`require_aware`).

A date-only bound means a whole day in the asset class's *day zone*:

- **America/New_York** for stocks, options, and indices;
- **UTC** for forex and crypto;
- **trading sessions** for futures: a date names the session whose `session_end_date` equals
  it. Futures bars are requested over a `window_start` range widened by one day on each side
  in America/Chicago and then filtered on `SessionEndDate`.

Ranges are half-open, `[start, end)`. A date `start` is that day's midnight; a date `end` is the
*next* day's midnight, so `history("2025-03-03", "2025-03-03")` is the whole of March 3.

`bounds` applies these rules to a dataset's `history()` arguments. `flat_file_published` decides
whether a day's flat file can exist yet: files land "by approximately 11:00 AM ET the following
day", so a day's file counts as published from the first configured cutoff (New York time) after
the next day starts in the class's day zone.

Every function is pure (the current time is a parameter) and thread-safe.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from typing import TYPE_CHECKING, Final
from zoneinfo import ZoneInfo

from deephaven_massive._enums import AssetClass

if TYPE_CHECKING:
    from collections.abc import Mapping

    from deephaven_massive._catalog import DatasetSpec
    from deephaven_massive._types import DateLike

__all__ = [
    "CHICAGO",
    "DAY_ZONES",
    "NEW_YORK",
    "FuturesSessions",
    "TimeRange",
    "bounds",
    "flat_file_published",
    "futures_sessions",
    "history_range",
    "parse_date_like",
    "publish_time",
    "require_aware",
]

logger = logging.getLogger(__name__)

NEW_YORK: Final = ZoneInfo("America/New_York")
"""The day zone of stocks, options, and indices, and the zone of the flat-file cutoff."""

CHICAGO: Final = ZoneInfo("America/Chicago")
"""The zone in which futures bar requests are widened around session dates."""

DAY_ZONES: Final[Mapping[AssetClass, tzinfo]] = {
    AssetClass.STOCKS: NEW_YORK,
    AssetClass.OPTIONS: NEW_YORK,
    AssetClass.INDICES: NEW_YORK,
    AssetClass.FOREX: UTC,
    AssetClass.CRYPTO: UTC,
}
"""The zone whose midnights bound a date-only `start` or `end`, per asset class.

Futures are absent: a futures date names a trading session, not a calendar day (see `bounds`
and `futures_sessions`). Datasets without an asset class use New York."""

_DATE_ONLY: Final = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ONE_DAY: Final = timedelta(days=1)


def parse_date_like(value: DateLike, name: str) -> date | datetime:
    """Parse one time bound into a `date` or an aware `datetime`.

    Args:
        value: An ISO-8601 string (`"2025-03-03"` or `"2025-03-03T09:30:00-05:00"`), a `date`,
            or a timezone-aware `datetime`.
        name: The parameter's name, used in error messages (`"start"`, `"end"`, `"date"`).

    Returns:
        A `date` for a date-only value; otherwise an aware `datetime`.

    Raises:
        TypeError: If `value` is not a `str`, `date`, or `datetime`.
        ValueError: For a naive `datetime` or a string with a time but no offset (the message
            names the parameter, the value, and the fix; `require_aware`), or a string that
            is not ISO-8601.
    """
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        return value
    elif isinstance(value, str):
        text = value.strip()
        try:
            if _DATE_ONLY.match(text):
                return date.fromisoformat(text)
            parsed = datetime.fromisoformat(text)
        except ValueError:
            raise ValueError(f"{name}={value!r} is not an ISO-8601 date or datetime") from None
    else:
        raise TypeError(f"{name} must be a str, date, or datetime, not {type(value).__name__}")
    require_aware(name, value, parsed)
    return parsed


def require_aware(name: str, raw: object, moment: datetime) -> None:
    """Reject a time without a timezone (DESIGN § 11).

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


def _midnight(day: date, zone: tzinfo) -> datetime:
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
    """A half-open interval of instants, `[start, end)`, in UTC.

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

    def partition_dates(self, zone: tzinfo) -> tuple[date, ...]:
        """Return the calendar dates the range touches in a zone, oldest first.

        These are the flat-file partitions to read: every date from the date of `start` to the
        date of `end - 1 ns`, inclusive.

        Args:
            zone: The asset class's day zone.

        Returns:
            The dates, oldest first.
        """
        first = self.start.astimezone(zone).date()
        last = (self.end - timedelta(microseconds=1)).astimezone(zone).date()
        return tuple(first + timedelta(days=n) for n in range((last - first).days + 1))


def _epoch_ns(moment: datetime) -> int:
    """Convert an aware `datetime` to nanoseconds since the Unix epoch, exactly.

    Args:
        moment: An aware `datetime`.

    Returns:
        The epoch nanoseconds.
    """
    delta = moment - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86_400 + delta.seconds) * 10**9 + delta.microseconds * 1_000


def history_range(start: DateLike, end: DateLike, zone: tzinfo) -> TimeRange:
    """Normalize the bounds of `history()` (or of a `…T` time axis) to a half-open range.

    Each bound converts independently: a date `start` is that day's midnight in `zone`, a date
    `end` is the next day's midnight (whole days are inclusive), and an aware `datetime` is
    used as is.

    Args:
        start: The first day or instant to include.
        end: The last day to include, or the first instant to exclude.
        zone: The asset class's day zone (`DAY_ZONES[asset_class]`).

    Returns:
        The range in UTC.

    Raises:
        ValueError: If a bound cannot be parsed (see `parse_date_like`), or the range is empty
            (`start` is not before `end`).
    """
    first = parse_date_like(start, "start")
    last = parse_date_like(end, "end")
    lower = first if isinstance(first, datetime) else _midnight(first, zone)
    upper = last if isinstance(last, datetime) else _midnight(last + _ONE_DAY, zone)
    lower, upper = lower.astimezone(UTC), upper.astimezone(UTC)
    if lower >= upper:
        raise ValueError(f"start ({start!r}) must be before end ({end!r})")
    return TimeRange(lower, upper)


@dataclass(frozen=True, slots=True)
class FuturesSessions:
    """The futures sessions a date range names, and the bar request window around them.

    Attributes:
        first: The first `session_end_date` included.
        last: The last `session_end_date` included.
        window: The `window_start` range a bars request uses: from midnight in Chicago one day
            before `first` to midnight two days after `last`, so every bar of every included
            session is fetched; rows are then filtered to `SessionEndDate` in
            `[first, last]`.
    """

    first: date
    last: date
    window: TimeRange


def futures_sessions(start: date, end: date) -> FuturesSessions:
    """Return the futures sessions between two session dates, inclusive.

    A date names the trading session whose `session_end_date` equals it. Trades and quotes
    filter on `session_end_date` directly; bars widen the `window_start` range by one day on
    each side in America/Chicago. Bounds given as `datetime`s are not sessions: futures then
    filter on `Timestamp` with `history_range`.

    Args:
        start: The first session date.
        end: The last session date.

    Returns:
        The inclusive session dates and the widened bar window.

    Raises:
        ValueError: If `start` is after `end`.
    """
    if start > end:
        raise ValueError(f"start ({start}) must not be after end ({end})")
    window = TimeRange(_midnight(start - _ONE_DAY, CHICAGO), _midnight(end + 2 * _ONE_DAY, CHICAGO))
    return FuturesSessions(start, end, window)


def publish_time(partition: date, *, zone: tzinfo, cutoff: time) -> datetime:
    """Return the instant a day's flat file counts as published (DESIGN § 16).

    That is the first `cutoff` in New York strictly after the start of the next day,
    `partition + 1 day`, in the class's day zone. For a New York class it is the cutoff on the
    following day; for a UTC class it is the first cutoff after the following UTC midnight.

    Args:
        partition: The file's partition date.
        zone: The asset class's day zone.
        cutoff: `MassiveConfig.flat_file_publish_cutoff`, a New York wall-clock time.

    Returns:
        The publication instant, aware.
    """
    # Comparisons are made in UTC: Python compares two datetimes that share a tzinfo by their
    # wall clocks, which is wrong across the repeated hour of a daylight-saving change.
    start = _midnight(partition + _ONE_DAY, zone)
    local = start.astimezone(NEW_YORK)
    candidate = datetime.combine(local.date(), cutoff, tzinfo=NEW_YORK)
    if candidate.astimezone(UTC) <= start:
        candidate = datetime.combine(local.date() + _ONE_DAY, cutoff, tzinfo=NEW_YORK)
    return candidate


def flat_file_published(partition: date, *, now: datetime, zone: tzinfo, cutoff: time) -> bool:
    """Tell whether a day's flat file should exist yet (DESIGN § 16).

    Files land "by approximately 11:00 AM ET the following day", so a partition counts as
    published once `now` reaches `publish_time(partition)`. Full instants are compared, which
    keeps the rule correct in every day zone.

    Args:
        partition: The file's partition date.
        now: The current instant, aware.
        zone: The asset class's day zone.
        cutoff: `MassiveConfig.flat_file_publish_cutoff`, a New York wall-clock time.

    Returns:
        True if the file can be expected to exist.
    """
    published = publish_time(partition, zone=zone, cutoff=cutoff)
    return now.astimezone(UTC) >= published.astimezone(UTC)


def bounds(spec: DatasetSpec, start: DateLike, end: DateLike) -> TimeRange | FuturesSessions:
    """Normalize the bounds of a dataset's `history()` call by its asset class's day rules.

    Futures with two date bounds name trading sessions and get `FuturesSessions` (trades and
    quotes filter `session_end_date`; bars use the widened window). Every other case is a
    half-open `TimeRange`: date bounds are midnights in the class day zone (New York for
    datasets without a class, Chicago for a futures range given with a `datetime` bound, where
    futures filter on `Timestamp`). Unparseable, naive, or empty bounds raise the `ValueError`
    of `parse_date_like`, `history_range`, or `futures_sessions`.

    Args:
        spec: The dataset.
        start: The first day or instant, as passed or already parsed by `parse_date_like`
            (parsed bounds pass through unchanged).
        end: The last day, or the first instant excluded, in the same forms.

    Returns:
        The sessions for date-bounded futures, otherwise the instant range.
    """
    first = parse_date_like(start, "start")
    last = parse_date_like(end, "end")
    if spec.asset_class is AssetClass.FUTURES:
        if isinstance(first, datetime) or isinstance(last, datetime):
            return history_range(first, last, CHICAGO)
        return futures_sessions(first, last)
    zone = NEW_YORK if spec.asset_class is None else DAY_ZONES[spec.asset_class]
    return history_range(first, last, zone)
