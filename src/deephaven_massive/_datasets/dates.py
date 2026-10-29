"""Date parsing and the day rules of `history()` and the time-axis filters.

Every time bound a user passes is a `DateLike`: an ISO-8601 string, a `date`, or a
timezone-aware `datetime`. A bound with a time of day but no timezone is ambiguous across the
three day zones Massive uses, so it is rejected with a `ValueError` that names the parameter,
the value, and the fix (`require_aware`).

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

Bars are kept only when their whole window lies inside the range (`bar_starts`, `bar_sessions`):
an intraday or 1-day bar's window is its own interval. For stocks, options, indices, and futures
sessions, which do not trade at weekends, a week runs Monday to Friday and months, quarters, and
years are calendar periods less their leading and trailing weekend days. Forex and crypto trade
at weekends (forex from Sunday evening UTC), so their weeks run Monday to Sunday and their
months, quarters, and years are whole calendar periods. A bar of several units spans that many;
several days of stocks, options, and indices count as trading days, since whether Massive counts
calendar or trading days is not documented and trading days end later, and without a holiday
calendar their end allows for holidays (`last_trading_day`), as a futures bar of several
sessions does. A bar is kept only when its conservative end, the `TimestampEnd` it gets, lies
inside the range and has passed (`history_source`), so a bar whose window may not have ended yet
(the current month, say) is never returned. There is no holiday calendar: a week whose Friday is
a holiday is still the Monday-to-Friday week.

The futures trading day rolls at 17:00 Chicago time, so an instant belongs to the session of
its Chicago date until 17:00 and to the next day's session from then on (`futures_session`).
An exchange holiday folds into the next session, which then opens earlier: after Good Friday
2024 the session of Monday 2024-04-01 opened on Thursday 2024-03-28 at 17:00. A request that
must reach whole sessions by time (futures bars named by date) starts
`FUTURES_HOLIDAY_MARGIN` before the first session's regular open (`futures_bar_request`), and
its rows are kept by their session, `Date`.

`day_start` and `instant_bound` turn a date sent to a REST parameter that compares instants
into its instant. `flat_file_published` decides whether a day's flat file can exist yet: files
land "by approximately 11:00 AM ET the following day", so a day's file counts as published
from the first configured cutoff (New York time) after the next day starts in the class's day
zone.

Every function is pure (the current time is a parameter) and thread-safe.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from typing import TYPE_CHECKING, Final
from zoneinfo import ZoneInfo

from deephaven_massive._enums import AssetClass, Timespan
from deephaven_massive._massive import WireUnit

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._types import DateLike

__all__ = [
    "CHICAGO",
    "DAY_ZONES",
    "DAY_ZONE_NAMES",
    "FUTURES_BAR_CLOSE",
    "FUTURES_HOLIDAY_MARGIN",
    "MIXED_BOUNDS",
    "NEW_YORK",
    "DaySpan",
    "TimeRange",
    "add_weekdays",
    "bar_days",
    "bar_sessions",
    "bar_starts",
    "bounds",
    "calendar_day",
    "check_order",
    "day_class",
    "day_end",
    "day_start",
    "day_zone",
    "flat_file_published",
    "futures_bar_request",
    "futures_period_last",
    "futures_session",
    "futures_session_before",
    "futures_session_end",
    "futures_session_open",
    "instant_bound",
    "intraday",
    "last_trading_day",
    "parse_date_like",
    "publish_time",
    "require_aware",
    "trades_at_weekends",
    "unit_ns",
]

NEW_YORK: Final = ZoneInfo("America/New_York")
"""The day zone of stocks, options, and indices, and the zone of the flat-file cutoff."""

CHICAGO: Final = ZoneInfo("America/Chicago")
"""The zone in which futures sessions open and roll (17:00 America/Chicago)."""

FUTURES_HOLIDAY_MARGIN: Final = timedelta(days=4)
"""How far before its regular open a futures session may start, so a request reaches it.

A holiday's trading folds into the next session, which then opens on an earlier evening. A
Monday holiday moves Tuesday's open one day earlier (the session of 2025-01-21 opened on
Sunday 2025-01-19 at 17:00 Chicago time). The longest known fold is a Friday holiday whose
trading goes to Monday's session (Good Friday 2024: the session of 2024-04-01 opened on
Thursday 2024-03-28 at 17:00), exactly three days before the regular Sunday open; the fourth
day is slack for rows Massive stamps just before an open. The rows a widened request returns
from earlier sessions are dropped by their session, `Date` (Massive's `session_end_date`)."""

FUTURES_BAR_CLOSE: Final = time(18)
"""The Chicago wall-clock time up to which the futures bars of a last session are requested.

The session of a date ends by 16:00 Chicago time and the next one opens at 17:00, so 18:00
reaches every bar of the session; the bars of the next session the request also returns are
dropped by their session, `Date` (Massive's `session_end_date`)."""

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
_ONE_DAY: Final = timedelta(days=1)
_INDEX_DAY_END: Final = timedelta(hours=1)
"""How much later an index day ends than the New York day: 01:00 New York time, midnight in
Chicago, since whether an index day bar covers the New York or the Chicago day is not
verified."""
_ONE_MICROSECOND: Final = timedelta(microseconds=1)
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)
_SATURDAY: Final = 5
_DAYS_A_WEEK: Final = 7
_WEEKEND_DAYS: Final = 2
_WEEKEND_TRADING: Final = frozenset({AssetClass.FOREX, AssetClass.CRYPTO})
"""The classes that trade at weekends, whose bar periods keep their Saturdays and Sundays."""

_INTRADAY_SECONDS: Final[Mapping[Timespan, int]] = {
    Timespan.SECOND: 1,
    Timespan.MINUTE: 60,
    Timespan.HOUR: 3600,
}
"""The length of one intraday bar unit, in seconds; such bars sit on whole units of UTC time."""

_PERIOD_MONTHS: Final[Mapping[Timespan, int]] = {
    Timespan.MONTH: 1,
    Timespan.QUARTER: 3,
    Timespan.YEAR: 12,
}
"""The calendar months of one bar unit of a timespan longer than a week."""


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
            names the parameter, the value, and the fix; `require_aware`), a string that is
            not ISO-8601, or a string with more than six fractional digits of a second, which
            a `datetime` would silently cut to microseconds.
    """
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        return value
    elif isinstance(value, str):
        text = value.strip()
        if _BELOW_MICROSECOND.search(text):
            raise ValueError(
                f"{name}={value!r} has more than six fractional digits of a second, which a "
                "datetime cannot hold; cutting them would move the time, so give at most "
                "microseconds"
            )
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
        return TimeRange(_midnight(self.first, zone), _midnight(self.last + _ONE_DAY, zone))


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


_SESSION_ROLL: Final = timedelta(hours=7)
"""Adding this to a Chicago time moves 17:00, when the next futures session opens, to midnight."""

FUTURES_SESSION_END: Final = time(17)
"""The Chicago wall-clock time a futures session is over by: the next session opens then, and the
session's own trading closes by 16:00 (the hour between is the daily maintenance break)."""


def futures_session_end(session: date) -> datetime:
    """Return the instant by which a futures session is over: 17:00 Chicago on its date.

    The session's trading closes by 16:00 Chicago time and the next session opens at 17:00, so
    17:00 is the first instant no part of the session can still be open, whatever its products'
    own closes. It is a futures session bar's `TimestampEnd`.

    Args:
        session: The session's `session_end_date`.

    Returns:
        The aware instant, in Chicago time.
    """
    return datetime.combine(session, FUTURES_SESSION_END, tzinfo=CHICAGO)


def add_weekdays(day: date, count: int) -> date:
    """Return the day `count` weekdays after another, or before it when `count` is negative.

    Futures sessions end on weekdays only, so counting sessions without a holiday calendar is
    counting weekdays: one weekday after a Friday is the Monday. A Saturday or Sunday counts as
    the gap it sits in: one weekday after it is the Monday, one before it the Friday.

    Args:
        day: The day to count from.
        count: The number of weekdays to move; 0 returns `day` unchanged.

    Returns:
        The day.
    """
    step = _ONE_DAY if count > 0 else -_ONE_DAY
    moved = day
    for _ in range(abs(count)):
        moved += step
        while moved.weekday() >= _SATURDAY:
            moved += step
    return moved


def _weekday_on_or_after(day: date) -> date:
    """Return the day itself on a weekday, else the Monday after it.

    Args:
        day: Any day.

    Returns:
        A weekday.
    """
    while day.weekday() >= _SATURDAY:
        day += _ONE_DAY
    return day


def _weekday_on_or_before(day: date) -> date:
    """Return the day itself on a weekday, else the Friday before it.

    Args:
        day: Any day.

    Returns:
        A weekday.
    """
    while day.weekday() >= _SATURDAY:
        day -= _ONE_DAY
    return day


def futures_session_open(session: date) -> datetime:
    """Return the instant a futures trading session opens: 17:00 Chicago on the day before.

    The inverse of `futures_session` at the session's first instant, from the same roll; a
    session a holiday folded into opens earlier (`FUTURES_HOLIDAY_MARGIN`).

    Args:
        session: The session's `session_end_date`.

    Returns:
        The aware instant, in Chicago time.
    """
    midnight = datetime(session.year, session.month, session.day, tzinfo=CHICAGO)
    return midnight - _SESSION_ROLL


def futures_session(moment: datetime) -> date:
    """Return the futures trading session an instant belongs to, as its `session_end_date`.

    The futures trading day rolls at 17:00 America/Chicago (the weekly open is Sunday 17:00
    CT, and 16:00 to 17:00 CT is the daily maintenance break), so an instant before 17:00
    Chicago time belongs to the session ending on its Chicago date, and one at or after 17:00 to
    the session ending the next day. Daylight-saving changes are followed through the zone.

    Args:
        moment: An aware `datetime`.

    Returns:
        The session's end date.
    """
    return (moment.astimezone(CHICAGO) + _SESSION_ROLL).date()


def futures_session_before(end: datetime) -> date:
    """Return the futures session of the last instant before an exclusive end.

    A range's `end` instant is excluded, so the range's last session is the one the instant
    just before it belongs to: an end at a session's 17:00 Chicago open closes the previous
    session, not the one that opens then.

    Args:
        end: An aware `datetime`, the first instant excluded.

    Returns:
        The session's end date (`futures_session` of `end` less one microsecond, the finest
        step a `datetime` takes).
    """
    return futures_session(end - _ONE_MICROSECOND)


def futures_bar_request(span: DaySpan) -> TimeRange:
    """Return the `window_start` range that reaches every futures bar of whole sessions.

    From the first session's regular open less `FUTURES_HOLIDAY_MARGIN`, so a session a holiday
    opened early is reached, to `FUTURES_BAR_CLOSE` (18:00 Chicago time) on the last session
    date. The rows of other sessions the range also returns are dropped by their session.

    Args:
        span: The sessions.

    Returns:
        The instants to request, in UTC.
    """
    lower = futures_session_open(span.first) - FUTURES_HOLIDAY_MARGIN
    upper = datetime.combine(span.last, FUTURES_BAR_CLOSE, tzinfo=CHICAGO)
    return TimeRange(lower.astimezone(UTC), upper.astimezone(UTC))


def trades_at_weekends(asset_class: AssetClass | None) -> bool:
    """Tell whether an asset class trades at weekends, so its Saturdays and Sundays hold data.

    Forex trades from Sunday evening UTC and crypto every day; stocks, options, indices,
    futures sessions, and data of no class do not.

    Args:
        asset_class: The class, or None.

    Returns:
        True for forex and crypto.
    """
    return asset_class in _WEEKEND_TRADING


def intraday(timespan: Timespan) -> bool:
    """Tell whether a bar timespan is shorter than a day (second, minute, or hour).

    Args:
        timespan: The bar unit.

    Returns:
        True for `SECOND`, `MINUTE`, and `HOUR`.
    """
    return timespan in _INTRADAY_SECONDS


@dataclass(frozen=True, slots=True)
class _Periods:
    """The bar periods of one asset class and timespan of a day or longer.

    A period is named by its *anchor*, its first calendar day (a Monday for a week). A day
    belongs to the period whose *membership* starts on or before it; a period's *window* is
    the days its bar covers. For a class that does not trade at weekends (stocks, options,
    indices, and futures sessions; not forex or crypto), the window of a week, month,
    quarter, or year leaves out the period's leading and trailing weekend days
    (a week runs Monday to Friday, and March 2024 from Friday the 1st to Friday the 29th), and
    a week's Saturday and Sunday belong to the next week, so a bar Massive stamps on the Sunday
    before a week still names that week. There is no holiday calendar: a holiday is a
    weekday like any other.

    Attributes:
        asset_class: The class; futures days are trading sessions.
        timespan: The bar unit: `DAY`, `SESSION`, `WEEK`, `MONTH`, `QUARTER`, or `YEAR`.
        multiplier: How many units one bar spans; a stock, option, or index bar of several
            days counts its days as weekdays (`sessions`).
        every_day: True to read weeks and longer periods as whole calendar periods, as for a
            class that trades every day.
    """

    asset_class: AssetClass | None
    timespan: Timespan
    multiplier: int = 1
    every_day: bool = False

    def day_of(self, moment: datetime) -> date:
        """Return the day an instant belongs to: its session for futures, else its zone date.

        Args:
            moment: An aware instant.

        Returns:
            The day.
        """
        if self.asset_class is AssetClass.FUTURES:
            return futures_session(moment)
        return moment.astimezone(day_zone(self.asset_class)).date()

    def day_start(self, day: date) -> datetime:
        """Return the instant a day starts: its session's regular open for futures, else midnight.

        Args:
            day: The day.

        Returns:
            The instant, in UTC.
        """
        return day_start(self.asset_class, day).astimezone(UTC)

    def anchor(self, day: date) -> date:
        """Return the anchor of the period a day belongs to.

        Args:
            day: The day.

        Returns:
            The period's first day: for a week, the Monday on or before the day. (The Saturday
            and Sunday before a Monday-to-Friday week still belong to it, through
            `membership`, when bar starts are kept.)
        """
        span = self.timespan
        if span is Timespan.WEEK:
            return day - timedelta(days=day.weekday())
        if span in _PERIOD_MONTHS:
            size = _PERIOD_MONTHS[span]
            return date(day.year, (day.month - 1) // size * size + 1, 1)
        return day

    @property
    def sessions(self) -> bool:
        """True when the periods are trading days, counted as weekdays.

        Futures sessions end on weekdays only. A stock, option, or index bar of several days
        may count calendar days or trading days, which Massive does not document; trading days
        end latest (a holiday inside the bar moves its end a day later), so its days count as
        trading days too, and its end allows for holidays (`last_trading_day`). Forex and
        crypto trade every day and keep calendar days.
        """
        if self.asset_class is AssetClass.FUTURES:
            return self.timespan is Timespan.SESSION
        several = self.timespan is Timespan.DAY and self.multiplier > 1
        return several and not self.every_day and not trades_at_weekends(self.asset_class)

    def period_of(self, day: date) -> date:
        """Return the anchor of the period a day belongs to, its leading weekend included.

        Args:
            day: A day of the period, such as the day Massive stamps a bar on.

        Returns:
            The anchor: for a Monday-to-Friday week, a Saturday or Sunday belongs to the week
            after it, so its anchor is the next Monday; otherwise `anchor(day)`.
        """
        if self._weekdays_only() and self.timespan is Timespan.WEEK and day.weekday() >= _SATURDAY:
            return self.anchor(day) + timedelta(days=_DAYS_A_WEEK)
        return self.anchor(day)

    def shift(self, anchor: date, count: int) -> date:
        """Return the anchor `count` periods after another (before, when negative).

        Futures sessions count weekdays (`add_weekdays`): the session after Friday's is
        Monday's. Days count calendar days, and weeks and longer periods whole periods.

        Args:
            anchor: A period's anchor.
            count: The number of periods.

        Returns:
            The anchor.
        """
        span = self.timespan
        if self.sessions:
            return add_weekdays(anchor, count)
        if span is Timespan.WEEK:
            return anchor + timedelta(days=_DAYS_A_WEEK * count)
        if span in _PERIOD_MONTHS:
            months = anchor.year * 12 + anchor.month - 1 + _PERIOD_MONTHS[span] * count
            return date(months // 12, months % 12 + 1, 1)
        return anchor + timedelta(days=count)

    def _weekdays_only(self) -> bool:
        """Tell whether the period's window leaves out its leading and trailing weekend days.

        Returns:
            True for weeks, months, quarters, and years of every class but forex and
            crypto, which trade at weekends.
        """
        longer = self.timespan is Timespan.WEEK or self.timespan in _PERIOD_MONTHS
        return longer and not self.every_day and not trades_at_weekends(self.asset_class)

    def window_start(self, anchor: date) -> date:
        """Return the first day of a period's window: its first weekday where weekends are out.

        Args:
            anchor: The period's anchor.

        Returns:
            The first day the period's bar covers.
        """
        day = anchor
        while self._weekdays_only() and day.weekday() >= _SATURDAY:
            day += _ONE_DAY
        return day

    def window_end(self, anchor: date) -> date:
        """Return the day after a period's window: a Saturday for a Monday-to-Friday week.

        Args:
            anchor: The period's anchor.

        Returns:
            The first day after the window: the next period's anchor, moved back over the
            period's trailing weekend days where weekends are out.
        """
        end = self.shift(anchor, 1)
        while self._weekdays_only() and (end - _ONE_DAY).weekday() >= _SATURDAY:
            end -= _ONE_DAY
        return end

    def membership(self, anchor: date) -> date:
        """Return the first day that belongs to a period.

        Args:
            anchor: The period's anchor.

        Returns:
            The Saturday before a Monday-to-Friday week; otherwise the anchor.
        """
        if self._weekdays_only() and self.timespan is Timespan.WEEK:
            return anchor - timedelta(days=_WEEKEND_DAYS)
        return anchor

    def last_member(self, anchor: date) -> date:
        """Return the last day that belongs to a period.

        Args:
            anchor: The period's anchor.

        Returns:
            The day before the next period's first member: for a Monday-to-Friday week, its
            Friday; for a futures session, the session's own date (a Saturday or Sunday ends
            no session).
        """
        if self.sessions:
            return anchor
        return self.membership(self.shift(anchor, 1)) - _ONE_DAY

    def admitted(self, first: date, last: date, multiplier: int) -> tuple[date, date]:
        """Return the anchors of the first and last bars whose whole window lies in the days.

        Args:
            first: The first day of the range.
            last: The last day of the range.
            multiplier: How many periods one bar spans.

        Returns:
            The first and last anchors; the first is after the last when no bar fits. For
            futures sessions both are weekdays: the first on or after `first`, and the last
            `multiplier - 1` weekdays before the last weekday on or before `last`.
        """
        if self.sessions:
            low = _weekday_on_or_after(first)
            return low, add_weekdays(_weekday_on_or_before(last), 1 - multiplier)
        low = self.anchor(first)
        if self.window_start(low) < first:
            low = self.shift(low, 1)
        high = self.anchor(last)
        if self.window_end(high) > last + _ONE_DAY:
            high = self.shift(high, -1)
        return low, self.shift(high, 1 - multiplier)


def _round_to_unit(moment: datetime, seconds: int, *, up: bool) -> datetime:
    """Round an instant to a whole number of units since the Unix epoch.

    Args:
        moment: An aware instant.
        seconds: The unit, in seconds.
        up: True to round up, False to round down.

    Returns:
        The rounded instant, in UTC.
    """
    micros = (moment - _EPOCH) // _ONE_MICROSECOND
    unit = seconds * 1_000_000
    whole = -(-micros // unit) if up else micros // unit
    return _EPOCH + timedelta(microseconds=whole * unit)


def bar_starts(
    asset_class: AssetClass | None, span: TimeRange, *, timespan: Timespan, multiplier: int
) -> TimeRange:
    """Return the range of bar starts whose whole bar lies in a range.

    A bar is kept only when its whole window lies inside `span`. An intraday bar sits on whole
    units of UTC time (so on whole hours of every zone the library uses) and spans `multiplier`
    units from its start. A bar of a day or longer spans `multiplier` periods from the period
    its start belongs to (`_Periods`): days, futures sessions, weeks, or calendar months,
    quarters, and years. Weeks run Monday to Friday and longer periods lose their leading and
    trailing weekend days, except for forex and crypto, which trade at weekends and keep
    whole calendar weeks and periods. Days are those of the class's day zone; for futures
    they are sessions, which open at 17:00 Chicago time on the day before.

    Args:
        asset_class: The class whose days apply (`day_class`), or None for New York days.
        span: The range the bars must lie in. Pass an end no later than now, so a bar that
            has not finished yet is left out.
        timespan: The bar unit.
        multiplier: How many units one bar spans, at least 1.

    Returns:
        The half-open range of bar starts (`Timestamp`) to keep, empty when no bar fits. It
        starts where the first fitting period's days begin to belong to it (the Saturday
        before a Monday-to-Friday week, the 1st of a month even on a weekend), so a bar
        stamped anywhere in that period counts; a request for the bars starts there too.
    """
    if timespan in _INTRADAY_SECONDS:
        unit = _INTRADAY_SECONDS[timespan]
        low = _round_to_unit(span.start, unit, up=True)
        whole = timedelta(seconds=unit * (multiplier - 1))
        high = _round_to_unit(span.end, unit, up=False) - whole
        return TimeRange(low, max(low, high))
    periods = _Periods(asset_class, timespan, multiplier)
    first = periods.day_of(span.start)
    if periods.day_start(first) < span.start:
        first += _ONE_DAY
    last = periods.day_of(span.end - _ONE_MICROSECOND)
    if periods.day_start(last + _ONE_DAY) > span.end:
        last -= _ONE_DAY
    first_period, last_period = periods.admitted(first, last, multiplier)
    lower = periods.day_start(periods.membership(first_period))
    upper = periods.day_start(periods.membership(periods.shift(last_period, 1)))
    return TimeRange(lower, max(lower, upper))


def bar_sessions(span: DaySpan, *, timespan: Timespan, multiplier: int) -> DaySpan:
    """Return the session labels (`Date`) of the futures bars whose whole window lies in sessions.

    A futures bar of a session or longer is labelled with a session date. A bar of one unit
    covers the period that date belongs to: the session, a Monday-to-Friday week, or a
    calendar month, quarter, or year of session dates less its leading and trailing weekend
    days. A bar of `multiplier` units covers that many periods, but Massive does not document
    whether its label is the first period's session or the last's, so a bar is kept only when
    it fits under both readings: its label's period and the `multiplier - 1` periods after it,
    and its label's period and the `multiplier - 1` periods before it, all lie inside `span`.
    Sessions count as weekdays (there is no holiday calendar).

    Args:
        span: The sessions the bars must lie in. End it no later than the last finished
            session, so a bar that has not finished yet is left out.
        timespan: The bar unit: `SESSION`, `WEEK`, `MONTH`, `QUARTER`, or `YEAR`.
        multiplier: How many units one bar spans, at least 1.

    Returns:
        The session dates to keep; empty when no bar fits.
    """
    periods = _Periods(AssetClass.FUTURES, timespan)
    low, high = periods.admitted(span.first, span.last, multiplier)
    # Labelled with its last period, a bar kept must start no earlier than `low`.
    earliest = periods.shift(low, multiplier - 1)
    return DaySpan(periods.membership(earliest), periods.last_member(high))


def last_trading_day(first: date, sessions: int) -> date:
    """Return the latest day a run of trading days can end on, without a holiday calendar.

    A bar of several trading days (a stock, option, or index bar of `multiplier` days, which
    Massive may count as trading days; a futures bar of several sessions) covers `sessions`
    trading days from `first`, and every weekday holiday inside it pushes its end one weekday
    later. Without a holiday calendar the end allows one holiday, and one more for every five
    further sessions: the exchanges never close on more weekdays than that within a run (from
    2024-12-24 the New York exchange holds six sessions over eight weekdays), and the bound
    holds for every run of the NYSE and CME calendars of 2024 to 2026. A join on the end
    therefore never sees the bar early.

    Args:
        first: The first trading day (for futures, session) of the run.
        sessions: How many trading days it spans, at least 1.

    Returns:
        The weekday `sessions - 1` trading days plus the holiday allowance after `first`.
    """
    return add_weekdays(first, sessions + (sessions - 1) // 5)


def bar_days(
    asset_class: AssetClass | None,
    day: date,
    *,
    timespan: Timespan,
    multiplier: int,
    every_day: bool = False,
) -> tuple[date, date]:
    """Return the days a bar of a day or longer covers, from a day of its first period.

    The window of `multiplier` periods from the period `day` belongs to: days, weeks, or
    calendar months, quarters, and years. For stocks, options, and indices, which do not trade
    at weekends, a week runs Monday to Friday (a Saturday or Sunday belongs to the week after
    it) and a longer period loses its leading and trailing weekend days; forex and crypto keep
    whole calendar weeks (Monday to Sunday) and periods. A stock, option, or index bar of
    several days may count trading days, which Massive does not document, so its window runs
    to the latest day such a run can end on (`last_trading_day`). It gives a bar's
    `TimestampStart` and `TimestampEnd`, the midnights of the two days in the class day zone.

    Args:
        asset_class: The class (`day_class`); not futures, whose bars are sessions.
        day: A day of the bar's first period: the day its stamp falls on.
        timespan: The bar unit: `DAY`, `WEEK`, `MONTH`, `QUARTER`, or `YEAR`.
        multiplier: How many units one bar spans, at least 1.
        every_day: True to read the periods as whole calendar periods whatever the class, as
            for a class that trades every day; the later reading for an index that updates
            around the clock.

    Returns:
        The window's first day, and the day after its last day.
    """
    periods = _Periods(None if every_day else asset_class, timespan, multiplier, every_day)
    anchor = periods.period_of(day)
    if periods.sessions:
        return anchor, last_trading_day(anchor, multiplier) + _ONE_DAY
    return periods.window_start(anchor), periods.window_end(periods.shift(anchor, multiplier - 1))


def futures_period_last(label: date, *, timespan: Timespan, multiplier: int) -> date:
    """Return the last weekday a futures bar of a week or longer can cover, from its label.

    The label is a session of the bar's first period or of its last; which is not documented.
    Read as the first, the bar ends with the last weekday of the `multiplier`-th period from
    the label's, which is the later of the two readings, so it is the conservative end.

    Args:
        label: The bar's `session_end_date`.
        timespan: The bar unit: `WEEK`, `MONTH`, `QUARTER`, or `YEAR`.
        multiplier: How many units one bar spans, at least 1.

    Returns:
        The last weekday of the bar's last period under the later reading.
    """
    periods = _Periods(AssetClass.FUTURES, timespan)
    last = periods.shift(periods.anchor(label), multiplier - 1)
    return periods.window_end(last) - _ONE_DAY


def unit_ns(timespan: Timespan) -> int:
    """Return the length of one intraday bar unit, in nanoseconds.

    Args:
        timespan: `SECOND`, `MINUTE`, or `HOUR`.

    Returns:
        The unit's length.
    """
    return _INTRADAY_SECONDS[timespan] * 10**9


def publish_time(partition: date, *, zone: tzinfo, cutoff: time) -> datetime:
    """Return the instant a day's flat file counts as published.

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
    """Tell whether a day's flat file should exist yet.

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


def day_end(asset_class: AssetClass | None, day: date) -> datetime:
    """Return the instant by which a class's trading day is over: a day bar's `TimestampEnd`.

    Every row that covers whole days ends here, and a `history()` range of dates ends at its
    last day's end, so a bar is kept by the same instant a join on its end would use. The
    next midnight is the calendar's next midnight in the class's day zone, never 24 hours
    later, so a day across a daylight-saving change keeps its length.

    Args:
        asset_class: The class (`day_class`), or None for New York days.
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


def day_zone(asset_class: AssetClass | None) -> tzinfo:
    """Return the zone whose calendar days an asset class's dates and instants are read in.

    New York for stocks, options, indices, and data of no class; UTC for forex and crypto;
    Chicago for futures, whose sessions roll there.

    Args:
        asset_class: The class, or None.

    Returns:
        The zone.
    """
    if asset_class is AssetClass.FUTURES:
        return CHICAGO
    return NEW_YORK if asset_class is None else DAY_ZONES[asset_class]


def day_class(spec: DatasetSpec, classes: Iterable[AssetClass] = ()) -> AssetClass | None:
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
        The class, or None for New York days.
    """
    if spec.asset_class is not None:
        return spec.asset_class
    shared = set(classes)
    if not spec.fetch_paths_by_class or len(shared) != 1:
        return None
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
        value: The day, as an ISO-8601 date string or a `date`.
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


def day_start(asset_class: AssetClass | None, day: date) -> datetime:
    """Return the instant a date starts under an asset class's day rules.

    A calendar day starts at midnight in the class's day zone (`day_zone`; New York without
    a class). A futures date names a trading session, which opens at 17:00 Chicago time on the
    day before its `session_end_date` (`futures_session_open`).

    Args:
        asset_class: The class whose rules apply (`day_class`), or None.
        day: The date.

    Returns:
        The aware instant, in the day zone.
    """
    if asset_class is AssetClass.FUTURES:
        return futures_session_open(day)
    return datetime(day.year, day.month, day.day, tzinfo=day_zone(asset_class))


def instant_bound(
    asset_class: AssetClass | None,
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
        asset_class: The class whose day rules apply (`day_class`), or None for New York days.
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
