"""Bar periods: which bars of a timespan lie wholly inside a range, and the days a bar covers.

A bar is kept only when its whole window lies inside the range (`bar_starts`): an intraday or
1-day bar's window is its own interval. Massive starts a week on its Sunday, at 00:00 in the
class day zone, and stamps the week's bar there. For stocks and options, which do not trade at
weekends, a week's window is its Monday to Friday and months, quarters, and years are calendar
periods less their leading and trailing weekend days. Indices (which can update around the
clock), forex (which trades from Sunday evening UTC), and crypto keep their weekends: their
weeks run Sunday to Saturday and their months, quarters, and years are whole calendar periods.
A bar of several units spans that many. A bar of several days spans that many calendar days,
weekends included, as Massive counts them: a 2-day stock bar stamped on a Friday holds Friday
and Saturday, so Friday's trading alone, and the next one is stamped on the Sunday. A bar is
kept only when the `TimestampEnd` it gets lies inside the range and has passed
(`history_source`), so a bar whose window may not have ended yet (the current month, say) is
never returned. There is no holiday calendar: a stock week whose Friday is a holiday still runs
Monday to Friday.

**Futures bars of a session or longer** are buckets of whole UTC days. Massive puts each
session in the bucket that holds the day the session opens (17:00 Chicago, the evening before
its date), starts the bar's `window_start` at 00:00 UTC on the bucket's first day, and labels
the bar with the bucket's last session (`session_end_date`). A bucket of `n` sessions is `n`
calendar days, so it can hold fewer than `n` sessions; a week is 7 days, and months, quarters,
and years are calendar periods (`futures_bucket_end`). A bar has certainly ended once its
label's session has closed and, when the label may not yet be the bucket's last session, one
weekday after the latest session the bucket can hold (`futures_bar_ended`). Named by session
dates, such bars are requested from the first bucket that can start inside the range
(`futures_bar_request`, `futures_first_bucket_day`). Intraday futures bars sit on whole units of
UTC time like every other class's; named by date, their request starts
`FUTURES_HOLIDAY_MARGIN` before the first session's regular open, and their rows are kept by
their session, `Date`.

`history_source` keeps the bars `bar_starts` admits, and `row_times` fills a bar's window from
`bar_days` and judges whether a futures bar has ended with `futures_bar_ended`. Every function
is pure and thread-safe.
"""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING, Final

from deephaven_massive._datasets.dates import (
    DaySpan,
    TimeRange,
    day_start,
    day_zone,
    midnight,
    trades_at_weekends,
)
from deephaven_massive._datasets.epoch import EPOCH
from deephaven_massive._datasets.futures_sessions import (
    CHICAGO,
    FUTURES_HOLIDAY_MARGIN,
    add_weekdays,
    futures_session_end,
    futures_session_open,
    weekday_on_or_before,
)
from deephaven_massive._enums import AssetClass, ClassFromPrefix, Timespan

if TYPE_CHECKING:
    from collections.abc import Mapping


__all__ = [
    "FUTURES_BAR_CLOSE",
    "bar_days",
    "bar_starts",
    "futures_bar_ended",
    "futures_bar_request",
    "futures_bucket_end",
    "futures_first_bucket_day",
    "intraday",
    "unit_ns",
]


FUTURES_BAR_CLOSE: Final = time(18)
"""The Chicago wall-clock time up to which the futures bars of a last session are requested.

The session of a date ends by 16:00 Chicago time and the next one opens at 17:00, so 18:00
reaches every bar of the session; the bars of the next session the request also returns are
dropped by their session, `Date` (Massive's `session_end_date`)."""

_ONE_DAY: Final = timedelta(days=1)

_ONE_MICROSECOND: Final = timedelta(microseconds=1)

_DAYS_A_WEEK: Final = 7

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


def futures_bar_request(span: DaySpan, *, timespan: Timespan, multiplier: int) -> TimeRange:
    """Return the `window_start` range that reaches every futures bar of whole sessions.

    The range ends at `FUTURES_BAR_CLOSE` (18:00 Chicago time) on the last session date. It
    starts:

    - for intraday and 1-session bars, at the first session's regular open less
      `FUTURES_HOLIDAY_MARGIN`, so a session a holiday opened early is reached; the rows of
      other sessions the range also returns are dropped by their session, `Date`;
    - for bars of several sessions, at 00:00 UTC on `futures_first_bucket_day`, the first
      bucket day whose sessions all lie on or after the first date;
    - for week, month, quarter, and year bars, at 00:00 UTC on the first day of the period
      that holds the first date: a week's Sunday, or a month's, quarter's, or year's 1st.

    Args:
        span: The sessions.
        timespan: The bar unit.
        multiplier: How many units one bar spans, at least 1.

    Returns:
        The instants to request, in UTC.
    """
    upper = datetime.combine(span.last, FUTURES_BAR_CLOSE, tzinfo=CHICAGO).astimezone(UTC)
    if intraday(timespan) or (timespan is Timespan.SESSION and multiplier == 1):
        lower = futures_session_open(span.first) - FUTURES_HOLIDAY_MARGIN
        return TimeRange(lower.astimezone(UTC), upper)
    if timespan is Timespan.SESSION:
        # A futures bucket starts at 00:00 UTC of its first day.
        return TimeRange(midnight(futures_first_bucket_day(span.first), UTC), upper)
    period = _Periods(AssetClass.FUTURES, timespan).anchor(span.first)
    return TimeRange(midnight(period, UTC), upper)


def futures_first_bucket_day(first: date) -> date:
    """Return the earliest day a futures bucket may start on and hold no session before `first`.

    A session opens on its own date or an earlier day, never before the weekend that precedes
    it, so a bucket that starts on day `d` holds only sessions on or after the first weekday on
    or after `d`. That weekday is on or after `first` exactly when `d` is on or after the day
    after the last weekday before `first`: `first` itself on a Tuesday to Friday, and the
    Saturday before on a Monday, Saturday, or Sunday.

    Args:
        first: The first session date of a range.

    Returns:
        The day.
    """
    return weekday_on_or_before(first - _ONE_DAY) + _ONE_DAY


def futures_bucket_end(first_day: date, *, timespan: Timespan, multiplier: int) -> date:
    """Return the day after a futures bar's bucket, from the bucket's first day.

    Massive's grid: `multiplier` calendar days for bars of sessions, `7 * multiplier` days for
    weeks, and `multiplier` calendar months, quarters, or years from the period that holds the
    first day.

    Args:
        first_day: The bucket's first day, the UTC date of the bar's `window_start`.
        timespan: `SESSION`, `WEEK`, `MONTH`, `QUARTER`, or `YEAR`.
        multiplier: How many units one bar spans, at least 1.

    Returns:
        The first day after the bucket.
    """
    return _Periods(AssetClass.FUTURES, timespan).shift(first_day, multiplier)


def _latest_possible_session(after: date) -> date:
    """Return the latest session date a session opening inside a bucket of days can have.

    A session that opens on a Sunday to Thursday, at 17:00 Chicago time or later, ends on the
    next day; a session of a product that trades by day only (livestock opens at 08:30) ends
    on the day it opens, which can be a Friday. Nothing opens on a Saturday. So the bound is
    the day after the bucket's last Sunday to Thursday, or its last Friday when that is later.

    Args:
        after: The first day after the bucket.

    Returns:
        The latest possible session date, a weekday.
    """
    day = after - _ONE_DAY
    if day.weekday() == calendar.SATURDAY:
        day -= _ONE_DAY
    return day if day.weekday() == calendar.FRIDAY else day + _ONE_DAY


def futures_bar_ended(
    first_day: date, label: date, *, timespan: Timespan, multiplier: int
) -> datetime:
    """Return the instant from which a futures bar of a session or longer has certainly ended.

    A 1-session bar has ended at its `TimestampEnd`, 17:00 Chicago time on its label. A longer
    bar's label is the last session Massive has seen in its bucket, which is its last session
    only once the bucket is over: a session a holiday folded, or one that has not traded yet,
    can still follow. So when the label is earlier than the latest session the bucket can
    hold, the bar has ended only once one weekday more than that latest session has closed,
    and in any case not before its label's session closed.

    Args:
        first_day: The bucket's first day, the UTC date of the bar's `window_start`.
        label: The bar's `session_end_date`.
        timespan: `SESSION`, `WEEK`, `MONTH`, `QUARTER`, or `YEAR`.
        multiplier: How many units one bar spans, at least 1.

    Returns:
        The aware instant, in Chicago time.
    """
    closes = futures_session_end(label)
    if timespan is Timespan.SESSION and multiplier == 1:
        return closes
    after = futures_bucket_end(first_day, timespan=timespan, multiplier=multiplier)
    latest = _latest_possible_session(after)
    if label >= latest:
        return closes
    return futures_session_end(add_weekdays(latest, 1))


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

    A period is named by its *anchor*, its first calendar day: for a week, the Sunday Massive
    starts it on. A period's *window* is the days its bar covers. For stocks and options, which
    do not trade at weekends, the window of a week, month, quarter, or year leaves out the
    period's leading and trailing weekend days (a week runs Monday to Friday, and March 2024
    from Friday the 1st to Friday the 29th). An index can update around the clock
    (`I:AAVE100`), so its windows are whole calendar periods, weekends included, as are those
    of forex and crypto: a week runs Sunday to Saturday and March 2024 from the 1st to the
    31st, for its start and its end alike. A futures bar's periods are UTC calendar days, and
    so are its buckets (`futures_bucket_end`): a bucket of sessions counts calendar days, as
    Massive does. There is no holiday calendar: a holiday is a weekday like any other.

    Attributes:
        asset_class: The class.
        timespan: The bar unit: `DAY`, `SESSION`, `WEEK`, `MONTH`, `QUARTER`, or `YEAR`.
    """

    asset_class: AssetClass | ClassFromPrefix
    timespan: Timespan

    def day_of(self, moment: datetime) -> date:
        """Return the day an instant belongs to: its date in the class day zone.

        Args:
            moment: An aware instant.

        Returns:
            The day.
        """
        return moment.astimezone(day_zone(self.asset_class)).date()

    def day_start(self, day: date) -> datetime:
        """Return the instant a day starts: midnight in the class day zone.

        Args:
            day: The day.

        Returns:
            The instant, in UTC.
        """
        return day_start(self.asset_class, day)

    def anchor(self, day: date) -> date:
        """Return the anchor of the period a day belongs to.

        Args:
            day: The day.

        Returns:
            The period's first day: for a week, the Sunday on or before the day; for a month,
            quarter, or year, its 1st.
        """
        span = self.timespan
        if span is Timespan.WEEK:
            return day - timedelta(days=(day.weekday() - calendar.SUNDAY) % _DAYS_A_WEEK)
        if span in _PERIOD_MONTHS:
            size = _PERIOD_MONTHS[span]
            return date(day.year, (day.month - 1) // size * size + 1, 1)
        return day

    def shift(self, anchor: date, count: int) -> date:
        """Return the anchor `count` periods after another (before, when negative).

        Days and futures sessions count calendar days, and weeks and longer periods whole
        periods: a month's shift lands on the 1st.

        Args:
            anchor: A period's anchor.
            count: The number of periods.

        Returns:
            The anchor.
        """
        span = self.timespan
        if span is Timespan.WEEK:
            return anchor + timedelta(days=_DAYS_A_WEEK * count)
        if span in _PERIOD_MONTHS:
            months = anchor.year * 12 + anchor.month - 1 + _PERIOD_MONTHS[span] * count
            return date(months // 12, months % 12 + 1, 1)
        return anchor + timedelta(days=count)

    def _trimmed(self) -> bool:
        """Tell whether a period's window leaves out its leading and trailing weekend days.

        Returns:
            True for the weeks, months, quarters, and years of the classes that do not trade
            at weekends, except indices, whose windows are whole calendar periods since an
            index can update at weekends.
        """
        longer = self.timespan is Timespan.WEEK or self.timespan in _PERIOD_MONTHS
        weekdays = not trades_at_weekends(self.asset_class)
        return longer and weekdays and self.asset_class is not AssetClass.INDICES

    def window_start(self, anchor: date) -> date:
        """Return the first day of a period's window: its first weekday where weekends are out.

        Args:
            anchor: The period's anchor.

        Returns:
            The first day the period's bar covers.
        """
        day = anchor
        while self._trimmed() and day.weekday() >= calendar.SATURDAY:
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
        while self._trimmed() and (end - _ONE_DAY).weekday() >= calendar.SATURDAY:
            end -= _ONE_DAY
        return end

    def admitted(self, first: date, last: date, multiplier: int) -> tuple[date, date]:
        """Return the anchors of the first and last bars whose whole window lies in the days.

        Args:
            first: The first day of the range.
            last: The last day of the range.
            multiplier: How many periods one bar spans.

        Returns:
            The first and last anchors; the first is after the last when no bar fits.
        """
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
    micros = (moment - EPOCH) // _ONE_MICROSECOND
    unit = seconds * 1_000_000
    whole = -(-micros // unit) if up else micros // unit
    return EPOCH + timedelta(microseconds=whole * unit)


def bar_starts(
    asset_class: AssetClass | ClassFromPrefix,
    span: TimeRange,
    *,
    timespan: Timespan,
    multiplier: int,
) -> TimeRange:
    """Return the range of bar starts whose whole bar lies in a range.

    A bar is kept only when its whole window lies inside `span`. An intraday bar sits on whole
    units of UTC time (so on whole hours of every zone the library uses) and spans `multiplier`
    units from its start. A bar of a day or longer spans `multiplier` periods from the period
    its start belongs to (`_Periods`): days, weeks, or calendar months, quarters, and years. A
    week starts on its Sunday. A stock or option week covers Monday to Friday and its longer
    periods lose their leading and trailing weekend days, while indices, forex, and crypto keep
    whole calendar weeks (Sunday to Saturday) and periods. Days are those of the class's day
    zone. Futures bars of a session or longer are UTC-day buckets that this function does not
    select: `history_source` keeps them by their filled window (`futures_bar_ended`).

    Args:
        asset_class: The class whose days apply (`day_class`), or `ClassFromPrefix.ANY` for New
            York days; futures only for intraday bars.
        span: The range the bars must lie in. Pass an end no later than now, so a bar that
            has not finished yet is left out.
        timespan: The bar unit.
        multiplier: How many units one bar spans, at least 1.

    Returns:
        The half-open range of bar starts (`Timestamp`) to keep, empty when no bar fits. It
        starts where the first fitting period's days begin to belong to it (a week's Sunday,
        the 1st of a month even on a weekend), so a bar stamped anywhere in that period
        counts; a request for the bars starts there too.
    """
    if timespan in _INTRADAY_SECONDS:
        unit = _INTRADAY_SECONDS[timespan]
        low = _round_to_unit(span.start, unit, up=True)
        whole = timedelta(seconds=unit * (multiplier - 1))
        high = _round_to_unit(span.end, unit, up=False) - whole
        return TimeRange(low, max(low, high))
    periods = _Periods(asset_class, timespan)
    first = periods.day_of(span.start)
    if periods.day_start(first) < span.start:
        first += _ONE_DAY
    last = periods.day_of(span.end - _ONE_MICROSECOND)
    if periods.day_start(last + _ONE_DAY) > span.end:
        last -= _ONE_DAY
    first_period, last_period = periods.admitted(first, last, multiplier)
    lower = periods.day_start(first_period)
    upper = periods.day_start(periods.shift(last_period, 1))
    return TimeRange(lower, max(lower, upper))


def bar_days(
    asset_class: AssetClass | ClassFromPrefix,
    day: date,
    *,
    timespan: Timespan,
    multiplier: int,
) -> tuple[date, date]:
    """Return the days a bar of a day or longer covers, from a day of its first period.

    The window of `multiplier` periods from the period `day` belongs to: days, weeks, or
    calendar months, quarters, and years. A week starts on its Sunday. For stocks and options,
    which do not trade at weekends, a week covers its Monday to Friday and a longer period
    loses its leading and trailing weekend days; an index covers its whole calendar period,
    since an index can update at weekends, as forex and crypto do (a week runs Sunday to
    Saturday). A bar of several days spans that many calendar days, weekends included, as
    Massive counts them. It gives a bar's `TimestampStart` and `TimestampEnd`, the midnights of
    the two days in the class day zone.

    Args:
        asset_class: The class (`day_class`); not futures, whose bars are sessions.
        day: A day of the bar's first period: the day its stamp falls on.
        timespan: The bar unit: `DAY`, `WEEK`, `MONTH`, `QUARTER`, or `YEAR`.
        multiplier: How many units one bar spans, at least 1.

    Returns:
        The window's first day, and the day after its last day.
    """
    periods = _Periods(asset_class, timespan)
    anchor = periods.anchor(day)
    return periods.window_start(anchor), periods.window_end(periods.shift(anchor, multiplier - 1))


def unit_ns(timespan: Timespan) -> int:
    """Return the length of one intraday bar unit, in nanoseconds.

    Args:
        timespan: `SECOND`, `MINUTE`, or `HOUR`.

    Returns:
        The unit's length.
    """
    return _INTRADAY_SECONDS[timespan] * 10**9
