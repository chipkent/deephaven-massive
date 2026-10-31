"""Bar periods: which bars of a timespan lie wholly inside a range, and the days a bar covers.

A bar is kept only when its whole window lies inside the range (`bar_starts`, `bar_sessions`):
an intraday or 1-day bar's window is its own interval. Massive starts a week on its Sunday, at
00:00 in the class day zone, and stamps the week's bar there. For stocks and options, which do
not trade at weekends, a week's window is its Monday to Friday and months, quarters, and years
are calendar periods less their leading and trailing weekend days. Indices (which can update
around the clock), forex (which trades from Sunday evening UTC), and crypto keep their weekends:
their weeks run Sunday to Saturday and their months, quarters, and years are whole calendar
periods. A futures week is the Monday-to-Friday week of session dates. A bar of several units
spans that many. A bar of several days, of every class but futures, spans that many calendar
days, weekends included, as Massive counts them: a 2-day stock bar stamped on a Friday holds
Friday and Saturday, so Friday's trading alone, and the next one is stamped on the Sunday. A
futures bar of several sessions counts weekdays, and without a holiday calendar its end allows
for holidays (`last_trading_day`). A bar is kept only when its conservative end, the `TimestampEnd`
it gets, lies inside the range and has passed (`history_source`), so a bar whose window may not
have ended yet (the current month, say) is never returned. There is no holiday calendar: a stock
week whose Friday is a holiday still runs Monday to Friday.

A request that must reach whole futures sessions by time (futures bars named by date) starts
`FUTURES_HOLIDAY_MARGIN` before the first session's regular open (`futures_bar_request`), and
its rows are kept by their session, `Date`.

`history_source` keeps the bars `bar_starts` and `bar_sessions` admit, and `row_times` fills a
bar's window from `bar_days`, `last_trading_day`, and `futures_period_last`. Every function is
pure and thread-safe.
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
    trades_at_weekends,
)
from deephaven_massive._datasets.epoch import EPOCH
from deephaven_massive._datasets.futures_sessions import (
    CHICAGO,
    FUTURES_HOLIDAY_MARGIN,
    add_weekdays,
    futures_session,
    futures_session_open,
    weekday_on_or_after,
    weekday_on_or_before,
)
from deephaven_massive._enums import AssetClass, ClassFromPrefix, Timespan

if TYPE_CHECKING:
    from collections.abc import Mapping


__all__ = [
    "FUTURES_BAR_CLOSE",
    "bar_days",
    "bar_sessions",
    "bar_starts",
    "futures_bar_request",
    "futures_period_last",
    "intraday",
    "last_trading_day",
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

_WEEKEND_DAYS: Final = 2

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
    starts it on, or for futures the Monday of its Monday-to-Friday week of sessions. A day
    belongs to the period whose *membership* starts on or before it; a period's *window* is
    the days its bar covers. For stocks and options, which do not trade at weekends, the window
    of a week, month, quarter, or year leaves out the period's leading and trailing weekend
    days (a week runs Monday to Friday, and March 2024 from Friday the 1st to Friday the 29th).
    A futures week's Saturday and Sunday belong to the week after them, as their trading does.
    An index can update around the clock (`I:AAVE100`), so its windows are whole calendar
    periods, weekends included, as are those of forex and crypto: a week runs Sunday to
    Saturday and March 2024 from the 1st to the 31st, for its start and its end alike. There
    is no holiday calendar: a holiday is a weekday like any other.

    Attributes:
        asset_class: The class; futures days are trading sessions.
        timespan: The bar unit: `DAY`, `SESSION`, `WEEK`, `MONTH`, `QUARTER`, or `YEAR`.
    """

    asset_class: AssetClass | ClassFromPrefix
    timespan: Timespan

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
        return day_start(self.asset_class, day)

    def anchor(self, day: date) -> date:
        """Return the anchor of the period a day belongs to.

        Args:
            day: The day.

        Returns:
            The period's first day: for a week, the Sunday on or before the day, or for
            futures the Monday. (The Saturday and Sunday before a futures week still belong to
            it, through `membership`.)
        """
        span = self.timespan
        if span is Timespan.WEEK:
            first = calendar.MONDAY if self.asset_class is AssetClass.FUTURES else calendar.SUNDAY
            return day - timedelta(days=(day.weekday() - first) % _DAYS_A_WEEK)
        if span in _PERIOD_MONTHS:
            size = _PERIOD_MONTHS[span]
            return date(day.year, (day.month - 1) // size * size + 1, 1)
        return day

    @property
    def sessions(self) -> bool:
        """True when the periods are futures sessions, counted as weekdays.

        Futures sessions end on weekdays only. The days of every other class are calendar
        days, for a bar of several days too: Massive stamps 2-day stock bars on a Friday and
        then on the Sunday, each holding its own two calendar days.
        """
        return self.asset_class is AssetClass.FUTURES and self.timespan is Timespan.SESSION

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

    def _weekend_leads(self) -> bool:
        """Tell whether a week's leading Saturday and Sunday belong to it: a futures week's do.

        Returns:
            True for a futures week, whose Monday-to-Friday sessions follow the weekend before
            it; False for every other class, whose week starts on its Sunday.
        """
        return self.timespan is Timespan.WEEK and self.asset_class is AssetClass.FUTURES

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

    def membership(self, anchor: date) -> date:
        """Return the first day that belongs to a period.

        Args:
            anchor: The period's anchor.

        Returns:
            The Saturday before a futures week; otherwise the anchor.
        """
        if self._weekend_leads():
            return anchor - timedelta(days=_WEEKEND_DAYS)
        return anchor

    def last_member(self, anchor: date) -> date:
        """Return the last day that belongs to a period.

        Args:
            anchor: The period's anchor.

        Returns:
            The day before the next period's first member: for a futures week, its Friday;
            for a futures session, the session's own date (a Saturday or Sunday ends no
            session).
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
            low = weekday_on_or_after(first)
            return low, add_weekdays(weekday_on_or_before(last), 1 - multiplier)
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
    its start belongs to (`_Periods`): days, futures sessions, weeks, or calendar months,
    quarters, and years. A week starts on its Sunday. A stock or option week covers Monday to
    Friday and its longer periods lose their leading and trailing weekend days, while indices,
    forex, and crypto keep whole calendar weeks (Sunday to Saturday) and periods. Days are
    those of the class's day zone; for futures they are sessions, which open at 17:00 Chicago
    time on the day before.

    Args:
        asset_class: The class whose days apply (`day_class`), or `ClassFromPrefix.ANY` for New
            York days.
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

    A futures bar of several sessions covers `sessions` trading days from `first`, and every
    weekday holiday inside it pushes its end one weekday later. Without a holiday calendar the
    end allows one holiday, and one more for every five further sessions: the exchanges never
    close on more weekdays than that within a run (from 2024-12-24 the New York exchange holds
    six sessions over eight weekdays), and the bound holds for every run of the NYSE and CME
    calendars of 2024 to 2026. A join on the end therefore never sees the bar early.

    Args:
        first: The first trading day (for futures, session) of the run.
        sessions: How many trading days it spans, at least 1.

    Returns:
        The weekday `sessions - 1` trading days plus the holiday allowance after `first`.
    """
    return add_weekdays(first, sessions + (sessions - 1) // 5)


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
