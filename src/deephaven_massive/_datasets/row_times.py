"""The library's own time columns: when a row happened, its trading day, and when it arrived.

Every market-data row answers three questions, whichever transport served it:

- **When it happened.** A tick (a trade, a quote, an index value, an FMV, a LULD band, an
  imbalance) keeps `Timestamp`, the event time Massive sends. A bar, an indicator value, and a
  daily open-close summary have a window instead: `TimestampStart`, where it begins, and
  `TimestampEnd`, its exclusive end, the first instant at which the window is closed. Joins
  key on the end (`aj(..., on=["Symbol", "Timestamp>=TimestampEnd"])`), so a join never
  attaches a bar whose window is still open. The end is when the window closes, not when
  Massive published the bar, and Massive may correct a bar afterwards.
- **Its trading day.** `Date`, ISO `yyyy-MM-dd` text, is on every tick and bar table: the New
  York date for stocks, options, and indices, the UTC date for forex and crypto, and for
  futures the session (`session_end_date`, the trading date) the row belongs to. A bar that
  spans more than one date (several days, a week or longer, several futures sessions, or an
  intraday bar across midnight) has a null `Date`.
- **When the library received it.** `TimestampInsert`, the last column of every dataset, is the
  local clock (`time.time_ns()`) when the REST page, the flat-file block, or the WebSocket
  frame that carried the row arrived (a frame's rows of one channel are stamped together, as
  the decoded frame is handed over). Every row of one page or block shares it. It is
  not monotonic and is not Massive's own receive time; on `history()` rows it is the download
  time, and on a `poll()` table the latest poll's.

`RowTimes` says which of these a dataset has, and `TimeRule.fill` computes them for one batch
of rows already in the dataset's schema, after the transports have converted the vendor's
fields and, for bars of a day or longer, checked and moved their stamps
(`history_source.stamp_rule`). The vendor's own time fields feed these columns through the
dataset's declared keys and are not kept beside them: the bar start (`t`, `s`,
`window_start`) becomes `TimestampStart`, the WebSocket bar end `e` becomes `TimestampEnd`
(recomputed, see below), and the futures `session_end_date` and the open-close `from` and `day`
become `Date`.

**The windows.**

- **Tick.** `Date` is the day zone's date of `Timestamp`; for futures it is Massive's
  `session_end_date`, or on the WebSocket, which sends none, the 17:00 Chicago rule corrected
  by the schedules (`SessionDates`).
- **Intraday bar of n units.** `TimestampStart` is Massive's start and `TimestampEnd` the start
  plus n units (for a futures one-second WebSocket bar, the later of that and Massive's `e`).
  `Date` is the start's date, null when the bar ends on a later date; for futures it is the
  session as for ticks, null when the bar crosses 17:00 Chicago time.
- **1-day bar.** 00:00 of the day to 00:00 of the next day (01:00 New York time for indices);
  `Date` is the day.
- **n-day, week, month, quarter, or year bar.** 00:00 of the window's first day (a week's
  Monday; for stocks, options, and indices a period's first weekday) to 00:00 after its last
  day (last weekday for stocks and options; for indices, which can update around the clock,
  the period's last calendar day and one hour more); stock, option, and index days count as
  trading days with a holiday allowance (`dates.last_trading_day`); `Date` is null.
- **Futures session bar.** Massive's `window_start` to 17:00 Chicago time on its session date;
  `Date` is that date.
- **Futures bar of several sessions.** `window_start` to 17:00 Chicago time on the latest day
  its sessions can end on, counted with the same holiday allowance (`dates.last_trading_day`)
  from the session date of its start plus one more session (a folded first session opens on
  the evening before its holiday), or on its label if that is later; `Date` is null.
- **Futures week or longer.** `window_start` to 17:00 Chicago time on the last weekday of the
  latest period its label allows; `Date` is null.
- **Daily open-close.** As a 1-day bar of its date.

The indices' extra hour is conservative: an index day bar is stamped at midnight in Chicago,
and whether it covers the New York or the Chicago day is not verified, so its end is the later
of the two. The futures ends of bars of several sessions, weeks, or longer are conservative for
the same reason: Massive does not document whether such a bar is labelled with its first or its
last session, so the end is the later of the two readings. The next midnight is always the
calendar's next midnight in the zone, never 24 hours later, so a day across a daylight-saving
change keeps its true length.

**Zones.** Day boundaries are computed with `zoneinfo`, once per distinct UTC day of a batch
(and once more on a day that changes its clock), and applied to the whole column with Arrow;
Arrow's own time-zone database is not used, so nothing depends on one being installed.

The module is pure: no I/O, no Deephaven call, and its only state is the once-only log line of
a futures WebSocket bar whose end disagrees with its length.
"""

from __future__ import annotations

import calendar
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Final, Protocol, TypeVar, cast

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.dates import (
    CHICAGO,
    FUTURES_SESSION_ROLL,
    bar_days,
    day_end,
    day_zone,
    futures_period_last,
    futures_session,
    futures_session_end,
    intraday,
    last_trading_day,
    unit_ns,
)
from deephaven_massive._datasets.timestamps import ISSUES_URL
from deephaven_massive._enums import AssetClass, Timespan
from deephaven_massive._errors import MassiveError, log_once

if TYPE_CHECKING:
    from datetime import tzinfo

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import Request

__all__ = [
    "DATE",
    "END",
    "INSERT",
    "LIBRARY_COLUMNS",
    "START",
    "TIMESTAMP",
    "BarBounds",
    "RowTimes",
    "SessionDates",
    "StartRule",
    "TimeRule",
    "local_dates",
    "time_column",
    "time_rule",
    "within",
]

logger = logging.getLogger(__name__)

DATE: Final = "Date"
"""The trading day of a tick or bar row, ISO `yyyy-MM-dd` text."""
TIMESTAMP: Final = "Timestamp"
"""The event time of a tick, and the primary time of any other row that has one."""
START: Final = "TimestampStart"
"""The start of a bar's window."""
END: Final = "TimestampEnd"
"""The exclusive end of a bar's window: the first instant at which the bar is complete."""
INSERT: Final = "TimestampInsert"
"""When the library received the row: the last column of every dataset."""

LIBRARY_COLUMNS: Final = frozenset({DATE, TIMESTAMP, START, END, INSERT})
"""The time columns the library owns. On a dataset that has them, only its declared keys and
this module fill them: no vendor field keeps `Timestamp` or a `Timestamp<Qualifier>` outside a
declared key, nor `Date` on a tick, bar, or daily-summary dataset (a vendor `date` keeps the
name only on a dataset without the library's `Date`), and no `NAMES` entry of the refresh may
coin one."""

_DAY_NS: Final = 86_400 * 10**9
_SECOND_NS: Final = 10**9
_ROLL_NS: Final = FUTURES_SESSION_ROLL // timedelta(microseconds=1) * 1000
"""`dates.FUTURES_SESSION_ROLL` in nanoseconds, for the column form of the session rule."""
_EPOCH_WEEKDAY: Final = calendar.THURSDAY
"""The weekday (Monday is 0) of 1970-01-01, day 0 of the epoch."""
_NEVER: Final = 2**62
"""An instant later than any bounded timestamp: a day whose clock never changes."""
_INSTANT: Final = pa.timestamp("ns", tz="UTC")
_UTC_FLAG: Final = "IsUtc"
"""The column in which a daily summary (the crypto open-close) says its times are UTC."""
_EPOCH: Final = datetime(1970, 1, 1, tzinfo=UTC)

_Rows = TypeVar("_Rows", pa.RecordBatch, pa.Table)


class RowTimes(StrEnum):
    """Which of the library's time columns a dataset's rows carry (`DatasetSpec.row_times`).

    Every dataset ends with `TimestampInsert`, whatever its member.
    """

    TICK = "tick"
    """An event at one instant: `Date` and `Timestamp`. The trades, quotes, index values,
    FMV, LULD, and imbalances."""
    BAR = "bar"
    """A window of the query's timespan and multiplier: `Date`, `TimestampStart`, and
    `TimestampEnd`. The bars and the indicators, one value per bar."""
    DAY = "day"
    """One calendar day named by a date: `Date`, `TimestampStart`, and `TimestampEnd`, as a
    1-day bar. The daily open-close summaries."""
    NONE = "none"
    """Reference and other data: only `TimestampInsert` (and `Timestamp` where a time key is
    declared)."""
    BY_KIND = "by-kind"
    """A dataset declaration's default: the dataset's kind decides (`DatasetSpec.times`):
    `BAR` for a bars dataset, `TICK` for any other dataset in a kind, `NONE` otherwise. A
    resolved dataset never carries it."""


def time_column(row_times: RowTimes) -> str:
    """Return the column a dataset's declared `time` key takes.

    Args:
        row_times: The dataset's row times.

    Returns:
        `TimestampStart` for bars (and a daily summary, which declares no time key), and
        `Timestamp` for every other dataset.
    """
    return START if row_times in (RowTimes.BAR, RowTimes.DAY) else TIMESTAMP


class SessionDates(Protocol):
    """Corrects the futures session dates the 17:00 Chicago rule gives WebSocket rows.

    WebSocket futures messages carry no session date, so `TimeRule.fill` dates each such row
    by the rule and passes the dates here; the WebSocket transport's corrector moves the rows
    its schedules place in another session, and never waits on the network.
    """

    def __call__(self, symbols: pa.Array, instants: pa.Array, dates: pa.Array) -> pa.Array:
        """Return the rows' corrected session dates.

        Args:
            symbols: The rows' canonical symbols, `string`.
            instants: The rows' instants (a tick's time, a bar's start), epoch nanoseconds,
                `int64`.
            dates: The rule's session dates, ISO text.

        Returns:
            The dates to keep, ISO text, one per row.
        """
        ...


def _to_int(values: pa.Array | pa.ChunkedArray) -> pa.Array:
    """Return an instant or integer column as one `int64` array of epoch nanoseconds.

    Args:
        values: The column, chunked or not.

    Returns:
        The `int64` array.
    """
    if isinstance(values, pa.ChunkedArray):
        values = values.combine_chunks()
    return pc.cast(values, pa.int64())


def _column(rows: pa.RecordBatch | pa.Table, name: str) -> pa.Array:
    """Return one column of a batch or table as a single array.

    Args:
        rows: The rows.
        name: The column.

    Returns:
        The column, its chunks combined.
    """
    values = rows.column(name)
    return values.combine_chunks() if isinstance(values, pa.ChunkedArray) else values


def _offset_ns(zone: tzinfo, ns: int) -> int:
    """Return a zone's UTC offset at an instant, in nanoseconds.

    Args:
        zone: The zone.
        ns: The instant, epoch nanoseconds.

    Returns:
        The offset; negative west of Greenwich.
    """
    moment = _EPOCH + timedelta(microseconds=ns // 1000)
    offset = moment.astimezone(zone).utcoffset()
    return 0 if offset is None else offset // timedelta(microseconds=1) * 1000


def _day_offsets(zone: tzinfo, day: int) -> tuple[int, int, int]:
    """Return a zone's offsets over one UTC day: before, when it changes, and after.

    Args:
        zone: The zone.
        day: The UTC day, in days since the epoch.

    Returns:
        `(before, change, after)`: the offset at the day's start, the first instant of the day
        at the new offset (`_NEVER` when the clock does not change that day), and the offset at
        the day's end. The change is found to the second by bisection.
    """
    low = day * _DAY_NS
    high = low + _DAY_NS
    before, after = _offset_ns(zone, low), _offset_ns(zone, high)
    if before == after:
        return before, _NEVER, before
    lo, hi = low, high  # the change lies in (lo, hi]; both are whole seconds
    while hi - lo > _SECOND_NS:
        # At least two seconds apart, so the middle second lies strictly between them.
        middle = lo + (hi - lo) // 2 // _SECOND_NS * _SECOND_NS
        if _offset_ns(zone, middle) == before:
            lo = middle
        else:
            hi = middle
    return before, hi, after


def _local_ns(ns: pa.Array, zone: tzinfo) -> pa.Array:
    """Return epoch instants as wall-clock times of a zone, still counted in nanoseconds.

    One `zoneinfo` lookup per distinct UTC day of the column (bisected on a day the clock
    changes), applied with Arrow, so a column of a hundred million ticks costs a handful of
    Python calls.

    Args:
        ns: Epoch nanoseconds, `int64`; nulls stay null.
        zone: The zone.

    Returns:
        The wall-clock times as if they were UTC instants, `int64`.
    """
    if zone is UTC:
        return ns
    days = pc.divide(ns, _DAY_NS)
    distinct = pc.unique(days)
    befores, changes, afters = [], [], []
    for day in distinct.to_pylist():
        before, change, after = (0, _NEVER, 0) if day is None else _day_offsets(zone, day)
        befores.append(before)
        changes.append(change)
        afters.append(after)
    index = pc.index_in(days, value_set=distinct)
    before = pa.array(befores, pa.int64()).take(index)
    change = pa.array(changes, pa.int64()).take(index)
    after = pa.array(afters, pa.int64()).take(index)
    offset = pc.if_else(pc.greater_equal(ns, change), after, before)
    return pc.add(ns, offset)


def _days(ns: pa.Array, zone: tzinfo) -> pa.Array:
    """Return the zone's calendar date of each instant, as days since the epoch.

    Args:
        ns: Epoch nanoseconds, `int64`.
        zone: The zone.

    Returns:
        The days, `int64`; nulls stay null.
    """
    return pc.divide(_local_ns(ns, zone), _DAY_NS)


def _iso(days: pa.Array) -> pa.Array:
    """Return days since the epoch as ISO `yyyy-MM-dd` text.

    Args:
        days: The days, `int64`.

    Returns:
        The dates, `string`; nulls stay null.
    """
    return pc.cast(pc.cast(pc.cast(days, pa.int32()), pa.date32()), pa.string())


def local_dates(ns: pa.Array, zone: tzinfo) -> pa.Array:
    """Return the zone's calendar date of each instant, as ISO `yyyy-MM-dd` text.

    Args:
        ns: Epoch nanoseconds, `int64`.
        zone: The zone, such as New York or UTC.

    Returns:
        The dates, `string`; nulls stay null.
    """
    return _iso(_days(ns, zone))


def _session_days(ns: pa.Array) -> pa.Array:
    """Return the futures session of each instant by the 17:00 Chicago rule, as epoch days.

    The column form of `dates.futures_session`, from the same constant: the session ends on the
    Chicago date of the instant plus `FUTURES_SESSION_ROLL`, so an instant from 17:00 Chicago
    time on belongs to the next day's session.

    Args:
        ns: Epoch nanoseconds, `int64`.

    Returns:
        The session dates, days since the epoch, `int64`.
    """
    return pc.divide(pc.add(_local_ns(ns, CHICAGO), _ROLL_NS), _DAY_NS)


def _weekday_sessions(days: pa.Array) -> pa.Array:
    """Move session dates that fall on a weekend to the Monday after.

    The column form of `dates.weekday_on_or_after`: no futures session ends on a Saturday or
    Sunday.

    Args:
        days: Session dates, days since the epoch, `int64`.

    Returns:
        The dates, every one a weekday.
    """
    shifted = pc.add(days, _EPOCH_WEEKDAY)
    weekday = pc.subtract(shifted, pc.multiply(pc.divide(shifted, 7), 7))
    saturday, sunday = pc.equal(weekday, calendar.SATURDAY), pc.equal(weekday, calendar.SUNDAY)
    move = pc.if_else(saturday, 2, pc.if_else(sunday, 1, 0))
    return pc.add(days, move)


def _instant(ns: int) -> datetime:
    """Return epoch nanoseconds as an aware UTC `datetime`, to the microsecond.

    Args:
        ns: The instant; bar starts are positive, so the division floors.

    Returns:
        The instant.
    """
    return _EPOCH + timedelta(microseconds=ns // 1000)


def _ns(moment: datetime) -> int:
    """Return an aware `datetime` as epoch nanoseconds.

    Args:
        moment: The instant, a whole number of microseconds.

    Returns:
        The nanoseconds.
    """
    return (moment - _EPOCH) // timedelta(microseconds=1) * 1000


class StartRule(StrEnum):
    """How `history()` bounds the start of the bars it keeps (`BarBounds.start_rule`)."""

    RANGE = "range"
    """A bar's `TimestampStart` must be no earlier than the range's start."""
    LABEL = "label"
    """Bars are kept by their session label, not their start: futures named by session dates,
    whose bar may open before its first session's date when a holiday folds it."""


@dataclass(frozen=True, slots=True)
class BarBounds:
    """The window a `history()` call keeps its bars in, judged by their filled time columns.

    Built by `history_source.history_window` for a dataset of bars; frozen, so the reader
    threads of a call may share it.

    Attributes:
        earliest: The range's start: the earliest `TimestampStart` a kept bar may have under
            `StartRule.RANGE`.
        latest: The latest `TimestampEnd` a kept bar may have: the earlier of now and the
            range's end, which for dates is the last day's end (`dates.day_end`; for futures,
            17:00 Chicago on the last session's date).
        start_rule: Whether `earliest` bounds the bars' starts.
    """

    earliest: datetime
    latest: datetime
    start_rule: StartRule


def within(rows: _Rows, bounds: BarBounds) -> _Rows:
    """Keep the bars whose filled window lies inside a range: the whole-window rule.

    `history()` keeps a bar only when its window has ended by the end of the range and by
    now, judged by the same `TimestampEnd` a join on the end uses, so a bar a join would not
    yet attach is never returned; and, under `StartRule.RANGE`, only when its `TimestampStart`
    is no earlier than the range's start, so a bar whose conservative start reaches before the
    range (an index week stamped on its weekend) is left out. A row whose start or end is
    unknown (a futures bar sent without its start) is kept, the column null.

    Args:
        rows: Rows with their time columns filled (`TimeRule.fill`).
        bounds: The window (`HistoryWindow.bars`).

    Returns:
        The rows kept, in order; every row of a batch without the bar columns.
    """
    names = rows.schema.names
    if END not in names:
        return rows
    ends = rows.column(END)
    latest = pa.scalar(_ns(bounds.latest), ends.type)
    keep = pc.fill_null(pc.less_equal(ends, latest), fill_value=True)
    if bounds.start_rule is StartRule.RANGE and START in names:
        starts = rows.column(START)
        late = pc.greater_equal(starts, pa.scalar(_ns(bounds.earliest), starts.type))
        keep = pc.and_(keep, pc.fill_null(late, fill_value=True))
    return rows.filter(keep)


def _midnight(day: date, zone: tzinfo) -> datetime:
    """Return the first instant of a calendar day in a zone.

    Args:
        day: The day.
        zone: The zone.

    Returns:
        Its midnight, aware; the calendar's own midnight, so a day around a daylight-saving
        change keeps its length.
    """
    return datetime(day.year, day.month, day.day, tzinfo=zone)


def _instants(values: list[int | None]) -> pa.Array:
    """Return epoch nanoseconds as an Instant column.

    Args:
        values: The instants; None is null.

    Returns:
        The column, `timestamp("ns", "UTC")`.
    """
    return pa.array(values, pa.int64()).cast(_INSTANT)


@dataclass(frozen=True, slots=True)
class TimeRule:
    """How the time columns of one request's rows are computed (see the module docstring).

    Made per request (for a REST call of the indicators, per call, since each symbol's class
    picks its day zone) on the thread that reads it, and only read afterwards, so reader
    threads may share it.

    Example:
        ::

            from deephaven_massive._datasets.row_times import RowTimes, TimeRule
            from deephaven_massive._enums import AssetClass, Timespan

            rule = TimeRule("stocks.bars", RowTimes.BAR, AssetClass.STOCKS, Timespan.MINUTE, 5)
            filled = rule.fill(batch, received=time.time_ns())  # Date, ends, insert time

    Attributes:
        dataset: The dataset, for messages.
        row_times: Which time columns the rows carry.
        asset_class: The class whose day rules apply (`dates.day_class`): its day zone, and
            for futures its sessions; None for New York days.
        timespan: The bar unit of a `BAR` request; `DAY` for a daily summary; None for every
            other dataset.
        multiplier: How many units one bar spans; 1 for everything but bars.
    """

    dataset: str
    row_times: RowTimes
    asset_class: AssetClass | None
    timespan: Timespan | None = None
    multiplier: int = 1

    def __post_init__(self) -> None:
        """Reject a bar rule without a timespan.

        Raises:
            ValueError: If `row_times` is `BAR` and `timespan` is None: a bar's window needs
                its unit.
        """
        if self.row_times is RowTimes.BAR and self.timespan is None:
            raise ValueError(f"{self.dataset}: a bar rule needs its timespan")

    @property
    def _futures(self) -> bool:
        """True for futures rows, whose days are trading sessions."""
        return self.asset_class is AssetClass.FUTURES

    def fill(
        self,
        rows: _Rows,
        *,
        received: int | pa.Array,
        session_dates: SessionDates | None = None,
    ) -> _Rows:
        """Fill the time columns of one batch of rows, in place of what the batch carries.

        The batch is in the dataset's schema. Its `Timestamp` or `TimestampStart` is Massive's
        (for bars of a day or longer, already moved to 00:00 of their day by the stamp check),
        its `Date` the futures session or open-close date where the source sends one (else
        null), and its `TimestampEnd` Massive's WebSocket bar end where the source sends one
        (else null). This computes the rest by the table of the module docstring and sets
        every row's `TimestampInsert` to `received`.

        Args:
            rows: The batch, a `pyarrow.RecordBatch` or `pyarrow.Table` of the dataset's schema.
            received: When the page, block, or frame that carried the rows arrived, epoch
                nanoseconds (`time.time_ns()`): one instant for every row, or an `int64` array
                with one per row, for a batch gathered from several WebSocket frames.
            session_dates: For futures WebSocket rows, which carry no session date, the
                corrector of the rule's dates; None keeps the rule's dates.

        Returns:
            The rows, of the same type and schema, with `Date`, `TimestampStart`,
            `TimestampEnd`, and `TimestampInsert` filled as the dataset has them.

        A daily summary whose `IsUtc` is false (the crypto open-close) raises the
        `MassiveError` of `_daily`: its day would not be the UTC day its window is filled with.
        """
        arrived = (
            pa.repeat(pa.scalar(received, _INSTANT), rows.num_rows)
            if isinstance(received, int)
            else received
        )
        rows = _set(rows, INSERT, arrived)
        if self.row_times is RowTimes.TICK:
            return self._tick(rows, session_dates)
        if self.row_times is RowTimes.DAY:
            return self._daily(rows)
        if self.row_times is RowTimes.BAR:
            return self._bar(rows, session_dates)
        return rows

    def _zone(self) -> tzinfo:
        """Return the zone whose calendar days the rows' dates are."""
        return day_zone(self.asset_class)

    def _tick(self, rows: _Rows, session_dates: SessionDates | None) -> _Rows:
        """Fill a tick's `Date`: the zone's date of `Timestamp`, or its futures session.

        Args:
            rows: The batch.
            session_dates: The futures WebSocket corrector, or None.

        Returns:
            The batch with `Date`.
        """
        ns = _to_int(rows.column(TIMESTAMP))
        if self._futures:
            return _set(rows, DATE, self._sessions(rows, ns, session_dates))
        return _set(rows, DATE, local_dates(ns, self._zone()))

    def _sessions(self, rows: _Rows, ns: pa.Array, session_dates: SessionDates | None) -> pa.Array:
        """Return the futures session of each row: Massive's, else the rule's, corrected.

        Args:
            rows: The batch, whose `Date` holds Massive's `session_end_date` where the source
                sends one.
            ns: The rows' instants (a tick's time, a bar's start), epoch nanoseconds.
            session_dates: The WebSocket corrector, or None.

        Returns:
            The session dates, ISO text.
        """
        sent = _column(rows, DATE)
        if sent.null_count == 0:
            return sent
        ruled = _iso(_weekday_sessions(_session_days(ns)))
        if session_dates is not None:
            ruled = session_dates(_column(rows, "Symbol"), ns, ruled)
        return pc.coalesce(sent, ruled)

    def _daily(self, rows: _Rows) -> _Rows:
        """Fill a daily summary's window from its `Date`: that calendar day in the zone.

        Args:
            rows: The batch.

        Returns:
            The batch with `TimestampStart` and `TimestampEnd`.

        Raises:
            MassiveError: If a row's `IsUtc` (the crypto open-close's) is false: its day is
                filled as the UTC day, which would then cover other hours than Massive's.
        """
        if _UTC_FLAG in rows.schema.names:
            flags = _column(rows, _UTC_FLAG)
            if pc.any(pc.invert(pc.fill_null(flags, fill_value=True))).as_py():
                raise MassiveError(
                    f"{self.dataset}: Massive sent a day summary with isUTC false, so its day "
                    "is not the UTC day whose window the library gives it; report it at "
                    f"{ISSUES_URL}"
                )
        zone = self._zone()
        labels = _column(rows, DATE)
        distinct = pc.unique(labels)
        starts: list[int | None] = []
        ends: list[int | None] = []
        for text in distinct.to_pylist():
            if text is None:
                starts.append(None)
                ends.append(None)
                continue
            day = date.fromisoformat(text)
            starts.append(_ns(_midnight(day, zone)))
            ends.append(_ns(day_end(self.asset_class, day)))
        index = pc.index_in(labels, value_set=distinct)
        rows = _set(rows, START, _instants(starts).take(index))
        return _set(rows, END, _instants(ends).take(index))

    def _bar(self, rows: _Rows, session_dates: SessionDates | None) -> _Rows:
        """Fill a bar's window and `Date` by its timespan, multiplier, and class.

        Args:
            rows: The batch.
            session_dates: The futures WebSocket corrector, or None.

        Returns:
            The batch with `Date`, `TimestampStart`, and `TimestampEnd`.
        """
        timespan = cast("Timespan", self.timespan)  # a bar rule has one (`__post_init__`)
        if intraday(timespan):
            return self._intraday(rows, session_dates)
        if self._futures:
            return self._futures_long(rows, timespan)
        return self._days(rows, timespan)

    def _intraday(self, rows: _Rows, session_dates: SessionDates | None) -> _Rows:
        """Fill an intraday bar: its end is its start plus its length.

        Args:
            rows: The batch.
            session_dates: The futures WebSocket corrector, or None.

        Returns:
            The batch with `Date` and `TimestampEnd`.
        """
        timespan = cast("Timespan", self.timespan)
        start = _to_int(rows.column(START))
        end = pc.add(start, unit_ns(timespan) * self.multiplier)
        if self._futures and timespan is Timespan.SECOND:
            end = self._later_end(rows, end)
        last = pc.subtract(end, 1)
        if self._futures:
            dates = self._sessions(rows, start, session_dates)
            same = pc.equal(_session_days(start), _session_days(last))
        else:
            zone = self._zone()
            first_day = _days(start, zone)
            dates = _iso(first_day)
            same = pc.equal(first_day, _days(last, zone))
        rows = _set(rows, DATE, pc.if_else(same, dates, pa.scalar(None, pa.string())))
        return _set(rows, END, pc.cast(end, _INSTANT))

    def _later_end(self, rows: _Rows, computed: pa.Array) -> pa.Array:
        """Return the later of a futures one-second bar's computed end and Massive's `e`.

        Massive's documented futures one-second sample spans 60 seconds from `s` to `e`;
        whichever is right, the later end never attaches a bar before its window closed. A
        disagreement is logged once per process.

        Args:
            rows: The batch, whose `TimestampEnd` holds Massive's `e` where the source sends it.
            computed: The start plus one second, epoch nanoseconds.

        Returns:
            The ends, epoch nanoseconds.
        """
        sent = _to_int(rows.column(END))
        differs = pc.fill_null(pc.not_equal(sent, computed), fill_value=False)
        if pc.any(differs).as_py():
            first = pc.filter(sent, differs)[0].as_py()
            log_once(
                logger,
                f"futures-second-end:{self.dataset}",
                "%s: a one-second bar's end from Massive (%d ns) is not its start plus one "
                "second; the later of the two is kept as TimestampEnd",
                self.dataset,
                first,
                level=logging.WARNING,
            )
        later = pc.if_else(pc.greater(sent, computed), sent, computed)
        return pc.coalesce(later, computed)

    def _days(self, rows: _Rows, timespan: Timespan) -> _Rows:
        """Fill a bar of a day or longer of a class with a day zone.

        Its start is moved to 00:00 of its window's first day and its end is 00:00 after its
        window's last day (`dates.bar_days`), plus an hour for indices; a 1-day bar's `Date` is
        its day, and every longer bar's is null.

        Args:
            rows: The batch, whose starts the stamp check already moved to 00:00 of their day.
            timespan: The bar unit, a day or longer.

        Returns:
            The batch with `Date`, `TimestampStart`, and `TimestampEnd`.
        """
        zone = self._zone()
        single = timespan is Timespan.DAY and self.multiplier == 1
        ns = _to_int(rows.column(START))
        distinct = pc.unique(ns)
        starts: list[int | None] = []
        ends: list[int | None] = []
        dates: list[str | None] = []
        for value in distinct.to_pylist():
            if value is None:
                starts.append(None)
                ends.append(None)
                dates.append(None)
                continue
            day = _instant(value).astimezone(zone).date()
            first, after = bar_days(
                self.asset_class, day, timespan=timespan, multiplier=self.multiplier
            )
            if self.asset_class is AssetClass.INDICES:
                # An index can update at weekends: a week stamped on its Saturday or Sunday may
                # hold that day, so it starts there, the earlier reading.
                first = min(first, day)
            starts.append(_ns(_midnight(first, zone)))
            ends.append(_ns(day_end(self.asset_class, after - timedelta(days=1))))
            dates.append(day.isoformat() if single else None)
        index = pc.index_in(ns, value_set=distinct)
        rows = _set(rows, START, _instants(starts).take(index))
        rows = _set(rows, END, _instants(ends).take(index))
        return _set(rows, DATE, pa.array(dates, pa.string()).take(index))

    def _futures_long(self, rows: _Rows, timespan: Timespan) -> _Rows:
        """Fill a futures bar of a session or longer from its start and its session label.

        One session: it ends at 17:00 Chicago time on its date, which is its `Date`. Several
        sessions: 17:00 Chicago on the latest day its sessions can end on, or on its label when
        that is later. The sessions are counted with the holiday allowance
        (`dates.last_trading_day`) from the session date of its start, plus one: a bar whose
        first session a holiday folded opens on the evening before the holiday, whose date is
        the holiday itself. A week or longer: 17:00 Chicago on the last weekday of the
        latest period its label allows (`dates.futures_period_last`). Only a one-session bar
        keeps its `Date`.

        Args:
            rows: The batch, whose `Date` holds the bars' `session_end_date`.
            timespan: `SESSION`, `WEEK`, `MONTH`, `QUARTER`, or `YEAR`.

        Returns:
            The batch with `Date` and `TimestampEnd`.
        """
        single = timespan is Timespan.SESSION and self.multiplier == 1
        starts = _to_int(rows.column(START)).to_pylist()
        labels = _column(rows, DATE).to_pylist()
        ends: dict[tuple[int | None, str | None], int | None] = {}
        column: list[int | None] = []
        for start, label in zip(starts, labels, strict=True):
            key = (start, label)
            if key not in ends:
                ends[key] = self._futures_end(start, label, timespan)
            column.append(ends[key])
        rows = _set(rows, END, _instants(column))
        if single:
            return rows
        return _set(rows, DATE, pa.nulls(rows.num_rows, pa.string()))

    def _futures_end(self, start: int | None, label: str | None, timespan: Timespan) -> int | None:
        """Return the end of one futures bar of a session or longer.

        Args:
            start: Its `window_start`, epoch nanoseconds, or None.
            label: Its `session_end_date`, ISO text, or None.
            timespan: The bar unit.

        Returns:
            The end, epoch nanoseconds; None without a label.
        """
        if label is None:
            return None
        session = date.fromisoformat(label)
        if timespan is not Timespan.SESSION:
            last = futures_period_last(session, timespan=timespan, multiplier=self.multiplier)
            return _ns(futures_session_end(last))
        if self.multiplier == 1:
            return _ns(futures_session_end(session))
        if start is None:
            return None
        # A session a holiday folded opens on the evening before the holiday, so the date its
        # `window_start` falls in can be the holiday itself: count one session more.
        last = last_trading_day(futures_session(_instant(start)), self.multiplier + 1)
        return _ns(futures_session_end(max(session, last)))


def _set(rows: _Rows, name: str, values: pa.Array) -> _Rows:
    """Replace one column of a batch or table, keeping its field.

    Args:
        rows: The batch or table, which has the column.
        name: The column.
        values: Its new values, of the column's type.

    Returns:
        The rows with the column replaced.
    """
    index = rows.schema.get_field_index(name)
    field = rows.schema.field(index)
    return rows.set_column(index, field, values.cast(field.type))


def time_rule(
    spec: DatasetSpec, request: Request, *, asset_class: AssetClass | None = None
) -> TimeRule:
    """Return how the time columns of a request's rows are computed.

    Args:
        spec: The dataset.
        request: The normalized arguments: for bars and indicators, the timespan and the
            multiplier.
        asset_class: The class whose day rules the request follows (`dates.day_class`); None,
            the default, takes the dataset's own.

    Returns:
        The rule: of the request's timespan and multiplier for bars and indicators, of one day
        for a daily summary, and of neither for every other dataset.
    """
    times = spec.times
    zone_class = spec.asset_class if asset_class is None else asset_class
    if times is RowTimes.BAR:
        return TimeRule(spec.name, times, zone_class, request.timespan, request.multiplier)
    if times is RowTimes.DAY:
        return TimeRule(spec.name, times, zone_class, Timespan.DAY)
    return TimeRule(spec.name, times, zone_class)
