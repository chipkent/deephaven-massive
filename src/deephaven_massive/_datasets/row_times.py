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
(`stamp_rule`). The vendor's own time fields feed these columns through the
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
- **n-day, week, month, quarter, or year bar.** 00:00 of the window's first day (for stocks
  and options a period's first weekday, a week's Monday; for every other class its first
  calendar day, a week's Sunday) to 00:00 after its last day (last weekday for stocks and
  options; for indices, which can update around the clock, the period's last calendar day and
  one hour more); stock, option, and index days count as trading days with a holiday
  allowance (`bar_periods.last_trading_day`); `Date` is null.
- **Futures session bar.** Massive's `window_start` to 17:00 Chicago time on its session date;
  `Date` is that date.
- **Futures bar of several sessions.** `window_start` to 17:00 Chicago time on the latest day
  its sessions can end on, counted with the same holiday allowance (`bar_periods.last_trading_day`)
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

**Bar stamps.** Bars of a day or longer are stamped at 00:00 of the day they start on, in the
class day zone, whichever source serves them, before their time columns are filled. Massive
stamps the same bar differently by source (the day's start on the aggregates range, the 16:00
New York session close on the stock grouped daily and `/prev`, the day's last millisecond in
the forex and crypto day files), so each stamp at a time of day Massive is known to use moves
to that day's 00:00 (`day_starts`), and any other raises `MassiveError`, so a bar of another
day cut is never returned under a label it may not match. `stamp_rule` decides which times
each class accepts, and the transports apply it through `StampRule.stamp`.

**Zones.** Day boundaries are computed with `zoneinfo`, once per distinct UTC day of a batch
(and once more on a day that changes its clock), and applied to the whole column with Arrow;
Arrow's own time-zone database is not used, so nothing depends on one being installed.

The module is long but has one subject, a row's time columns: the bar-stamp rule is the first
step of filling a bar's window, and every other rule here fills one of those columns or keeps
rows by them.

The module is pure: no I/O, no Deephaven call, and its only state is the once-only log line of
a futures WebSocket bar whose end disagrees with its length.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Final, Protocol, TypeVar, cast

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.bar_periods import (
    bar_days,
    futures_period_last,
    intraday,
    last_trading_day,
    unit_ns,
)
from deephaven_massive._datasets.dates import (
    DAY_ZONES,
    day_end,
    day_zone,
)
from deephaven_massive._datasets.futures_sessions import (
    CHICAGO,
    futures_session,
    futures_session_end,
    session_days,
    weekday_sessions,
)
from deephaven_massive._enums import AssetClass, ClassFromPrefix, Timespan
from deephaven_massive._errors import ErrorCause, MassiveError
from deephaven_massive._log_limits import log_once

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping
    from datetime import tzinfo

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import Request

__all__ = [
    "DATE",
    "END",
    "INSERT",
    "ISSUES_URL",
    "LIBRARY_COLUMNS",
    "START",
    "TIMESTAMP",
    "BarBounds",
    "RowTimes",
    "SessionDates",
    "StampRule",
    "StartRule",
    "TimeRule",
    "day_starts",
    "local_dates",
    "stamp_rule",
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
            for futures its sessions; `ClassFromPrefix.ANY` for New York days.
        timespan: The bar unit of a `BAR` request; `DAY` for a daily summary; None for every
            other dataset.
        multiplier: How many units one bar spans; 1 for everything but bars.
    """

    dataset: str
    row_times: RowTimes
    asset_class: AssetClass | ClassFromPrefix
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
        ruled = _iso(weekday_sessions(session_days(_local_ns(ns, CHICAGO))))
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
                    f"{ISSUES_URL}",
                    cause=ErrorCause.LIBRARY_DEFECT,
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
            same = pc.equal(
                session_days(_local_ns(start, CHICAGO)), session_days(_local_ns(last, CHICAGO))
            )
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
        disagreement is logged once per process, at INFO: the later end is already the safe
        choice, so there is nothing for the user to do.

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
            )
        later = pc.if_else(pc.greater(sent, computed), sent, computed)
        return pc.coalesce(later, computed)

    def _days(self, rows: _Rows, timespan: Timespan) -> _Rows:
        """Fill a bar of a day or longer of a class with a day zone.

        Its start is moved to 00:00 of its window's first day and its end is 00:00 after its
        window's last day (`bar_periods.bar_days`), plus an hour for indices; a 1-day bar's
        `Date` is its day, and every longer bar's is null.

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
        (`bar_periods.last_trading_day`) from the session date of its start, plus one: a bar whose
        first session a holiday folded opens on the evening before the holiday, whose date is
        the holiday itself. A week or longer: 17:00 Chicago on the last weekday of the
        latest period its label allows (`bar_periods.futures_period_last`). Only a one-session bar
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
    spec: DatasetSpec, request: Request, *, asset_class: AssetClass | ClassFromPrefix
) -> TimeRule:
    """Return how the time columns of a request's rows are computed.

    Args:
        spec: The dataset.
        request: The normalized arguments: for bars and indicators, the timespan and the
            multiplier.
        asset_class: The class whose day rules the request follows (`dates.day_class`): the
            dataset's own, or for a dataset of every class (the indicators) its symbols'.

    Returns:
        The rule: of the request's timespan and multiplier for bars and indicators, of one day
        for a daily summary, and of neither for every other dataset.
    """
    times = spec.times
    if times is RowTimes.BAR:
        return TimeRule(spec.name, times, asset_class, request.timespan, request.multiplier)
    if times is RowTimes.DAY:
        return TimeRule(spec.name, times, asset_class, Timespan.DAY)
    return TimeRule(spec.name, times, asset_class)


@dataclass(frozen=True, slots=True)
class StampRule:
    """How the transports stamp the bars of a request of a day or longer (`stamp_rule`).

    Made per request on the calling thread and only read afterwards, so reader threads may
    share it. A transport sets `remedy` for its own verb (`dataclasses.replace`) before
    stamping.

    Attributes:
        zone: The class day zone; every bar is stamped at 00:00 there of the day it starts on.
        known: The other wall-clock times of day, in `zone`, at which Massive is known to stamp
            the class's bars of this length on some source; each moves to 00:00 of its day.
        what: The kind of row, for messages: `"1-day bar"`, `"bar"`, or `"indicator value"`.
        weekdays: For week bars, the weekdays (Monday is 0) a bar may be stamped on in `zone`;
            empty for every other timespan.
        months: For month, quarter, and year bars, the months whose 1st a bar may be stamped
            on in `zone`; empty for every other timespan.
        remedy: What the caller can do instead when a stamp is refused, for the message.
    """

    zone: tzinfo
    known: frozenset[time]
    what: str
    weekdays: frozenset[int] = frozenset()
    months: frozenset[int] = frozenset()
    remedy: str = ""

    def stamp(self, label: str, values: pa.Array, *, source: str) -> pa.Array:
        """Stamp a column of bars at 00:00 of their days (`day_starts`).

        Args:
            label: The dataset, for the message.
            values: The bars' `Timestamp` column, an Arrow `timestamp("ns", "UTC")` array.
            source: Where the bars came from, for the message: a REST path template or a
                flat-file prefix.

        Returns:
            The stamps at the start of their days, in the same type; nulls stay null.

        A stamp at another time of day than 00:00 or one of `known`, or on a day `weekdays`
        or `months` rules out, raises the `MassiveError` of `day_starts`, which
        ends with `remedy`.
        """
        return day_starts(
            label,
            values,
            zone=self.zone,
            known=self.known,
            weekdays=self.weekdays,
            months=self.months,
            source=source,
            what=self.what,
            remedy=self.remedy,
        )


_SESSION_CLOSE: Final = time(16)
"""The 16:00 New York session close: grouped daily, `/prev`, and the stock day files stamp a
stock, option, or index 1-day bar there."""

_EARLY_CLOSE: Final = time(13)
"""The 13:00 New York close of a short trading day (the day after Thanksgiving, Christmas Eve,
July 3), presumed to be where a source that stamps a 1-day bar at its close stamps such a
day. It lies inside its own New York day, so accepting it on any date never changes which day
a bar is labelled with."""

_INDEX_OPTION_CLOSES: Final = frozenset({time(13, 15), time(16, 15)})
"""The closes of index options such as SPX and SPXW, 15 minutes after the stock market's on
full and short days, presumed to be where `/prev` may stamp such a contract's 1-day bar."""

_CHICAGO_MIDNIGHT: Final = time(1)
"""Midnight in Chicago, as New York time (the two zones change clocks together): the indices
range and day files stamp their 1-day bars there."""

_LAST_MILLISECOND: Final = time(23, 59, 59, 999_000)
"""The last millisecond of a UTC day: forex grouped daily and `/prev` and the forex and crypto
day files stamp their 1-day bars there."""

_ONE_DAY_STAMPS: Final[Mapping[AssetClass, frozenset[time]]] = {
    AssetClass.STOCKS: frozenset({_EARLY_CLOSE, _SESSION_CLOSE}),
    AssetClass.OPTIONS: frozenset({_EARLY_CLOSE, _SESSION_CLOSE, *_INDEX_OPTION_CLOSES}),
    AssetClass.INDICES: frozenset({_CHICAGO_MIDNIGHT, _EARLY_CLOSE, _SESSION_CLOSE}),
    AssetClass.FOREX: frozenset({_LAST_MILLISECOND}),
    AssetClass.CRYPTO: frozenset({_LAST_MILLISECOND}),
}
"""Per class with a day zone (`dates.DAY_ZONES`), the times of day besides 00:00 at which some
source stamps a 1-day bar: those of Massive's documented samples, and the closes of a short
trading day and of index options, each of which lies inside its own New York day."""

_LONGER_STAMPS: Final[Mapping[AssetClass, frozenset[time]]] = {
    AssetClass.STOCKS: frozenset(),
    AssetClass.OPTIONS: frozenset(),
    AssetClass.INDICES: frozenset({_CHICAGO_MIDNIGHT}),
    AssetClass.FOREX: frozenset(),
    AssetClass.CRYPTO: frozenset(),
}
"""Per class with a day zone, the times of day besides 00:00 at which the aggregates range
stamps its bars: accepted for a bar of several days, a week, or longer, which only the range
serves, and for an indicator value of a day or longer, which is computed over the range's
bars. The indices range's midnight in Chicago is that of its 1-day bars and of the index
indicator samples' underlying aggregates."""

_SUNDAY: Final = frozenset({6})
"""The day a week bar must be stamped on, in every class: the Sunday Massive starts its week on
(00:00 UTC for forex and crypto, 00:00 New York time for stocks, options, and indices)."""

_PERIOD_MONTHS: Final[Mapping[Timespan, frozenset[int]]] = {
    Timespan.MONTH: frozenset(range(1, 13)),
    Timespan.QUARTER: frozenset({1, 4, 7, 10}),
    Timespan.YEAR: frozenset({1}),
}
"""The months whose 1st a month, quarter, or year bar may be stamped on."""

_DAYS_OR_LONGER: Final = frozenset(
    {Timespan.DAY, Timespan.WEEK, Timespan.MONTH, Timespan.QUARTER, Timespan.YEAR}
)


def stamp_rule(
    spec: DatasetSpec,
    request: Request,
    *,
    asset_class: AssetClass | ClassFromPrefix = ClassFromPrefix.ANY,
) -> StampRule | None:
    """Return how the transports stamp a request's bars, or None to keep the stamps as sent.

    One `history()` call returns the same rows whichever source answers it, so every bar of a
    day or longer of a class with a day zone (`dates.DAY_ZONES`: every class but futures) is
    stamped at 00:00 of the day it starts on in that zone (New York for stocks, options, and
    indices; UTC for forex and crypto). Massive stamps a 1-day bar at the day's start on the
    aggregates range but at the session close (stocks, options, indices) or the day's last
    millisecond (forex, crypto) on other sources, and the indices range and day files at
    midnight in Chicago; those stamps move to the day's start, and any other stamp fails,
    since such a bar may cover other days than its label (the documented crypto REST samples
    sit at midnight New York time, another day cut). A week, month, quarter, or year bar must
    also be stamped on the day its period starts (a week's Sunday, a longer period's 1st), or
    the whole-window rule would place it in another period.

    The indicators' values are computed over the aggregates range's bars and carry those
    bars' stamps (Massive's samples show the underlying aggregates at 00:00 New York time for
    stocks and 01:00 for indices), so an indicator value of a day or longer is checked by the
    range's rule for its symbols' class: 00:00 of its day, or the indices' midnight in
    Chicago, on its period's first day. A value stamped otherwise fails like a bar.

    Args:
        spec: The dataset.
        request: The normalized arguments.
        asset_class: For a dataset of every class whose rows are bars (the indicators), the
            class of the request's symbols (`dates.day_class`); ignored for every other
            dataset.

    Returns:
        The rule, with no `remedy` yet, for bars and indicator values of a day or longer of
        stocks, options, indices, forex, and crypto; None for intraday bars, futures bars,
        and every dataset whose rows are not bars.
    """
    indicator = not spec.holds_bars and spec.times is RowTimes.BAR
    cls = asset_class if indicator else spec.asset_class
    if (
        not (spec.holds_bars or indicator)
        or request.timespan not in _DAYS_OR_LONGER
        or not isinstance(cls, AssetClass)
        or cls not in DAY_ZONES
    ):
        return None
    zone = DAY_ZONES[cls]
    timespan = request.timespan
    single = timespan is Timespan.DAY and request.multiplier == 1
    if single and not indicator:
        return StampRule(zone, _ONE_DAY_STAMPS[cls], "1-day bar")
    known = _LONGER_STAMPS[cls]
    what = "indicator value" if indicator else "bar"
    if timespan is Timespan.WEEK:
        return StampRule(zone, known, what, weekdays=_SUNDAY)
    return StampRule(zone, known, what, months=_PERIOD_MONTHS.get(timespan, frozenset()))


ISSUES_URL: Final = "https://github.com/chipkent/deephaven-massive/issues"
"""Where a refused bar stamp is reported, so its day cut can be settled."""

_WEEKDAY_NAMES: Final = (
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
)

_MONTH_NAMES: Final = (
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
)


def day_starts(
    label: str,
    values: pa.Array,
    *,
    zone: tzinfo,
    known: Collection[time],
    source: str,
    what: str,
    remedy: str,
    weekdays: Collection[int] = (),
    months: Collection[int] = (),
) -> pa.Array:
    """Stamp each bar at 00:00 of the day it falls on, refusing a stamp of an unknown form.

    Massive stamps the same bar of a day or longer differently by source: at the start of the
    day on the aggregates range, at the session close (16:00 New York time) on the stock
    grouped daily and `/prev`, at the day's last millisecond in the forex day files. Each
    stamp whose wall-clock time of day in `zone`, to the millisecond, is 00:00 or one of
    `known`, on a day `weekdays` and `months` allow, moves to 00:00 of its date there, so
    every source labels a bar alike. Any other stamp raises: the bar may cover other days
    than the one it would be labelled with, so it is never relabelled.

    The zone's offsets come from `zoneinfo`, one conversion per distinct stamp (a batch of
    daily bars holds about one per day), applied to the column with Arrow's `take`; Arrow's
    own time-zone database is not used.

    Args:
        label: The dataset, for the message, such as `"stocks.bars"`.
        values: The bars' `Timestamp` column, an Arrow `timestamp("ns", "UTC")` array.
        zone: The class day zone: New York for stocks, options, and indices; UTC for forex and
            crypto. Its midnights start the bars' days, daylight saving time included.
        known: The other wall-clock times of day, in `zone`, at which Massive is known to
            stamp these bars; empty when only 00:00 is accepted.
        source: Where the bars came from, for the message, such as a REST path template or a
            flat-file prefix.
        what: The kind of bar, for the message, such as `"1-day bar"`.
        remedy: What the caller can do instead, for the message, such as reading the other
            source.
        weekdays: The weekdays (Monday is 0) a stamp's date in `zone` may fall on; empty for
            any day.
        months: The months whose 1st a stamp's date in `zone` must be; empty for any day.

    Returns:
        The stamps at 00:00 of their day in `zone`, in the type of `values`; nulls stay null.

    A stamp at any other time of day or on any other day raises the `MassiveError` of
    `_refuse`, naming the dataset, the source, the stamp, the accepted stamps, and `remedy`.
    """
    ns = values.cast(pa.int64())
    distinct = pc.unique(ns)
    accepted = {0, *(_millisecond_of_day(moment) for moment in known)}
    starts: list[int | None] = []
    refused: list[bool] = []
    for value in distinct.to_pylist():
        if value is None:
            starts.append(None)
            refused.append(False)
            continue
        local = _instant(value).astimezone(zone)
        day = local.date()
        on_day = (not weekdays or day.weekday() in weekdays) and (
            not months or (day.day == 1 and day.month in months)
        )
        refused.append(not on_day or _millisecond_of_day(local.time()) not in accepted)
        starts.append(_ns(datetime.combine(day, time(), zone)))
    index = pc.index_in(ns, value_set=distinct)
    odd = pa.array(refused, pa.bool_()).take(index)
    expected = _expected(zone, known, weekdays=weekdays, months=months)
    _refuse(
        label, values, odd, zone=zone, expected=expected, source=source, what=what, remedy=remedy
    )
    return pa.array(starts, pa.int64()).take(index).cast(values.type)


def _millisecond_of_day(moment: time) -> int:
    """Return a wall-clock time of day as whole milliseconds since midnight.

    Args:
        moment: The time of day, naive.

    Returns:
        The milliseconds; a fraction of a millisecond is dropped.
    """
    return ((moment.hour * 60 + moment.minute) * 60 + moment.second) * 1000 + (
        moment.microsecond // 1000
    )


def _expected(
    zone: tzinfo, known: Collection[time], *, weekdays: Collection[int], months: Collection[int]
) -> str:
    """Describe the stamps a check accepts, for its message.

    Args:
        zone: The day zone.
        known: The accepted times of day besides 00:00.
        weekdays: The accepted weekdays, or empty for any.
        months: The months whose 1st is accepted, or empty for any day.

    Returns:
        Such as `"00:00:00.000, 16:00:00.000 in America/New_York"` or
        `"00:00:00.000 in UTC, on a Sunday"`.

    """
    times = ", ".join(t.isoformat("milliseconds") for t in sorted({time(0), *known}))
    text = f"{times} in {zone}"
    if weekdays:
        text += ", on a " + " or ".join(_WEEKDAY_NAMES[day] for day in sorted(weekdays))
    if months:
        every = len(months) == len(_MONTH_NAMES)
        named = "a month" if every else " or ".join(_MONTH_NAMES[m - 1] for m in sorted(months))
        text += f", on the 1st of {named}"
    return text


def _refuse(
    label: str,
    values: pa.Array,
    odd: pa.Array,
    *,
    zone: tzinfo,
    expected: str,
    source: str,
    what: str,
    remedy: str,
) -> None:
    """Raise for the first bar a mask marks as stamped at an unknown time or day, if any.

    Args:
        label: The dataset, for the message.
        values: The stamps, an Arrow `timestamp("ns", "UTC")` array.
        odd: True for each refused stamp; False for an accepted or null one.
        zone: The class day zone.
        expected: The accepted stamps, described (`_expected`).
        source: Where the bars came from, for the message.
        what: The kind of bar, for the message, such as `"1-day bar"`.
        remedy: What the caller can do instead, for the message.

    Raises:
        MassiveError: If `odd` holds a True, naming the first such stamp.
    """
    if not pc.any(odd).as_py():
        return
    bad = _instant(pc.filter(values.cast(pa.int64()), odd)[0].as_py())
    local = "" if zone is UTC else f" ({bad.astimezone(zone).isoformat()} in {zone})"
    article = "an" if what[:1] in "aeiou" else "a"
    raise MassiveError(
        f"{label}: {article} {what} from {source} is stamped {bad.isoformat()}{local}, which is "
        f"not a stamp Massive is known to use for such a bar ({expected}), so it may cover "
        f"other days than the one it would be labelled with; {remedy}, and report the stamp at "
        f"{ISSUES_URL} so the day cut can be settled",
        cause=ErrorCause.LIBRARY_DEFECT,
    )
