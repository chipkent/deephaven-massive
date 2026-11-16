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
fields and, for bars of a day or longer, moved their stamps to the start of their day
(`stamp_rule`). The vendor's own time fields feed these columns through the
dataset's declared keys and are not kept beside them: the bar start (`t`, `s`,
`window_start`) becomes `TimestampStart`, the WebSocket bar end `e` becomes `TimestampEnd`
as sent, and the futures
`session_end_date` and the open-close `from` and `day` become `Date`.

**The windows.**

- **Tick.** `Date` is the day zone's date of `Timestamp`; for futures it is Massive's
  `session_end_date`, or on the WebSocket, which sends none, the 17:00 Chicago rule corrected
  by the schedules (`SessionDates`), null for a row they cannot place.
- **Intraday bar of n units.** `TimestampStart` is Massive's start and `TimestampEnd` Massive's
  `e` as sent where the WebSocket sends one, else the start plus n units, Massive's documented
  length (REST and flat-file bars carry no end).
  `Date` is the start's date, null when the bar ends on a later date; for futures it is the
  session as for ticks, null when the bar crosses 17:00 Chicago time.
- **1-day bar.** 00:00 of the day to 00:00 of the next day (01:00 New York time for indices);
  `Date` is the day.
- **n-day, week, month, quarter, or year bar.** 00:00 of the window's first day (for stocks
  and options a period's first weekday, a week's Monday; for every other class its first
  calendar day, a week's Sunday) to 00:00 after its last day (last weekday for stocks and
  options; for indices, which can update around the clock, the period's last calendar day and
  one hour more); a bar of several days spans that many calendar days, weekends included, as
  Massive counts them; `Date` is null.
- **Futures bar of a session or longer.** Massive's `window_start` (00:00 UTC on the first
  day of the bar's bucket of UTC days, `bar_periods`) to 17:00 Chicago time on its label,
  Massive's `session_end_date`, the bucket's last session. `Date` is the label for a
  1-session bar and null for a longer one. Since a session opens the evening before its date,
  consecutive bars overlap by about 22 hours.
- **Daily open-close.** As a 1-day bar of its date. A crypto summary whose `isUTC` is false
  keeps Massive's `Date` and `IsUtc`, but its day is not the UTC day and the library cannot
  tell which zone's day it is, so its window is the widest that date's day has in any time
  zone: from 00:00 at UTC+14 to 24:00 at UTC-12. It logs one WARNING per dataset per session.
- **A period stamped where Massive is not known to stamp one.** A bar or indicator value of a
  day or longer whose stamp `StampRule` does not expect is withheld ("Bar stamps" below).

The indices' extra hour is conservative: an index day bar is stamped at midnight in Chicago,
and whether it covers the New York or the Chicago day is not verified, so its end is the later
of the two. A futures bar longer than one session whose label may not yet be its bucket's last
session has a `TimestampEnd` that can still move, so `history()` returns it only once it has
certainly ended (`within`, `bar_periods.futures_bar_ended`). The next midnight is always the
calendar's next midnight in the zone, never 24 hours later, so a day across a daylight-saving
change keeps its true length.

**Bar stamps.** Bars of a day or longer are stamped at 00:00 of the day they start on, in the
class day zone, whichever source serves them, before their time columns are filled. Massive
stamps the same bar differently by source (the day's start on the aggregates range, the 16:00
New York session close on the stock grouped daily and `/prev`, the day's last millisecond in
the forex and crypto day files), so every known stamp moves to 00:00 of the day it falls on
(`day_starts`). A stamp at a time or on a day Massive is not known to use for the class and
timespan cannot be placed: which day it covers would be a guess, and a wrong guess could end
the window before the data closed, a look-ahead. `StampRule.stamp` returns such rows for the
transports to withhold (`unconvertible_values`), before `TimeRule.fill` sees them, and logs
one WARNING per form of stamp per session that names the symbol and the stamp and asks for it
to be reported. `stamp_rule` decides which stamps each class is known to use.

**Zones.** Day boundaries are computed with `zoneinfo`, once per distinct UTC day of a batch
(and once more on a day that changes its clock), and applied to the whole column with Arrow;
Arrow's own time-zone database is not used, so nothing depends on one being installed.

The module is long but has one subject, a row's time columns: the bar-stamp rule is the first
step of filling a bar's window, and every other rule here fills one of those columns or keeps
rows by them.

The module does no I/O and makes no Deephaven call. Its only state is the once-only log line of
a futures WebSocket bar whose end disagrees with its length; the WARNINGs of an unexpected bar
stamp and of a day summary not in UTC go to the session's once-per-session warnings, which the
transports pass in.
"""

from __future__ import annotations

import calendar
import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Final, Protocol, TypeVar

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.bar_periods import (
    bar_days,
    futures_bar_ended,
    futures_session_unit,
    intraday,
    unit_ns,
)
from deephaven_massive._datasets.dates import (
    DAY_ZONES,
    day_end,
    day_zone,
    midnight,
)
from deephaven_massive._datasets.epoch import DAY_NS, epoch_instant, epoch_ns
from deephaven_massive._datasets.futures_sessions import (
    CHICAGO,
    futures_session_end,
    session_days,
    weekday_sessions,
)
from deephaven_massive._enums import AssetClass, ClassFromPrefix, Timespan
from deephaven_massive._errors import required
from deephaven_massive._log_limits import log_once

if TYPE_CHECKING:
    from collections.abc import Collection, Mapping
    from datetime import tzinfo

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._log_limits import BoundedLog

__all__ = [
    "DATE",
    "END",
    "INSERT",
    "ISSUES_URL",
    "LIBRARY_COLUMNS",
    "START",
    "TIMESTAMP",
    "BarBounds",
    "FuturesBarCompletion",
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

_SECOND_NS: Final = 10**9
_NEVER: Final = 2**62
"""An instant later than any bounded timestamp: a day whose clock never changes."""
_INSTANT: Final = pa.timestamp("ns", tz="UTC")
_UTC_FLAG: Final = "IsUtc"
"""The column in which a daily summary (the crypto open-close) says its times are UTC."""
_ANY_ZONE_DAY: Final = (14 * 3600 * _SECOND_NS, 12 * 3600 * _SECOND_NS)
"""How much earlier than its UTC day, and how much later, a date's day can start and end in
some time zone: clocks run from UTC-12 to UTC+14, so the day starts at most 14 hours before
UTC midnight and ends at most 12 hours after the next."""

_Rows = TypeVar("_Rows", pa.RecordBatch, pa.Table)


class RowTimes(StrEnum):
    """Which of the library's time columns a dataset's rows carry (`DatasetSpec.row_times`).

    Every dataset ends with `TimestampInsert`, whatever its member.
    """

    TICK = "tick"
    """An event at one instant: `Date` and `Timestamp`. The trades, quotes, index values,
    FMV, LULD, imbalances, and options admin messages."""
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
            The dates to keep, ISO text, one per row; null where the schedules cannot place
            the row.
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
    offset = epoch_instant(ns).astimezone(zone).utcoffset()
    assert offset is not None  # an aware datetime always has one; only a naive one has none
    return offset // timedelta(microseconds=1) * 1000


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
    low = day * DAY_NS
    high = low + DAY_NS
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
    days = pc.divide(ns, DAY_NS)
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
    return pc.divide(_local_ns(ns, zone), DAY_NS)


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


class StartRule(StrEnum):
    """How `history()` bounds the start of the bars it keeps (`BarBounds.start_rule`)."""

    RANGE = "range"
    """A bar's `TimestampStart` must be no earlier than the range's start."""
    LABEL = "label"
    """Bars are kept by their session label, not their start: futures intraday and 1-session bars
    named by session dates, whose bar may open before its session's date when a holiday folds
    it."""


@dataclass(frozen=True, slots=True)
class FuturesBarCompletion:
    """When a futures bar of a session or longer has certainly ended.

    Carried by `BarBounds.futures_completion`. A bar longer than one session is labelled with
    the last session Massive has seen in its bucket, so its `TimestampEnd` can still move while
    the bucket is open; `bar_periods.futures_bar_ended` says from when it cannot. Frozen, so
    reader threads may share it.

    Attributes:
        timespan: The bar unit: `SESSION`, `DAY`, `WEEK`, `MONTH`, `QUARTER`, or `YEAR`.
        multiplier: How many units one bar spans, at least 1.
        now: The current instant of the `history()` call: a bar is kept only when it has
            certainly ended by then.
    """

    timespan: Timespan
    multiplier: int
    now: datetime


@dataclass(frozen=True, slots=True)
class BarBounds:
    """The window a `history()` call keeps its bars in, judged by their filled time columns.

    Built by `history_source.history_window` for a dataset of bars; frozen, so the reader
    threads of a call may share it.

    Attributes:
        earliest: The earliest `TimestampStart` a kept bar may have under `StartRule.RANGE`:
            the range's start, or for futures bars of several sessions or longer named by
            dates, the first bucket day that holds no session before the range.
        latest: The latest `TimestampEnd` a kept bar may have: the earlier of now and the
            range's end, which for dates is the last day's end (`dates.day_end`; for futures,
            17:00 Chicago on the last session's date).
        start_rule: Whether `earliest` bounds the bars' starts.
        futures_completion: For futures bars of a session or longer, when such a bar has
            certainly ended; None for every other bar, which has ended once its
            `TimestampEnd` has passed.
    """

    earliest: datetime
    latest: datetime
    start_rule: StartRule
    futures_completion: FuturesBarCompletion | None = None


def within(rows: _Rows, bounds: BarBounds) -> _Rows:
    """Keep the bars whose filled window lies inside a range: the whole-window rule.

    `history()` keeps a bar only when its window has ended by the end of the range and by
    now, judged by the same `TimestampEnd` a join on the end uses, so a bar a join would not
    yet attach is never returned; and, under `StartRule.RANGE`, only when its `TimestampStart`
    is no earlier than `BarBounds.earliest`, so a bar whose start reaches before the range (an
    index week stamped on its weekend) is left out. A futures bar of a session or longer is
    kept only once it has certainly ended (`_futures_ended`). A row whose start or end is
    unknown (a futures bar sent without its label) is kept, the column null.

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
    latest = pa.scalar(epoch_ns(bounds.latest), ends.type)
    keep = pc.fill_null(pc.less_equal(ends, latest), fill_value=True)
    if bounds.start_rule is StartRule.RANGE and START in names:
        starts = rows.column(START)
        late = pc.greater_equal(starts, pa.scalar(epoch_ns(bounds.earliest), starts.type))
        keep = pc.and_(keep, pc.fill_null(late, fill_value=True))
    if bounds.futures_completion is not None and START in names:
        keep = pc.and_(keep, _futures_ended(rows, bounds.futures_completion))
    return rows.filter(keep)


def _futures_ended(rows: _Rows, completion: FuturesBarCompletion) -> pa.Array:
    """Tell which futures bars of a session or longer have certainly ended.

    The judgment reads only the visible columns: a bar's bucket starts on the UTC date of its
    `TimestampStart`, and its label is the Chicago date of its `TimestampEnd` (17:00 Chicago on
    the label). Each distinct pair is judged once (`bar_periods.futures_bar_ended`). The bars
    left out log one DEBUG line per batch with how many there were and the first one's window.

    Args:
        rows: Futures bars with their time columns filled.
        completion: The bars' unit and the call's current instant.

    Returns:
        One boolean per row: True where the bar has ended, or its start or end is unknown.
    """
    starts = _to_int(rows.column(START)).to_pylist()
    ends = _to_int(rows.column(END)).to_pylist()
    now = epoch_ns(completion.now)
    certain: dict[tuple[int, int], int] = {}
    keep: list[bool] = []
    open_bars: list[tuple[int, int]] = []
    for start, end in zip(starts, ends, strict=True):
        if start is None or end is None:
            keep.append(True)
            continue
        if (start, end) not in certain:
            label = epoch_instant(end).astimezone(CHICAGO).date()
            ended = futures_bar_ended(
                epoch_instant(start).date(),
                label,
                timespan=completion.timespan,
                multiplier=completion.multiplier,
            )
            certain[start, end] = epoch_ns(ended)
        keep.append(certain[start, end] <= now)
        if not keep[-1]:
            open_bars.append((start, end))
    if open_bars:
        start, end = open_bars[0]
        logger.debug(
            "%d futures %d-%s bars left out: not certainly ended at %s; the first starts %s, "
            "ends %s, and has certainly ended only at %s",
            len(open_bars),
            completion.multiplier,
            completion.timespan.value,
            completion.now.isoformat(),
            epoch_instant(start).isoformat(),
            epoch_instant(end).isoformat(),
            epoch_instant(certain[start, end]).isoformat(),
        )
    return pa.array(keep, pa.bool_())


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

            import time

            import pyarrow as pa

            from deephaven_massive._datasets.row_times import RowTimes, TimeRule
            from deephaven_massive._enums import AssetClass

            instant = pa.timestamp("ns", tz="UTC")
            trades = pa.table(
                {
                    "Symbol": ["AAPL"],
                    "Date": pa.array([None], pa.string()),
                    "Timestamp": pa.array([1_741_016_400_000_000_000], instant),
                    "TimestampInsert": pa.array([None], instant),
                }
            )
            rule = TimeRule("stocks.trades", RowTimes.TICK, AssetClass.STOCKS)
            filled = rule.fill(trades, received=time.time_ns())
            assert filled.column("Date").to_pylist() == ["2025-03-03"]  # New York's day

    Attributes:
        dataset: The dataset, for messages.
        row_times: Which time columns the rows carry.
        asset_class: The class whose day rules apply (`dates.day_class`): its day zone, and
            for futures its sessions; `ClassFromPrefix.ANY` for New York days.
        timespan: The bar unit of a `BAR` request; `DAY` for a daily summary; None for every
            other dataset.
        multiplier: How many units one bar spans; 1 for everything but bars.
        warnings: The session's once-per-session warnings (`SessionCore.warnings`), which log
            the WARNING of a day summary whose `IsUtc` is false; required for a `DAY` rule and
            None, absent, for every other.
        stamps: For bars and indicator values of a day or longer of a class with a day zone,
            the stamps Massive is known to use (`stamp_rule`), which tell the rows the
            transports kept as sent; None, absent, for every other rule, whose stamps are
            all taken as known.
    """

    dataset: str
    row_times: RowTimes
    asset_class: AssetClass | ClassFromPrefix
    timespan: Timespan | None = None
    multiplier: int = 1
    warnings: BoundedLog | None = None
    stamps: StampRule | None = None

    def __post_init__(self) -> None:
        """Reject a bar rule without a timespan and a daily rule without its warnings.

        Raises:
            ValueError: If `row_times` is `BAR` and `timespan` is None: a bar's window needs
                its unit; or if `row_times` is `DAY` and `warnings` is None: a day summary
                not in UTC is reported there.
        """
        if self.row_times is RowTimes.BAR and self.timespan is None:
            raise ValueError(f"{self.dataset}: a bar rule needs its timespan")
        if self.row_times is RowTimes.DAY and self.warnings is None:
            raise ValueError(f"{self.dataset}: a daily rule needs the session's warnings")

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
        (for bars of a day or longer, a known stamp already moved to 00:00 of its day by
        `StampRule.stamp`, any other kept as sent), its `Date` the futures session or
        open-close date where the source sends one (else null), and its `TimestampEnd`
        Massive's WebSocket bar end where the source sends one (else null). This computes the
        rest by the table of the module docstring and sets every row's `TimestampInsert` to
        `received`. A daily summary whose `IsUtc` is false (the crypto open-close) gets the
        widest window its date has in any time zone and logs one WARNING per dataset per
        session (`_daily`).

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

        A row whose `IsUtc` (the crypto open-close's) is false keeps its `Date`, but Massive
        cut its day elsewhere than at UTC midnight and does not say where, so its window is
        the widest that date's day has in any time zone (`_ANY_ZONE_DAY`): a join on its
        `TimestampEnd` never attaches it before its day ended, wherever that day was cut. It
        logs one WARNING per dataset per session through `warnings`.
        """
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
            starts.append(epoch_ns(midnight(day, zone)))
            ends.append(epoch_ns(day_end(self.asset_class, day)))
        index = pc.index_in(labels, value_set=distinct)
        start = _instants(starts).take(index)
        end = _instants(ends).take(index)
        if _UTC_FLAG in rows.schema.names:
            start, end = self._any_zone_days(rows, labels=labels, start=start, end=end)
        rows = _set(rows, START, start)
        return _set(rows, END, end)

    def _any_zone_days(
        self, rows: _Rows, *, labels: pa.Array, start: pa.Array, end: pa.Array
    ) -> tuple[pa.Array, pa.Array]:
        """Widen the window of each day summary not in UTC to its date's day in any zone.

        Args:
            rows: The batch, which has `IsUtc`.
            labels: The rows' `Date`, ISO text.
            start: The rows' UTC-day starts, epoch nanoseconds as Instants.
            end: The rows' UTC-day ends, likewise.

        Returns:
            The starts and ends, those of each row whose `IsUtc` is false moved 14 hours
            earlier and 12 hours later; a null `IsUtc` is taken as UTC, Massive's documented
            form.
        """
        flags = _column(rows, _UTC_FLAG)
        other = pc.invert(pc.fill_null(flags, fill_value=True))
        if not pc.any(other).as_py():
            return start, end
        first = pc.index(other, True).as_py()
        earliest, latest = _ANY_ZONE_DAY
        wide_start = pc.subtract(_to_int(start), earliest)
        wide_end = pc.add(_to_int(end), latest)
        logger.debug(
            "%s: %d of %d day summaries with isUTC false get their date's widest window in any "
            "time zone; the first, of %s for %s",
            self.dataset,
            pc.sum(other).as_py(),
            len(other),
            _column(rows, "Symbol")[first].as_py(),
            labels[first].as_py(),
        )
        required(self.warnings, f"{self.dataset}'s daily rule's warnings").log(
            f"day summary not in UTC {self.dataset}",
            "%s: Massive sent the day summary of %s for %s with isUTC false, so its day is not "
            "the UTC day the other crypto tables use, and Massive does not say which zone's day "
            "it is. The row keeps Massive's Date and IsUtc, and gets the widest window that "
            "date's day has in any time zone, TimestampStart %s to TimestampEnd %s (00:00 at "
            "UTC+14 to 24:00 at UTC-12), so a join on TimestampEnd never attaches it before its "
            "day ended; please report it at %s (logged once per dataset per session)",
            self.dataset,
            _column(rows, "Symbol")[first].as_py(),
            labels[first].as_py(),
            epoch_instant(wide_start[first].as_py()).isoformat(),
            epoch_instant(wide_end[first].as_py()).isoformat(),
            ISSUES_URL,
        )
        return (
            pc.if_else(other, pc.cast(wide_start, _INSTANT), start),
            pc.if_else(other, pc.cast(wide_end, _INSTANT), end),
        )

    def _bar(self, rows: _Rows, session_dates: SessionDates | None) -> _Rows:
        """Fill a bar's window and `Date` by its timespan, multiplier, and class.

        Args:
            rows: The batch.
            session_dates: The futures WebSocket corrector, or None.

        Returns:
            The batch with `Date`, `TimestampStart`, and `TimestampEnd`.
        """
        timespan = required(self.timespan, f"{self.dataset}'s bar timespan")
        if intraday(timespan):
            return self._intraday(rows, session_dates)
        if self._futures:
            return self._futures_long(rows, timespan)
        return self._long_bars(rows, timespan)

    def _intraday(self, rows: _Rows, session_dates: SessionDates | None) -> _Rows:
        """Fill an intraday bar: its end is Massive's `e`, else its start plus its length.

        Args:
            rows: The batch.
            session_dates: The futures WebSocket corrector, or None.

        Returns:
            The batch with `Date` and `TimestampEnd`.
        """
        timespan = required(self.timespan, f"{self.dataset}'s bar timespan")
        start = _to_int(rows.column(START))
        end = self._sent_end(rows, pc.add(start, unit_ns(timespan) * self.multiplier))
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

    def _sent_end(self, rows: _Rows, computed: pa.Array) -> pa.Array:
        """Return an intraday bar's end: Massive's `e`, or its start plus its length.

        The library supplies Massive's data as sent, so `TimestampEnd` keeps `e` even when it
        is not the start plus the bar's length (Massive's documented futures one-second sample
        spans 60 seconds from `s` to `e`). Such a disagreement is logged once per dataset per
        process, at WARNING, with both ends, so whoever reads the table learns that a window is
        not the bar's length. A row without `e` (every REST and flat-file bar, and a WebSocket
        bar that omits it) ends at its start plus its length.

        Args:
            rows: The batch, whose `TimestampEnd` holds Massive's `e` where the source sends it.
            computed: The start plus the bar's length, epoch nanoseconds.

        Returns:
            The ends, epoch nanoseconds.
        """
        sent = _to_int(rows.column(END))
        differs = pc.fill_null(pc.not_equal(sent, computed), fill_value=False)
        if pc.any(differs).as_py():
            first = pc.filter(sent, differs)[0].as_py()
            length = pc.filter(computed, differs)[0].as_py()
            log_once(
                logger,
                f"bar-end:{self.dataset}",
                "%s: a bar's end from Massive (%d ns) is not its start plus its length (%d ns); "
                "TimestampEnd keeps Massive's e, so the bar's window is not its length",
                self.dataset,
                first,
                length,
                level=logging.WARNING,
            )
        return pc.coalesce(sent, computed)

    def _long_bars(self, rows: _Rows, timespan: Timespan) -> _Rows:
        """Fill a bar of a day or longer of a class with a day zone.

        Its start is moved to 00:00 of its window's first day and its end is 00:00 after its
        window's last day (`bar_periods.bar_days`), plus an hour for indices; a 1-day bar's
        `Date` is its day, and every longer bar's is null.

        Args:
            rows: The batch, whose known starts `StampRule.stamp` already moved to 00:00 of
                their day.
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
            day = epoch_instant(value).astimezone(zone).date()
            first, after = bar_days(
                self.asset_class, day, timespan=timespan, multiplier=self.multiplier
            )
            starts.append(epoch_ns(midnight(first, zone)))
            ends.append(epoch_ns(day_end(self.asset_class, after - timedelta(days=1))))
            dates.append(day.isoformat() if single else None)
        index = pc.index_in(ns, value_set=distinct)
        rows = _set(rows, START, _instants(starts).take(index))
        rows = _set(rows, END, _instants(ends).take(index))
        return _set(rows, DATE, pa.array(dates, pa.string()).take(index))

    def _futures_long(self, rows: _Rows, timespan: Timespan) -> _Rows:
        """Fill a futures bar of a session or longer from its session label.

        Its `TimestampStart` stays Massive's `window_start`, and it ends at 17:00 Chicago time
        on its label, Massive's `session_end_date`, which is the bar's last session. Only a
        1-session bar keeps its `Date`. Whether a longer bar has certainly ended is judged
        apart (`within`), since its label can still move while its bucket is open.

        Args:
            rows: The batch, whose `Date` holds the bars' `session_end_date`.
            timespan: `SESSION`, `DAY`, `WEEK`, `MONTH`, `QUARTER`, or `YEAR`.

        Returns:
            The batch with `Date` and `TimestampEnd`; a row without a label has no end.
        """
        single = futures_session_unit(timespan) and self.multiplier == 1
        labels = _column(rows, DATE)
        distinct = pc.unique(labels)
        ends = [
            None if text is None else epoch_ns(futures_session_end(date.fromisoformat(text)))
            for text in distinct.to_pylist()
        ]
        index = pc.index_in(labels, value_set=distinct)
        rows = _set(rows, END, _instants(ends).take(index))
        if single:
            return rows
        return _set(rows, DATE, pa.nulls(rows.num_rows, pa.string()))


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
    spec: DatasetSpec,
    request: Request,
    *,
    asset_class: AssetClass | ClassFromPrefix,
    warnings: BoundedLog,
) -> TimeRule:
    """Return how the time columns of a request's rows are computed.

    Args:
        spec: The dataset.
        request: The normalized arguments: for bars and indicators, the timespan and the
            multiplier.
        asset_class: The class whose day rules the request follows (`dates.day_class`): the
            dataset's own, or for a dataset of every class (the indicators) its symbols'.
        warnings: The session's once-per-session warnings, which a daily summary's rule logs
            to (`TimeRule._daily`), and which its bars' stamp rule holds.

    Returns:
        The rule: of the request's timespan and multiplier for bars and indicators, with
        their stamp rule (`stamp_rule`, None where stamps are kept as sent), of one day for a
        daily summary, and of neither for every other dataset.
    """
    times = spec.times
    if times is RowTimes.BAR:
        return TimeRule(
            spec.name,
            times,
            asset_class,
            request.timespan,
            request.multiplier,
            stamps=stamp_rule(spec, request, warnings=warnings, asset_class=asset_class),
        )
    if times is RowTimes.DAY:
        return TimeRule(spec.name, times, asset_class, Timespan.DAY, warnings=warnings)
    return TimeRule(spec.name, times, asset_class)


@dataclass(frozen=True, slots=True)
class StampRule:
    """How the transports stamp the bars of a request of a day or longer (`stamp_rule`).

    Made per request on the calling thread and only read afterwards, so reader threads may
    share it; `warnings` is thread-safe.

    Attributes:
        zone: The class day zone; every bar is stamped at 00:00 there of the day it starts on.
        known: The other wall-clock times of day, in `zone`, at which Massive is known to stamp
            the class's bars of this length on some source.
        what: The kind of row, for the WARNING: `"1-day bar"`, `"bar"`, or `"indicator value"`.
        timespan: The request's bar unit, a day or longer, for the WARNING's period.
        multiplier: How many units one bar spans, for the WARNING's period.
        warnings: The session's once-per-session warnings (`SessionCore.warnings`), which log
            the WARNING of an unexpected stamp.
        weekdays: For week bars, the weekdays (Monday is 0) Massive is known to stamp a bar on
            in `zone`; empty for every other timespan.
        months: For month, quarter, and year bars, the months whose 1st Massive is known to
            stamp a bar on in `zone`; empty for every other timespan.
    """

    zone: tzinfo
    known: frozenset[time]
    what: str
    timespan: Timespan
    multiplier: int
    warnings: BoundedLog
    weekdays: frozenset[int] = frozenset()
    months: frozenset[int] = frozenset()

    def stamp(self, label: str, rows: _Rows, *, source: str) -> tuple[_Rows, pa.Array]:
        """Stamp a batch of bars at 00:00 of their days (`day_starts`), and find the odd stamps.

        A stamp at another time of day than 00:00 or one of `known`, or on a day `weekdays`
        or `months` rules out, cannot be placed: its row is returned for the caller to
        withhold, and logs one WARNING per form of stamp per session through `warnings`
        (`_warn`). The WARNING names the dataset, the source, the symbol and the stamp, and
        the stamps expected; says the row is withheld; and asks for the stamp to be reported
        at `ISSUES_URL`. Each batch with such a stamp logs one DEBUG line with how many rows
        had one.

        Args:
            label: The dataset, for the WARNING, such as `"stocks.bars"`.
            rows: The bars, a batch or table of the dataset's schema whose `TimestampStart`
                holds Massive's stamps.
            source: Where the bars came from, for the WARNING: a REST path template or a
                flat-file prefix.

        Returns:
            The rows, their known stamps at the start of their days, nulls and odd stamps as
            they were; and the mask of the rows with an odd stamp.
        """
        values = _column(rows, START)
        starts, odd = day_starts(
            values, zone=self.zone, known=self.known, weekdays=self.weekdays, months=self.months
        )
        if pc.any(odd).as_py():
            ns = _to_int(values)
            symbols = _column(rows, "Symbol")
            first = pc.index(odd, True).as_py()
            logger.debug(
                "%s: %d of %d rows from %s withheld for Massive's stamp, which it is not known to "
                "use for a %d-%s bar; the first, of %s, at %s",
                label,
                pc.sum(odd).as_py(),
                len(odd),
                source,
                self.multiplier,
                self.timespan.value,
                symbols[first].as_py(),
                epoch_instant(ns[first].as_py()).isoformat(),
            )
            for value in pc.unique(pc.filter(ns, odd)).to_pylist():
                row = pc.index(ns, value).as_py()
                self._warn(label, epoch_instant(value), symbols[row].as_py(), source=source)
        return _set(rows, START, starts), odd

    def reason(self, stamp: int) -> str:
        """Say why a bar with a stamp Massive is not known to use is withheld.

        Args:
            stamp: The stamp, epoch nanoseconds.

        Returns:
            The stamp and the bar it is not known for.
        """
        return (
            f"stamped at {epoch_instant(stamp).isoformat()}, which Massive is not known to use "
            f"for a {self.multiplier}-{self.timespan.value} bar"
        )

    def _warn(self, label: str, stamp: datetime, symbol: str | None, *, source: str) -> None:
        """Log the WARNING of one stamp that cannot be placed, once per form per session.

        A form is the dataset, the zone, the bar length, and the stamp's wall-clock time of day
        in `zone`, with its weekday for week bars and its day of the month for month, quarter,
        and year bars: each distinct odd stamp warns once, and a form Massive repeats every day
        does not warn again.

        Args:
            label: The dataset, such as `"stocks.bars"`.
            stamp: The stamp, an aware UTC `datetime`.
            symbol: The symbol of the first row with that stamp, or None when it is unreadable.
            source: Where the bars came from: a REST path template or a flat-file prefix.
        """
        zone = self.zone
        local_stamp = stamp.astimezone(zone)
        form = local_stamp.time().isoformat("milliseconds")
        if self.weekdays:
            form += f" {_WEEKDAY_NAMES[local_stamp.weekday()]}"
        if self.months:
            form += f" day {local_stamp.day}"
        local = "" if zone is UTC else f" ({local_stamp.isoformat()} in {zone})"
        article = "an" if self.what[:1] in "aeiou" else "a"
        unit = self.timespan.value if self.multiplier == 1 else f"{self.timespan.value}s"
        self.warnings.log(
            f"unexpected bar stamp {label} {zone} {self.multiplier} {self.timespan.value} {form}",
            "%s: %s %s of %s from %s is stamped %s%s, which is not a stamp Massive is known to "
            "use for such a bar (%s). The library cannot tell which day it covers, so the row is "
            "withheld rather than published at a guessed day (bars of %d %s). Please report the "
            "stamp at %s so the day cut can be settled (logged once per dataset, bar length, and "
            "time of day of the stamp per session)",
            label,
            article,
            self.what,
            symbol,
            source,
            stamp.isoformat(),
            local,
            _expected(zone, self.known, weekdays=self.weekdays, months=self.months),
            self.multiplier,
            unit,
            ISSUES_URL,
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

_SUNDAY: Final = frozenset({calendar.SUNDAY})
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
    warnings: BoundedLog,
    asset_class: AssetClass | ClassFromPrefix = ClassFromPrefix.ANY,
) -> StampRule | None:
    """Return how the transports stamp a request's bars, or None to keep the stamps as sent.

    One `history()` call returns the same rows whichever source answers it, so every bar of a
    day or longer of a class with a day zone (`dates.DAY_ZONES`: every class but futures) is
    stamped at 00:00 of the day it starts on in that zone (New York for stocks, options, and
    indices; UTC for forex and crypto). Massive stamps a 1-day bar at the day's start on the
    aggregates range but at the session close (stocks, options, indices) or the day's last
    millisecond (forex, crypto) on other sources, and the indices range and day files at
    midnight in Chicago; those stamps move to the day's start. Massive is known to stamp a
    week, month, quarter, or year bar on the day its period starts (a week's Sunday, a longer
    period's 1st). Every other stamp is served too, kept as Massive sent it, and logs one
    WARNING per form of stamp per session (`StampRule.stamp`): such a bar may cover other days than
    the one its stamp falls on (the documented crypto REST samples sit at midnight New York
    time, another day cut), so its row ends at the stamp plus its period and has no `Date`
    (`TimeRule`).

    The indicators' values are computed over the aggregates range's bars and carry those
    bars' stamps (Massive's samples show the underlying aggregates at 00:00 New York time for
    stocks and 01:00 for indices), so an indicator value of a day or longer is checked by the
    range's rule for its symbols' class: 00:00 of its day, or the indices' midnight in
    Chicago, on its period's first day. A value stamped otherwise is served and warns like a
    bar.

    Args:
        spec: The dataset.
        request: The normalized arguments.
        warnings: The session's once-per-session warnings (`SessionCore.warnings`), which log
            the WARNING of an unexpected stamp.
        asset_class: For a dataset of every class whose rows are bars (the indicators), the
            class of the request's symbols (`dates.day_class`); ignored for every other
            dataset.

    Returns:
        The rule, for bars and indicator values of a day or longer of stocks, options,
        indices, forex, and crypto; None for intraday bars, futures bars, and every dataset
        whose rows are not bars.
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
        return StampRule(zone, _ONE_DAY_STAMPS[cls], "1-day bar", timespan, 1, warnings)
    known = _LONGER_STAMPS[cls]
    what = "indicator value" if indicator else "bar"
    if timespan is Timespan.WEEK:
        return StampRule(
            zone, known, what, timespan, request.multiplier, warnings, weekdays=_SUNDAY
        )
    # Only a day bar of several days has no months to land on; any other unit must be listed.
    months = frozenset() if timespan is Timespan.DAY else _PERIOD_MONTHS[timespan]
    return StampRule(zone, known, what, timespan, request.multiplier, warnings, months=months)


ISSUES_URL: Final = "https://github.com/chipkent/deephaven-massive/issues"
"""Where an unexpected bar stamp is reported, so its day cut can be settled."""

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
    values: pa.Array,
    *,
    zone: tzinfo,
    known: Collection[time],
    weekdays: Collection[int] = (),
    months: Collection[int] = (),
) -> tuple[pa.Array, pa.Array]:
    """Stamp each bar at 00:00 of the day it falls on, and find the stamps of unexpected form.

    Massive stamps the same bar of a day or longer differently by source: at the start of the
    day on the aggregates range, at the session close (16:00 New York time) on the stock
    grouped daily and `/prev`, at the day's last millisecond in the forex day files. Every
    expected stamp moves to 00:00 of its date in `zone`, so every source labels a bar alike. A
    stamp is expected when its wall-clock time of day in `zone`, to the millisecond, is 00:00
    or one of `known`, on a day `weekdays` and `months` allow (`_expected_stamp`); any other
    is kept as sent, since which day it covers is not known, and marked so that the caller
    can warn.

    The zone's offsets come from `zoneinfo`, one conversion per distinct stamp (a batch of
    daily bars holds about one per day), applied to the column with Arrow's `take`; Arrow's
    own time-zone database is not used.

    Args:
        values: The bars' `Timestamp` column, an Arrow `timestamp("ns", "UTC")` array.
        zone: The class day zone: New York for stocks, options, and indices; UTC for forex and
            crypto. Its midnights start the bars' days, daylight saving time included.
        known: The other wall-clock times of day, in `zone`, at which Massive is known to
            stamp these bars; empty when only 00:00 is expected.
        weekdays: The weekdays (Monday is 0) an expected stamp's date in `zone` falls on;
            empty for any day.
        months: The months whose 1st an expected stamp's date in `zone` is; empty for any day.

    Returns:
        The stamps, in the type of `values`: each expected one at 00:00 of its day in `zone`,
        each other as sent, nulls staying null; and one boolean per stamp, True where it is
        unexpected (False for a null).
    """
    ns = values.cast(pa.int64())
    distinct = pc.unique(ns)
    accepted = {0, *(_millisecond_of_day(moment) for moment in known)}
    starts: list[int | None] = []
    unexpected: list[bool] = []
    for value in distinct.to_pylist():
        if value is None:
            starts.append(None)
            unexpected.append(False)
            continue
        local = epoch_instant(value).astimezone(zone)
        expected = _expected_stamp(local, accepted, weekdays=weekdays, months=months)
        unexpected.append(not expected)
        starts.append(epoch_ns(midnight(local.date(), zone)) if expected else value)
    index = pc.index_in(ns, value_set=distinct)
    odd = pa.array(unexpected, pa.bool_()).take(index)
    return pa.array(starts, pa.int64()).take(index).cast(values.type), odd


def _expected_stamp(
    local: datetime,
    accepted: Collection[int],
    *,
    weekdays: Collection[int],
    months: Collection[int],
) -> bool:
    """Tell whether a stamp has a form Massive is known to use for a bar.

    Args:
        local: The stamp, as wall-clock time in the class day zone.
        accepted: The expected times of day, in milliseconds since midnight
            (`_millisecond_of_day`).
        weekdays: The weekdays (Monday is 0) an expected stamp falls on; empty for any day.
        months: The months whose 1st an expected stamp falls on; empty for any day.

    Returns:
        True when the stamp's time of day is accepted and its day allowed.
    """
    day = local.date()
    on_day = (not weekdays or day.weekday() in weekdays) and (
        not months or (day.day == 1 and day.month in months)
    )
    return on_day and _millisecond_of_day(local.time()) in accepted


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
    """Describe the stamps Massive is known to use for a bar, for the WARNING of another.

    Args:
        zone: The day zone.
        known: The expected times of day besides 00:00.
        weekdays: The expected weekdays, or empty for any.
        months: The months whose 1st is expected, or empty for any day.

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
