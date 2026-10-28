"""Where `history()` reads from: the window it keeps, its partition dates, and the source.

`HistoryQuery.history()` asks this module what a call reads and which transport serves it,
then calls that transport; the transports themselves only do I/O. The module is pure (the
current time comes from `_now`, which tests replace) and makes no network or Deephaven call.

- `history_window` turns a call's bounds into a `HistoryWindow`: the bounds by kind (whole
  days or sessions, or exact instants; `dates.bounds`), the rows to keep (on `Timestamp`, or a
  bar's `TimestampStart`, and for futures named by date on their session, `Date`), and the
  instants a REST request covers. Bars are kept only when their whole window lies inside the
  range and has ended (`dates.bar_starts`, `dates.bar_sessions`).
- `partition_dates` turns a window into the dates of the flat files it needs, in the asset
  class's day zone; a futures date names a trading session.
- `rest_history_problem` says why REST cannot serve a request, if it cannot: the dataset has
  no REST history endpoint, or the symbols are `dm.ALL` and the request is not daily bars with
  a grouped-daily endpoint.
- `choose_source` decides between flat files and REST for the whole range. Flat files can serve
  a request when S3 credentials are configured, the dataset has a file for the request (for
  bars only 1-minute and 1-day bars, and 1-session bars for futures, and only unadjusted where
  the dataset has an `adjusted` parameter), and every file the range needs is already
  published. When both sources can, many symbols (`MassiveConfig.flat_file_symbol_threshold`
  or more, or `dm.ALL`) read flat files and fewer use REST. One call never mixes sources, and
  so never mixes adjusted and unadjusted prices: adjusted bars come only from REST. Bars of a
  day or longer are stamped alike by every source (`stamp_rule`), and a stamp of a form
  Massive is not known to use fails loudly, so the choice never changes which day a bar is
  labelled with.

A day's file counts as published from `MassiveConfig.flat_file_publish_cutoff` (New York time)
after the next day starts in the class's day zone (`dates.publish_time`); futures use their
Chicago session date. Massive says files land "by approximately 11:00 AM ET the following day".
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING, Final, NamedTuple

from deephaven_massive._datasets.catalog import filter_key, param_family
from deephaven_massive._datasets.dates import (
    CHICAGO,
    DAY_ZONES,
    NEW_YORK,
    DaySpan,
    TimeRange,
    bar_sessions,
    bar_starts,
    bounds,
    day_class,
    day_zone,
    flat_file_published,
    futures_bar_request,
    futures_session,
    futures_session_before,
    intraday,
    publish_time,
    trades_at_weekends,
)
from deephaven_massive._datasets.timestamps import day_starts
from deephaven_massive._enums import AssetClass, HistorySource, Timespan
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from datetime import tzinfo

    import pyarrow as pa

    from deephaven_massive._config import MassiveConfig
    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._types import DateLike

__all__ = [
    "NOT_PUBLISHED",
    "PUBLICATION_GRACE",
    "HistoryWindow",
    "SourceChoice",
    "StampRule",
    "choose_source",
    "flat_file_prefix",
    "history_window",
    "partition_dates",
    "published",
    "rest_history_problem",
    "stamp_rule",
    "within_publication_grace",
]

logger = logging.getLogger(__name__)

NOT_PUBLISHED: Final = "not yet published; flat files land ~11:00 ET next day"
"""The wording for a flat file that cannot be expected to exist yet."""

PUBLICATION_GRACE: Final = timedelta(days=1)
"""How long after its publication time a missing file may still just be late."""

_ONE_DAY: Final = timedelta(days=1)
_SESSION_DATE: Final = "session_end_date"
"""The futures parameter that names a session; `history()` sets it for sessions named by date."""

_PARTITION_ZONES: Final[Mapping[AssetClass | None, tzinfo]] = dict(
    [*DAY_ZONES.items(), (AssetClass.FUTURES, CHICAGO), (None, NEW_YORK)]
)
"""The zone whose calendar dates name a class's files and start its publication clock.

Futures files are named by trading session, and Massive standardizes futures data in Central
Time. A dataset without an asset class uses New York, as `dates.bounds` does."""


def _now() -> datetime:
    """Return the current instant: the one clock of `history()`, which tests replace.

    Returns:
        An aware UTC `datetime`.
    """
    return datetime.now(UTC)


@dataclass(frozen=True, slots=True)
class HistoryWindow:
    """What one `history()` call reads and keeps; built by `history_window`.

    Made once per call on the caller thread and handed to the transport that serves it;
    frozen, so the transport's reader threads may share it.

    Attributes:
        span: The bounds by kind: the whole days (for futures, sessions) of two dates, or the
            exact instants of two datetimes.
        times: The range a row's `Timestamp` must lie in (for bars, the range of the
            `TimestampStart`s whose whole window fits), or None where only `sessions` bounds
            the rows.
        sessions: The futures session dates (`Date`, Massive's `session_end_date`) a row must
            have, for futures named by date; else None.
        requested: The instants a REST request covers: a time axis's range, or the
            aggregates' `{from}` (where the first fitting bar's period begins) and `{to}`;
            None for futures trades and quotes named by date, which are requested one
            session at a time.
    """

    span: TimeRange | DaySpan
    times: TimeRange | None
    sessions: DaySpan | None
    requested: TimeRange | None

    @property
    def empty(self) -> bool:
        """True when no row can be kept, so no request needs to be made."""
        return (self.times is not None and self.times.empty) or (
            self.sessions is not None and self.sessions.empty
        )


def history_window(
    spec: DatasetSpec,
    request: Request,
    *,
    start: DateLike,
    end: DateLike | None,
    classes: Iterable[AssetClass] = (),
    now: datetime | None = None,
) -> HistoryWindow:
    """Return what a `history()` call reads and keeps.

    The bounds follow `dates.bounds`: two dates are whole days (for futures, sessions), two
    datetimes a half-open range of instants, and an `end` of None reads through now. Ticks are
    kept inside the range. A bar is kept only when its whole window lies inside the range and
    has ended by now (`dates.bar_starts`); for futures named by date, a bar of a session or
    longer is kept by its session label, `Date` (`dates.bar_sessions`, over the sessions
    finished by now, under both readings of the label of a bar of several units), and an
    intraday bar by its session and its end.

    Args:
        spec: The dataset.
        request: The normalized arguments; for bars, the timespan and multiplier.
        start: The first day or instant.
        end: The last day, the first instant excluded, or None for now.
        classes: The classes of the request's symbols, for `dates.day_class`.
        now: The current instant; None reads the clock.

    Returns:
        The window.

    An unparseable or naive bound, a date mixed with a datetime, or a reversed range raises
    the `ValueError` of `dates.bounds`. Futures trades and quotes named by session dates raise
    the `ValueError` of `_futures_sessions` for a `session_end_date` filter.
    """
    moment = _now() if now is None else now
    asset_class = day_class(spec, classes)
    span = bounds(spec, start, end, now=moment, classes=classes)
    futures = asset_class is AssetClass.FUTURES
    if isinstance(span, DaySpan) and futures:
        return _futures_sessions(spec, request, span, now=moment)
    instants = span if isinstance(span, TimeRange) else span.instants(day_zone(asset_class))
    timespan = request.timespan
    if not spec.is_bars or timespan is None:
        return HistoryWindow(span, instants, None, instants)
    ended = TimeRange(instants.start, min(instants.end, moment))
    keep = bar_starts(asset_class, ended, timespan=timespan, multiplier=request.multiplier)
    # Futures bars named by instants take `window_start` exactly. The aggregates' `{from}`
    # starts where the first whole period's days begin, so the bar of that period is returned
    # whether Massive selects bars by their stamp or builds them from `{from}`.
    requested = instants if futures else TimeRange(keep.start, instants.end)
    return HistoryWindow(span, keep, None, requested)


def _futures_sessions(
    spec: DatasetSpec, request: Request, span: DaySpan, *, now: datetime
) -> HistoryWindow:
    """Return the window of futures named by session dates.

    Args:
        spec: The dataset, with the futures class.
        request: The normalized arguments.
        span: The sessions.
        now: The current instant.

    Returns:
        Trades and quotes: every row of the sessions, requested one session at a time. Bars:
        the bars requested from `dates.futures_bar_request`, kept by the session rules of
        `history_window`.

    Raises:
        ValueError: For trades or quotes on a query that also filters `session_end_date` (in
            any of its forms), which the dates set themselves; the message says to drop the
            filter or pass datetimes.
    """
    timespan = request.timespan
    if not spec.is_bars or timespan is None:
        reserved = {filter_key(name) for name in param_family(_SESSION_DATE)}
        if any(key in reserved for key, _ in request.filters):
            raise ValueError(
                f"{spec.name}: history() dates set session_end_date themselves; drop the "
                "session_end_date filter, or pass datetimes to combine it with a time range"
            )
        return HistoryWindow(span, None, span, None)
    requested = futures_bar_request(span)
    if intraday(timespan):
        ended = TimeRange(requested.start, min(requested.end, now))
        keep = bar_starts(
            AssetClass.FUTURES, ended, timespan=timespan, multiplier=request.multiplier
        )
        return HistoryWindow(span, keep, span, requested)
    finished = DaySpan(span.first, min(span.last, futures_session(now) - _ONE_DAY))
    sessions = bar_sessions(finished, timespan=timespan, multiplier=request.multiplier)
    return HistoryWindow(span, None, sessions, requested)


def partition_dates(spec: DatasetSpec, window: HistoryWindow) -> tuple[date, ...]:
    """Return the dates of the flat files a `history()` window needs, oldest first.

    For futures named by date they are the sessions kept (`HistoryWindow.sessions`). For
    futures named by instants they are the sessions of the first and last kept instant and
    every one between; a session opens at 17:00 Chicago time on the day before its
    `session_end_date` (`dates.futures_session`). A holiday's trading lies in the next
    session's file, which the flat-file transport reads when the last sessions' files are
    missing. For every other class they are the dates of the kept range's first and last
    instant in the class's day zone (New York for stocks, options, and indices, UTC for forex
    and crypto) and every date between.

    Args:
        spec: The dataset; its asset class picks the day zone.
        window: The call's window (`history_window`).

    Returns:
        The partition dates; none for an empty window.
    """
    if window.sessions is not None:
        return window.sessions.days()
    times = window.times
    if times is None or times.empty:
        return ()
    if spec.asset_class is AssetClass.FUTURES:
        first, last = futures_session(times.start), futures_session_before(times.end)
        return DaySpan(first, last).days()
    return times.partition_dates(_PARTITION_ZONES[spec.asset_class])


def published(spec: DatasetSpec, day: date, *, cutoff: time, now: datetime | None = None) -> bool:
    """Tell whether a day's flat file of a dataset counts as published (`dates.publish_time`).

    Args:
        spec: The dataset; its asset class picks the day zone (Chicago sessions for futures).
        day: The file's partition date.
        cutoff: `MassiveConfig.flat_file_publish_cutoff`, a New York wall-clock time.
        now: The current instant; None reads the clock.

    Returns:
        True from the file's publication time on.
    """
    moment = _now() if now is None else now
    zone = _PARTITION_ZONES[spec.asset_class]
    return flat_file_published(day, now=moment, zone=zone, cutoff=cutoff)


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
        what: The kind of bar, for messages: `"1-day bar"` or `"bar"`.
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
        """Stamp a column of bars at 00:00 of their days (`timestamps.day_starts`).

        Args:
            label: The dataset, for the message.
            values: The bars' `Timestamp` column, an Arrow `timestamp("ns", "UTC")` array.
            source: Where the bars came from, for the message: a REST path template or a
                flat-file prefix.

        Returns:
            The stamps at the start of their days, in the same type; nulls stay null.

        A stamp at another time of day than 00:00 or one of `known`, or on a day `weekdays`
        or `months` rules out, raises the `MassiveError` of `timestamps.day_starts`, which
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
"""Per class with a day zone, the times of day besides 00:00 accepted for a bar of several
days, a week, or longer, which only the aggregates range serves. No sample shows one; the
indices range's midnight in Chicago is presumed from its 1-day bars."""

_WEEK_STAMP_DAYS: Final = frozenset({5, 6, 0})
"""The days a week bar of a class that does not trade at weekends may be stamped on: the
Saturday or Sunday before its Monday-to-Friday week, which belong to it, or that Monday."""

_MONDAY: Final = frozenset({0})
"""The day a forex or crypto week bar must be stamped on: the Monday its calendar week starts."""

_PERIOD_MONTHS: Final[Mapping[Timespan, frozenset[int]]] = {
    Timespan.MONTH: frozenset(range(1, 13)),
    Timespan.QUARTER: frozenset({1, 4, 7, 10}),
    Timespan.YEAR: frozenset({1}),
}
"""The months whose 1st a month, quarter, or year bar may be stamped on."""

_DAYS_OR_LONGER: Final = frozenset(
    {Timespan.DAY, Timespan.WEEK, Timespan.MONTH, Timespan.QUARTER, Timespan.YEAR}
)


def stamp_rule(spec: DatasetSpec, request: Request) -> StampRule | None:
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
    also be stamped on the day its period starts, or the whole-window rule would place it in
    another period.

    The indicators' values are per bar too, but their stamps are not checked: Massive
    documents none of their times (its samples sit at arbitrary milliseconds), and no other
    source serves them, so an indicator value of a day or longer is labelled with the day its
    stamp falls on (`row_times.TimeRule`).

    Args:
        spec: The dataset.
        request: The normalized arguments.

    Returns:
        The rule, with no `remedy` yet, for bars of a day or longer of stocks, options,
        indices, forex, and crypto; None for intraday bars, futures bars, and every dataset
        that is not bars.
    """
    asset_class = spec.asset_class
    # `is None` first narrows the type for the lookups below.
    if (
        not spec.is_bars
        or request.timespan not in _DAYS_OR_LONGER
        or asset_class is None
        or asset_class not in DAY_ZONES
    ):
        return None
    zone = DAY_ZONES[asset_class]
    timespan = request.timespan
    if timespan is Timespan.DAY and request.multiplier == 1:
        return StampRule(zone, _ONE_DAY_STAMPS[asset_class], "1-day bar")
    known = _LONGER_STAMPS[asset_class]
    if timespan is Timespan.WEEK:
        weekdays = _MONDAY if trades_at_weekends(asset_class) else _WEEK_STAMP_DAYS
        return StampRule(zone, known, "bar", weekdays=weekdays)
    return StampRule(zone, known, "bar", months=_PERIOD_MONTHS.get(timespan, frozenset()))


def within_publication_grace(
    spec: DatasetSpec, day: date, *, cutoff: time, now: datetime | None = None
) -> bool:
    """Tell whether a missing file for `day` may still just be late, rather than a holiday.

    A file counts as published from `publish_time` (the configured cutoff, New York time, after
    the day ends in the class's day zone). For `PUBLICATION_GRACE` after that, a missing file
    is treated as late; from then on, as a market holiday that has no file. There is no holiday
    calendar, so this window is the only way to tell the two apart.

    Args:
        spec: The dataset; its asset class picks the day zone (Chicago sessions for futures).
        day: The file's partition date.
        cutoff: `MassiveConfig.flat_file_publish_cutoff`, a New York wall-clock time.
        now: The current instant; None reads the clock.

    Returns:
        True before `publish_time(day) + PUBLICATION_GRACE`.
    """
    moment = _now() if now is None else now
    published = publish_time(day, zone=_PARTITION_ZONES[spec.asset_class], cutoff=cutoff)
    return moment.astimezone(UTC) < (published + PUBLICATION_GRACE).astimezone(UTC)


def flat_file_prefix(spec: DatasetSpec, request: Request) -> str | None:
    """Return the dataset's S3 key prefix for a request, or None when no file serves it.

    Args:
        spec: The dataset.
        request: The normalized arguments; for bars, the timespan selects the prefix, and only
            multiplier 1 has files.

    Returns:
        The prefix, such as `"us_stocks_sip/minute_aggs_v1"`; a futures prefix still holds the
        placeholder `{venue}`.
    """
    if request.multiplier != 1:
        return None
    return spec.flat_file_prefixes.get(request.timespan)


def rest_history_problem(spec: DatasetSpec, request: Request) -> str | None:
    """Tell why REST cannot serve a dataset's `history()` request, if it cannot.

    REST serves history when the dataset has a REST history endpoint and the symbols are
    explicit; `dm.ALL` is served only for 1-day bars of a dataset with a grouped-daily endpoint
    (one request per day for the whole market). Shared by `history(source=REST)`, which raises
    the reason, and the `AUTO` choice, which weighs it.

    Args:
        spec: The dataset.
        request: The normalized arguments (symbols, timespan, multiplier).

    Returns:
        The reason, such as `"indices.values has no REST history"`, or None when REST can
        serve the request.
    """
    if not spec.has_rest_history:
        return f"{spec.name} has no REST history"
    grouped = (
        spec.has_grouped_daily and request.timespan is Timespan.DAY and request.multiplier == 1
    )
    if isinstance(request.symbols, _All) and not grouped:
        return f"{spec.name} history over REST needs explicit symbols"
    return None


def _flat_file_problems(
    spec: DatasetSpec,
    request: Request,
    dates: Sequence[date],
    *,
    config: MassiveConfig,
    now: datetime,
) -> list[str]:
    """List every reason flat files cannot serve a request; empty when they can.

    Args:
        spec: The dataset.
        request: The normalized arguments.
        dates: The partition dates of the request (`partition_dates`).
        config: The session's configuration (credentials and publish cutoff).
        now: The current instant.

    Returns:
        One human-readable reason per violated constraint, in a fixed order: credentials, the
        dataset's files, adjustment, publication.
    """
    problems = []
    if config.s3_access_key_id is None:
        problems.append(
            "no S3 credentials are configured (MassiveConfig s3_access_key_id and "
            "s3_secret_access_key, or MASSIVE_S3_ACCESS_KEY_ID and MASSIVE_S3_SECRET_ACCESS_KEY)"
        )
    if flat_file_prefix(spec, request) is None:
        if spec.is_bars and spec.flat_file_prefixes:
            spans = ", ".join(f"1-{span}" for span in spec.flat_file_prefixes)
            problems.append(f"{spec.name} flat files hold only {spans} bars")
        else:
            problems.append(f"{spec.name} has no flat files")
    if request.adjusted is True:
        problems.append(
            "flat-file bars are unadjusted (as traded) and one history() never mixes adjusted "
            "and unadjusted prices, so adjusted bars come only from REST; pass adjusted=False "
            "for the flat files"
        )
    zone = _PARTITION_ZONES[spec.asset_class]
    cutoff = config.flat_file_publish_cutoff
    late = [d for d in dates if not flat_file_published(d, now=now, zone=zone, cutoff=cutoff)]
    if late:
        problems.append(
            f"the file for {late[0].isoformat()} is {NOT_PUBLISHED} (a day's file counts as "
            f"published from {cutoff.isoformat('minutes')} New York time after the day ends)"
        )
    return problems


class SourceChoice(NamedTuple):
    """Where a `history()` call reads from, and why."""

    source: HistorySource
    """`HistorySource.FLAT_FILES` or `HistorySource.REST`."""
    reason: str
    """Why, for the log line of an `AUTO` call, such as
    `"2 symbols < flat_file_symbol_threshold 50"`."""


def choose_source(
    spec: DatasetSpec,
    request: Request,
    dates: Sequence[date],
    *,
    requested: HistorySource,
    config: MassiveConfig,
    now: datetime | None = None,
) -> SourceChoice:
    """Decide whether a `history()` call reads flat files or REST, for the whole range.

    Flat files are possible when S3 credentials are configured, the dataset has a file for the
    request (for bars: multiplier 1 and a timespan the dataset has files for, which is `MINUTE`
    or `DAY`, or `MINUTE` or `SESSION` for futures), the request is not adjusted
    (`Request.adjusted` is not True), and every partition date is published at `now`. REST is
    possible when `rest_history_problem` finds nothing. Under `AUTO`, when both are possible,
    flat files win for `dm.ALL` (and for datasets without instruments) or for at least
    `config.flat_file_symbol_threshold` symbols, and REST wins for fewer. The choice covers the
    whole range: one unpublished date sends all of it to REST. Pure: no I/O.

    Args:
        spec: The dataset.
        request: The normalized arguments, with static symbols (a tuple, `dm.ALL`, or None for
            a dataset without instruments).
        dates: The partition dates of the request (`partition_dates`).
        requested: `HistorySource.AUTO` or `HistorySource.FLAT_FILES`. An explicit `REST` is
            checked by the query with `rest_history_problem` alone.
        config: The session's configuration.
        now: The current instant, for the publication rule; None reads the clock.

    Returns:
        The source and the reason it was chosen.

    Raises:
        ValueError: Under `FLAT_FILES`, if flat files cannot serve the request; under `AUTO`, if
            neither source can. The message lists every violated constraint, per source.
    """
    moment = _now() if now is None else now
    flat = _flat_file_problems(spec, request, dates, config=config, now=moment)
    if requested is HistorySource.FLAT_FILES:
        if flat:
            raise ValueError(f"{spec.name} cannot be read from flat files: {'; '.join(flat)}")
        return SourceChoice(HistorySource.FLAT_FILES, "requested")
    rest = rest_history_problem(spec, request)
    if flat and rest is not None:
        raise ValueError(
            f"{spec.name} history has no possible source. "
            f"Flat files: {'; '.join(flat)}. REST: {rest}."
        )
    if flat:
        return SourceChoice(HistorySource.REST, f"flat files cannot serve it: {'; '.join(flat)}")
    if rest is not None:
        return SourceChoice(HistorySource.FLAT_FILES, f"REST cannot serve it: {rest}")
    threshold = config.flat_file_symbol_threshold
    if not isinstance(request.symbols, tuple):
        return SourceChoice(HistorySource.FLAT_FILES, "every symbol")
    count = len(request.symbols)
    if count >= threshold:
        reason = f"{count} symbols >= flat_file_symbol_threshold {threshold}"
        return SourceChoice(HistorySource.FLAT_FILES, reason)
    return SourceChoice(
        HistorySource.REST, f"{count} symbols < flat_file_symbol_threshold {threshold}"
    )
