"""Where `history()` reads from: the window it keeps, its partition dates, and the source.

`HistoryQuery.history()` asks this module what a call reads and which transport serves it,
then calls that transport; the transports themselves only do I/O. The module is pure (the
current time comes from `_now`, which tests replace) and makes no network or Deephaven call.

- `history_window` turns a call's bounds into a `HistoryWindow`: the bounds by kind (whole
  days or sessions, or exact instants; `dates.bounds`), the rows to keep (on `Timestamp`, or a
  bar's `TimestampStart`, and for futures named by date on their session, `Date`), and the
  instants a REST request covers. Bars are kept only when their whole window lies inside the
  range and has ended: a request selects them by their start (`bar_periods.bar_starts`,
  `bar_periods.bar_sessions`), and a bar whose `TimestampEnd` lies after the range's end or after
  now, or whose `TimestampStart` lies before the range's start, is dropped once its time
  columns are filled (`HistoryWindow.bars`).
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
  day or longer are stamped alike by every source (`row_times.stamp_rule`), so the choice never
  changes which day a bar is labelled with; a stamp of a form Massive is not known to use is
  served too, labelled the same way, and logs a WARNING.

A day's file counts as published from `MassiveConfig.flat_file_publish_cutoff` (New York time)
after the next day starts in the class's day zone (`publish_time`); futures use their
Chicago session date. Massive says files land "by approximately 11:00 AM ET the following day".
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from typing import TYPE_CHECKING, Final, NamedTuple, cast

from deephaven_massive._datasets.bar_periods import (
    bar_sessions,
    bar_starts,
    futures_bar_request,
    intraday,
)
from deephaven_massive._datasets.catalog import timespan_key
from deephaven_massive._datasets.dates import (
    NEW_YORK,
    DaySpan,
    TimeRange,
    bounds,
    day_class,
    day_end,
    day_zone,
    midnight,
)
from deephaven_massive._datasets.filters import filter_key, param_family
from deephaven_massive._datasets.futures_sessions import (
    futures_session,
    futures_session_before,
    futures_session_end,
    futures_session_open,
)
from deephaven_massive._datasets.row_times import BarBounds, StartRule
from deephaven_massive._enums import AssetClass, HistorySource, Timespan
from deephaven_massive._massive import ParamKind, Transport, source
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence
    from datetime import tzinfo

    from deephaven_massive._config import MassiveConfig
    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._types import DateLike

__all__ = [
    "NOT_PUBLISHED",
    "PUBLICATION_GRACE",
    "Alternative",
    "HistoryWindow",
    "SourceChoice",
    "choose_source",
    "flat_file_prefix",
    "flat_file_published",
    "history_window",
    "partition_dates",
    "publish_time",
    "published",
    "rest_history_problem",
    "session_date_param",
    "within_publication_grace",
]

logger = logging.getLogger(__name__)

NOT_PUBLISHED: Final = "not yet published; flat files land ~11:00 ET next day"
"""The wording for a flat file that cannot be expected to exist yet."""

PUBLICATION_GRACE: Final = timedelta(days=1)
"""How long after its publication time a missing file may still just be late."""

_ONE_DAY: Final = timedelta(days=1)


def _now() -> datetime:
    """Return the current instant: the one clock of `history()`, which tests replace.

    Returns:
        An aware UTC `datetime`.
    """
    return datetime.now(UTC)


def session_date_param(spec: DatasetSpec) -> str:
    """Return the REST parameter that names one futures session on a dataset's history path.

    Futures trades and quotes named by date are requested one session at a time, by the one
    date parameter of their history path (`session_end_date`), and `history()` refuses a filter
    on it. The name is read from the path's generated parameters, so only the spec refresh
    writes it. A history path that takes no date parameter, or more than one, raises
    `ValueError`.

    Args:
        spec: A futures dataset of trades or quotes, with a REST history path.

    Returns:
        The parameter's wire name.
    """
    rest_path = source(Transport.REST, cast("str", spec.history_path))
    [name] = [param.name for param in rest_path.params if param.kind is ParamKind.DATE]
    return name


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
        bars: For a dataset of bars, the window its bars must lie in once their time columns
            are filled (`row_times.within`); the transports drop every other bar. None for
            ticks, which have no bar window.
    """

    span: TimeRange | DaySpan
    times: TimeRange | None
    sessions: DaySpan | None
    requested: TimeRange | None
    bars: BarBounds | None = None

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
    has ended by now: its start must fit (`bar_periods.bar_starts`; for futures named by date, a bar
    of a session or longer by its session label, `Date`, under both readings of the label of
    a bar of several units, `bar_periods.bar_sessions`, and an intraday bar by its session), and its
    `TimestampEnd` must be no later than `HistoryWindow.bars.latest`. The end is conservative, so
    a bar whose window might reach past the range is left out: an index day bar ends at 01:00
    New York time, and a futures bar of several sessions allows for holidays
    (`bar_periods.last_trading_day`).

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
    last = instants.end if isinstance(span, TimeRange) else day_end(asset_class, span.last)
    whole = BarBounds(instants.start, min(last, moment), StartRule.RANGE)
    return HistoryWindow(span, keep, None, requested, whole)


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
        the bars requested from `bar_periods.futures_bar_request`, kept by the session rules of
        `history_window`.

    Raises:
        ValueError: For trades or quotes on a query that also filters `session_end_date` (in
            any of its forms), which the dates set themselves; the message says to drop the
            filter or pass datetimes.
    """
    timespan = request.timespan
    if not spec.is_bars or timespan is None:
        reserved = {filter_key(name) for name in param_family(session_date_param(spec))}
        if any(key in reserved for key, _ in request.filters):
            raise ValueError(
                f"{spec.name}: history() dates set session_end_date themselves; drop the "
                "session_end_date filter, or pass datetimes to combine it with a time range"
            )
        return HistoryWindow(span, None, span, None)
    requested = futures_bar_request(span)
    # Kept by their label: a bar may open before its first session's date.
    whole = BarBounds(
        futures_session_open(span.first), min(futures_session_end(span.last), now), StartRule.LABEL
    )
    if intraday(timespan):
        ended = TimeRange(requested.start, min(requested.end, now))
        keep = bar_starts(
            AssetClass.FUTURES, ended, timespan=timespan, multiplier=request.multiplier
        )
        return HistoryWindow(span, keep, span, requested, whole)
    finished = DaySpan(span.first, min(span.last, futures_session(now) - _ONE_DAY))
    sessions = bar_sessions(finished, timespan=timespan, multiplier=request.multiplier)
    return HistoryWindow(span, None, sessions, requested, whole)


def partition_dates(spec: DatasetSpec, window: HistoryWindow) -> tuple[date, ...]:
    """Return the dates of the flat files a `history()` window needs, oldest first.

    For futures named by date they are the sessions kept (`HistoryWindow.sessions`). For
    futures named by instants they are the sessions of the first and last kept instant and
    every one between; a session opens at 17:00 Chicago time on the day before its
    `session_end_date` (`futures_sessions.futures_session`). A holiday's trading lies in the next
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
    return times.partition_dates(day_zone(spec.asset_class))


def published(spec: DatasetSpec, day: date, *, cutoff: time, now: datetime | None = None) -> bool:
    """Tell whether a day's flat file of a dataset counts as published (`publish_time`).

    Args:
        spec: The dataset; its asset class picks the day zone (Chicago sessions for futures).
        day: The file's partition date.
        cutoff: `MassiveConfig.flat_file_publish_cutoff`, a New York wall-clock time.
        now: The current instant; None reads the clock.

    Returns:
        True from the file's publication time on.
    """
    moment = _now() if now is None else now
    zone = day_zone(spec.asset_class)
    return flat_file_published(day, now=moment, zone=zone, cutoff=cutoff)


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
    published = publish_time(day, zone=day_zone(spec.asset_class), cutoff=cutoff)
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
    return spec.flat_file_prefixes.get(timespan_key(request.timespan))


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


def _lasting_flat_file_problems(
    spec: DatasetSpec,
    request: Request,
    *,
    config: MassiveConfig,
) -> list[str]:
    """List every lasting reason flat files cannot serve a request; empty when none holds.

    A lasting reason does not change with time: credentials, the dataset's files, and
    adjustment. The one reason that does, a file not yet published, is `_unpublished`.

    Args:
        spec: The dataset.
        request: The normalized arguments.
        config: The session's configuration (credentials).

    Returns:
        One human-readable reason per violated constraint, in a fixed order: credentials, the
        dataset's files, adjustment.
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
            "adjusted=True needs REST, since flat-file bars are unadjusted (as traded) and one "
            "history() never mixes adjusted and unadjusted prices; pass adjusted=False for "
            "the flat files"
        )
    return problems


def _unpublished(
    spec: DatasetSpec, dates: Sequence[date], *, config: MassiveConfig, now: datetime
) -> str | None:
    """Say which flat file of a request is not published yet, if one is.

    The one reason flat files cannot serve a request that changes with time alone.

    Args:
        spec: The dataset.
        dates: The partition dates of the request.
        config: The session's configuration (the publish cutoff).
        now: The current instant.

    Returns:
        The reason, naming the first unpublished date, or None when every file is published.
    """
    zone = day_zone(spec.asset_class)
    cutoff = config.flat_file_publish_cutoff
    late = [d for d in dates if not flat_file_published(d, now=now, zone=zone, cutoff=cutoff)]
    if not late:
        return None
    return (
        f"the file for {late[0].isoformat()} is {NOT_PUBLISHED} (a day's file counts as "
        f"published from {cutoff.isoformat('minutes')} New York time after the day ends)"
    )


class Alternative(StrEnum):
    """Whether the source an `AUTO` choice passed over could serve the same call.

    `history()` warns about columns the chosen source leaves null only when the answer can
    differ between two runs of one call, or when the caller can pick the other source.
    """

    AVAILABLE = "available"
    """It can serve the request now: `source=` picks it."""
    AFTER_PUBLICATION = "after publication"
    """Flat files are held back only by a day's file not yet published, and once it is,
    `AUTO` picks them for this call: the same call changes source the next day."""
    UNAVAILABLE = "unavailable"
    """It cannot serve the request, for a reason that does not change with time (no
    credentials, no files or endpoint for the request, adjusted bars, `dm.ALL` over REST),
    or `AUTO` would not pick it once it could; also for an explicit source."""


class SourceChoice(NamedTuple):
    """Where a `history()` call reads from, why, and whether the other source could have."""

    source: HistorySource
    """`HistorySource.FLAT_FILES` or `HistorySource.REST`."""
    reason: str
    """Why, for the log line of an `AUTO` call and the table's description, such as
    `"2 symbols < flat_file_symbol_threshold 50"`."""
    alternative: Alternative = Alternative.UNAVAILABLE
    """Whether the other source could serve the same call (`Alternative`)."""


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
        The source, the reason it was chosen, and under `AUTO` whether the other source could
        serve the same call (`Alternative`).

    Raises:
        ValueError: Under `FLAT_FILES`, if flat files cannot serve the request; under `AUTO`, if
            neither source can. The message lists every violated constraint, per source.
    """
    moment = _now() if now is None else now
    lasting = _lasting_flat_file_problems(spec, request, config=config)
    unpublished = _unpublished(spec, dates, config=config, now=moment)
    flat = lasting if unpublished is None else [*lasting, unpublished]
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
    if rest is not None:
        return SourceChoice(HistorySource.FLAT_FILES, f"REST cannot serve it: {rest}")
    threshold = config.flat_file_symbol_threshold
    many = not isinstance(request.symbols, tuple) or len(request.symbols) >= threshold
    if flat:
        # Only an unpublished file changes with time; then the call moves to flat files later.
        later = many and not lasting
        return SourceChoice(
            HistorySource.REST,
            f"flat files cannot serve it: {'; '.join(flat)}",
            Alternative.AFTER_PUBLICATION if later else Alternative.UNAVAILABLE,
        )
    if not isinstance(request.symbols, tuple):
        return SourceChoice(HistorySource.FLAT_FILES, "every symbol", Alternative.AVAILABLE)
    count = len(request.symbols)
    if many:
        reason = f"{count} symbols >= flat_file_symbol_threshold {threshold}"
        return SourceChoice(HistorySource.FLAT_FILES, reason, Alternative.AVAILABLE)
    return SourceChoice(
        HistorySource.REST,
        f"{count} symbols < flat_file_symbol_threshold {threshold}",
        Alternative.AVAILABLE,
    )


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
    start = midnight(partition + _ONE_DAY, zone)
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
