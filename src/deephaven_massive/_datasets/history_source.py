"""Where `history()` reads from: the window it keeps, its partition dates, and the source.

`HistoryQuery.history()` asks this module what a call reads and which transport serves it,
then calls that transport, which does the I/O (the flat-file transport also reads from REST
what the files cannot serve, `_ingest/history_reads.py`). The module is pure (the
current time comes from `_now`, which tests replace) and makes no network or Deephaven call;
only `warn_unfilled` logs.

- `history_window` turns a call's bounds into a `HistoryWindow`: the bounds by kind (whole
  days or sessions, or exact instants; `dates.bounds`), the rows to keep (on `Timestamp`, or a
  bar's `TimestampStart`, and for futures named by date on their session, `Date`), and the
  instants a REST request covers. Bars are kept only when their whole window lies inside the
  range and has ended: a request selects them by their start (`bar_periods.bar_starts`), or
  for futures by their session label, and a bar whose `TimestampEnd` lies after the range's
  end or after now, whose `TimestampStart` lies before the range's start, or (for futures bars
  of several sessions or longer) that has not certainly ended, is dropped once its time
  columns are filled (`HistoryWindow.bars`).
- `partition_dates` turns a window into the dates of the flat files it needs, in the asset
  class's day zone; a futures date names a trading session.
- `rest_history_problem` says why REST cannot serve a request, if it cannot: the dataset has
  no REST history endpoint, or the symbols are `dm.ALL` and the request is not daily bars with
  a grouped-daily endpoint.
- `choose_source` decides between flat files and REST for the whole range. Flat files can serve
  a request when S3 credentials are configured, the dataset has a file for the request (for
  bars only 1-minute and 1-day bars, and 1-session bars for futures, and only unadjusted where
  the dataset has an `adjusted` parameter). When both sources can, many symbols
  (`MassiveConfig.flat_file_symbol_threshold` or more, or `dm.ALL`) read flat files and fewer
  use REST. Whether a day's file is published yet is never guessed from the clock: the
  flat-file transport finds out by reading, and under `AUTO` it reads the days whose
  files are not there yet from REST, with the same unadjusted request, so one call never
  mixes adjusted and unadjusted prices: adjusted bars come only from REST. Bars of a
  day or longer are stamped alike by every source (`row_times.stamp_rule`), so the choice never
  changes which day a bar is labelled with; a stamp of a form Massive is not known to use is
  served too, labelled the same way, and logs a WARNING.

- `warn_unfilled` warns, once per session, when an `AUTO` choice leaves columns null that the
  other source would fill.

Massive says a day's files land "by approximately 11:00 AM ET the following day", and the
time varies (Friday's files have landed late on Saturday morning), so every message about a
file not there yet says so (`PUBLICATION_NOTE`).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from enum import StrEnum
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, NamedTuple, cast

from deephaven_massive._datasets.bar_periods import (
    bar_starts,
    futures_bar_request,
    futures_first_bucket_day,
    intraday,
)
from deephaven_massive._datasets.catalog import timespan_key
from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._datasets.dates import (
    DaySpan,
    TimeRange,
    bounds,
    day_class,
    day_end,
    day_zone,
)
from deephaven_massive._datasets.filters import filter_key, param_family
from deephaven_massive._datasets.futures_sessions import (
    futures_session,
    futures_session_before,
    futures_session_end,
    futures_session_open,
    weekday_on_or_after,
    weekday_on_or_before,
)
from deephaven_massive._datasets.row_times import BarBounds, FuturesBarCompletion, StartRule
from deephaven_massive._enums import AssetClass, HistorySource, Timespan
from deephaven_massive._massive import ParamKind, Transport, source
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from deephaven_massive._config import MassiveConfig
    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._log_limits import BoundedLog
    from deephaven_massive._types import DateLike

__all__ = [
    "HISTORY_TRANSPORTS",
    "PUBLICATION_NOTE",
    "Alternative",
    "HistoryWindow",
    "SourceChoice",
    "choose_source",
    "current_partition",
    "flat_file_prefix",
    "history_window",
    "partition_dates",
    "rest_history_problem",
    "session_date_param",
    "warn_unfilled",
]

logger = logging.getLogger(__name__)

PUBLICATION_NOTE: Final = (
    "Massive usually publishes a day's flat files by about 11:00 ET the next day, but the time "
    "varies"
)
"""What every message about a flat file not published yet says about when it will be."""

_ONE_DAY: Final = timedelta(days=1)

HISTORY_TRANSPORTS: Final[Mapping[HistorySource, Transport]] = MappingProxyType(
    {HistorySource.REST: Transport.REST, HistorySource.FLAT_FILES: Transport.FLAT_FILES}
)
"""The transport behind each source `history()` can read."""


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
    frozen, so the transport's reader threads may share it. It holds the instant it was built
    for (`now`), so every read of the call keeps the same rows whenever it runs: a day the
    flat files lack, read from REST later in the call, builds its own window with that
    instant rather than the clock's.

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
        now: The instant the window was built for: the end of a range that ends now, and
            the instant by which a kept bar has ended. An aware UTC `datetime`.
    """

    span: TimeRange | DaySpan
    times: TimeRange | None
    sessions: DaySpan | None
    requested: TimeRange | None
    bars: BarBounds | None = None
    now: datetime = field(kw_only=True)

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
    has ended by now: its `TimestampStart` must be no earlier than the range's start
    (`bar_periods.bar_starts` selects the starts that fit), and its `TimestampEnd` no later than
    `HistoryWindow.bars.latest`. The end is conservative, so a bar whose window might reach past
    the range is left out: an index day bar ends at 01:00 New York time. A futures bar of a
    session or longer named by datetimes is kept by its own window, `TimestampStart` and
    `TimestampEnd` as filled, and only once it has certainly ended
    (`bar_periods.futures_bar_ended`); named by dates, by the rules of `_futures_sessions`.

    Args:
        spec: The dataset.
        request: The normalized arguments; for bars, the timespan and multiplier.
        start: The first day or instant.
        end: The last day, the first instant excluded, or None for now.
        classes: The classes of the request's symbols, for `dates.day_class`.
        now: The instant the window is built for, such as the instant a `history()` call
            began, so a later read of the same call keeps the same rows; None reads the clock.

    Returns:
        The window, holding the instant it was built for (`HistoryWindow.now`).

    Raises:
        ValueError: From `dates.bounds`, for an unparseable or naive bound, a date mixed with
            a datetime, or a reversed range; from `_futures_sessions`, for futures trades and
            quotes named by session dates with a `session_end_date` filter.
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
        return HistoryWindow(span, instants, None, instants, now=moment)
    ended = TimeRange(instants.start, min(instants.end, moment))
    last = instants.end if isinstance(span, TimeRange) else day_end(asset_class, span.last)
    if futures and not intraday(timespan):
        # A futures bucket starts at 00:00 UTC, before its first session's 17:00 Chicago open,
        # so the bars are kept by their own window, as filled, not by the sessions' opens.
        completion = FuturesBarCompletion(timespan, request.multiplier, moment)
        whole = BarBounds(instants.start, min(last, moment), StartRule.RANGE, completion)
        logger.debug(
            "%s %d-%s bars by instants: request %s to %s; keep a bar starting at or after %s "
            "and ending by %s once it has certainly ended",
            spec.name,
            request.multiplier,
            timespan.value,
            instants.start.isoformat(),
            instants.end.isoformat(),
            whole.earliest.isoformat(),
            whole.latest.isoformat(),
        )
        return HistoryWindow(span, ended, None, instants, whole, now=moment)
    keep = bar_starts(asset_class, ended, timespan=timespan, multiplier=request.multiplier)
    # Futures intraday bars named by instants take `window_start` exactly. The aggregates' `{from}`
    # starts where the first whole period's days begin, so the bar of that period is returned
    # whether Massive selects bars by their stamp or builds them from `{from}`.
    requested = instants if futures else TimeRange(keep.start, instants.end)
    whole = BarBounds(instants.start, min(last, moment), StartRule.RANGE)
    return HistoryWindow(span, keep, None, requested, whole, now=moment)


def _futures_sessions(
    spec: DatasetSpec, request: Request, span: DaySpan, *, now: datetime
) -> HistoryWindow:
    """Return the window of futures named by session dates.

    Dates name sessions, and a bar is kept only when its whole window lies in the range:

    - An intraday bar is kept when its session lies in the range and it has ended.
    - A 1-session bar is kept when its label, `Date`, lies in the range and its session has
      closed.
    - A bar of several sessions, a week, or longer is a bucket of UTC days labelled with its
      last session. It is kept when the first weekday on or after its bucket's first day (the
      UTC date of `TimestampStart`) is on or after the first date, so no session of the bucket
      lies before the range; when its label is no later than the last date; when its
      `TimestampStart` is no earlier than the `{from}` sent, so its whole window lies in the
      request; and when it has certainly ended (`bar_periods.futures_bar_ended`). These checks
      select whole windows by their stamps: a bucket Massive cut short and stamped at `{from}`
      looks whole to them and would be kept.
      So a month bar needs the range to reach the session that opens on the month's last
      day: September 2026 needs `2026-09-01` to `2026-10-01`.

    Args:
        spec: The dataset, with the futures class.
        request: The normalized arguments.
        span: The sessions.
        now: The current instant.

    Returns:
        Trades and quotes: every row of the sessions, requested one session at a time. Bars:
        the bars requested from `bar_periods.futures_bar_request`, kept by the rules above.

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
        return HistoryWindow(span, None, span, None, now=now)
    multiplier = request.multiplier
    requested = futures_bar_request(span, timespan=timespan, multiplier=multiplier)
    latest = min(futures_session_end(span.last), now)
    if intraday(timespan):
        # Kept by their session: a bar may open before its first session's date.
        whole = BarBounds(futures_session_open(span.first), latest, StartRule.LABEL)
        ended = TimeRange(requested.start, min(requested.end, now))
        keep = bar_starts(AssetClass.FUTURES, ended, timespan=timespan, multiplier=multiplier)
        return HistoryWindow(span, keep, span, requested, whole, now=now)
    if timespan is Timespan.SESSION and multiplier == 1:
        whole = BarBounds(futures_session_open(span.first), latest, StartRule.LABEL)
        # Only finished sessions, and no session ends on a Saturday or Sunday.
        last = min(span.last, futures_session(now) - _ONE_DAY)
        finished = DaySpan(weekday_on_or_after(span.first), weekday_on_or_before(last))
        return HistoryWindow(span, None, finished, requested, whole, now=now)
    first_bucket = futures_first_bucket_day(span.first)
    earliest = max(datetime.combine(first_bucket, time(), tzinfo=UTC), requested.start)
    completion = FuturesBarCompletion(timespan, multiplier, now)
    whole = BarBounds(earliest, latest, StartRule.RANGE, completion)
    logger.debug(
        "%s %d-%s bars of sessions %s to %s: request %s to %s; keep a bar starting at or after "
        "%s (its first session on or after %s), labelled no later than %s, once it has "
        "certainly ended",
        spec.name,
        multiplier,
        timespan.value,
        span.first.isoformat(),
        span.last.isoformat(),
        requested.start.isoformat(),
        requested.end.isoformat(),
        earliest.isoformat(),
        span.first.isoformat(),
        span.last.isoformat(),
    )
    return HistoryWindow(span, None, span, requested, whole, now=now)


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


def current_partition(spec: DatasetSpec, *, now: datetime | None = None) -> date:
    """Return the partition date being traded now, whose flat file cannot exist yet.

    A day's file is written after the day ends, so the file of this date, and of every later
    one, is certainly not published. The flat-file read looks for a later day's file only
    before this date.

    Args:
        spec: The dataset; its asset class picks the day zone (for futures, the session).
        now: The current instant; None reads the clock.

    Returns:
        The current date in the class's day zone, or for futures the current session.
    """
    moment = _now() if now is None else now
    if spec.asset_class is AssetClass.FUTURES:
        return futures_session(moment)
    return moment.astimezone(day_zone(spec.asset_class)).date()


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


def _flat_file_problems(
    spec: DatasetSpec,
    request: Request,
    *,
    config: MassiveConfig,
) -> list[str]:
    """List every lasting reason flat files cannot serve a request; empty when none holds.

    These reasons do not change with time: credentials, the dataset's files, and adjustment.
    A file not published yet is found by the flat-file read itself, never guessed here.

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


class Alternative(StrEnum):
    """Whether the source an `AUTO` choice passed over could serve the same call.

    `history()` warns about columns the chosen source leaves null only when the caller can
    pick the other source.
    """

    AVAILABLE = "available"
    """It can serve the request now: `source=` picks it."""
    UNAVAILABLE = "unavailable"
    """It cannot serve the request (no credentials, no files or endpoint for the request,
    adjusted bars, `dm.ALL` over REST); also for an explicit source."""


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
    *,
    requested: HistorySource,
    config: MassiveConfig,
) -> SourceChoice:
    """Decide whether a `history()` call reads flat files or REST, for the whole range.

    Flat files are possible when S3 credentials are configured, the dataset has a file for the
    request (for bars: multiplier 1 and a timespan the dataset has files for, which is `MINUTE`
    or `DAY`, or `MINUTE` or `SESSION` for futures), the request is not adjusted
    (`Request.adjusted` is not True). REST is possible when `rest_history_problem` finds
    nothing. Under `AUTO`, when both are possible, flat files win for `dm.ALL` (and for
    datasets without instruments) or for at least `config.flat_file_symbol_threshold` symbols,
    and REST wins for fewer. Whether each day's file is published is not part of the choice:
    the flat-file read finds the days whose files are not there yet, and the query reads those
    from REST under `AUTO` (`_ingest/history_reads.py`). Pure: no I/O.

    Args:
        spec: The dataset.
        request: The normalized arguments, with static symbols (a tuple, `dm.ALL`, or None for
            a dataset without instruments).
        requested: `HistorySource.AUTO` or `HistorySource.FLAT_FILES`. An explicit `REST` is
            checked by the query with `rest_history_problem` alone.
        config: The session's configuration.

    Returns:
        The source, the reason it was chosen, and under `AUTO` whether the other source could
        serve the same call (`Alternative`).

    Raises:
        ValueError: Under `FLAT_FILES`, if flat files cannot serve the request; under `AUTO`, if
            neither source can. The message lists every violated constraint, per source.
    """
    flat = _flat_file_problems(spec, request, config=config)
    if requested is HistorySource.FLAT_FILES:
        if flat:
            raise ValueError(f"{spec.name} cannot be read from flat files: {'; '.join(flat)}")
        return SourceChoice(HistorySource.FLAT_FILES, "requested")
    rest = rest_history_problem(spec, request)
    if (
        rest is not None
        and request.adjusted is True
        and len(flat) == 1
        and isinstance(request.symbols, _All)
    ):
        # Only adjustment holds flat files back, and only dm.ALL holds REST back: say so.
        raise ValueError(
            f"{spec.name} history of every symbol (dm.ALL) with adjusted=True has no source: "
            "only REST serves split-adjusted bars, and REST reads bars one symbol at a time "
            "(every symbol at once only for 1-day bars), while flat files hold as-traded "
            "prices. Pass adjusted=False to read the as-traded flat files, or name the symbols "
            "to read adjusted bars from REST"
        )
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
        return SourceChoice(
            HistorySource.REST,
            f"flat files cannot serve it: {'; '.join(flat)}",
            Alternative.UNAVAILABLE,
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


def warn_unfilled(
    spec: DatasetSpec, choice: SourceChoice, *, catalog: Catalog, warnings: BoundedLog
) -> None:
    """Warn when `AUTO` chose a source that leaves columns null which the other one fills.

    Under `AUTO` the source changes with the symbol count, so the same query could fill a
    column for many symbols and leave it null for few. The columns each source fills come from
    the generated schemas (`Catalog.history_columns`). The WARNING names the columns the chosen
    source leaves null, the choice and its reason, and what fills them: the `source=` of the
    other source (`Alternative.AVAILABLE`). It is logged once per session, dataset, and set of
    columns. When the other source can never serve the call (`Alternative.UNAVAILABLE`), the
    result cannot change and nothing is logged: the table description says which source it
    was. `history()` skips it for a range no row can lie in, which reads nothing.

    Args:
        spec: The dataset.
        choice: The `AUTO` choice.
        catalog: The session's catalog, which knows the columns each source fills.
        warnings: The session's once-per-session warnings (`SessionCore.warnings`).
    """
    if choice.alternative is Alternative.UNAVAILABLE:
        return  # the result cannot change, and the caller cannot pick the other source
    other = HistorySource.FLAT_FILES if choice.source is HistorySource.REST else HistorySource.REST
    missing = catalog.history_columns(spec, HISTORY_TRANSPORTS[other]) - (
        catalog.history_columns(spec, HISTORY_TRANSPORTS[choice.source])
    )
    if not missing:
        return
    columns = ", ".join(sorted(missing))
    remedy = f"pass source=dm.HistorySource.{other.name} to get them"
    warnings.log(
        f"history source {spec.name} {choice.source.name}: {columns}",
        "%s history: AUTO read %s (%s); the read leaves %s null, which the other source, "
        "%s, fills: %s (logged once per session)",
        spec.name,
        TRANSPORT_LABELS[HISTORY_TRANSPORTS[choice.source]],
        choice.reason,
        columns,
        TRANSPORT_LABELS[HISTORY_TRANSPORTS[other]],
        remedy,
    )
