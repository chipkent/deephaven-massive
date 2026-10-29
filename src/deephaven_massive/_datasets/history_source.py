"""Where `history()` reads from: the window it keeps, its partition dates, and the source.

`HistoryQuery.history()` asks this module what a call reads and which transport serves it,
then calls that transport; the transports themselves only do I/O. The module is pure (the
current time comes from `_now`, which tests replace) and makes no network or Deephaven call.

- `history_window` turns a call's bounds into a `HistoryWindow`: the bounds by kind (whole
  days or sessions, or exact instants; `dates.bounds`), the rows to keep (on `Timestamp`, and
  for futures named by date on `SessionEndDate`), and the instants a REST request covers.
  Bars are kept only when their whole window lies inside the range and has ended
  (`dates.bar_starts`, `dates.bar_sessions`).
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
  or more, or `dm.ALL`) read flat files and fewer use REST. Forex and crypto 1-day bars are
  stamped alike by every source (`stamp_rule`), and a crypto stamp off the UTC day boundary
  fails loudly, so the choice never changes which day a bar covers.

A day's file counts as published from `MassiveConfig.flat_file_publish_cutoff` (New York time)
after the next day starts in the class's day zone (`dates.publish_time`); futures use their
Chicago session date. Massive says files land "by approximately 11:00 AM ET the following day".
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from enum import Enum, auto
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
)
from deephaven_massive._enums import AssetClass, ClassFromPrefix, HistorySource, Timespan
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence
    from datetime import tzinfo

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

_PARTITION_ZONES: Final[Mapping[AssetClass | ClassFromPrefix, tzinfo]] = dict(
    [*DAY_ZONES.items(), (AssetClass.FUTURES, CHICAGO), (ClassFromPrefix.ANY, NEW_YORK)]
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
        times: The `Timestamp` range a row must lie in (for bars, the bar starts whose whole
            window fits), or None where only `sessions` bounds the rows.
        sessions: The `SessionEndDate`s a row must have, for futures named by date; else None.
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
    longer is kept by its `SessionEndDate` (`dates.bar_sessions`, over the sessions finished by
    now), and an intraday bar by its session and its end.

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
    the `ValueError` of `dates.bounds`. Futures named by session dates raise the `ValueError`
    of `_futures_sessions` for a `session_end_date` filter on trades or quotes, and for bars
    of a session or longer with a `multiplier` above 1.
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
            any of its forms), which the dates set themselves; or for bars of a session or
            longer that span several units (`multiplier` above 1), whose session label
            Massive does not document, so the bars that lie in the sessions cannot be told
            apart. Each message names what to pass instead.
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
    if not intraday(timespan) and request.multiplier > 1:
        raise ValueError(
            f"{spec.name}: bars of {request.multiplier} {timespan.value}s cannot be selected by "
            "session dates, since the session Massive labels such a bar with is not verified; "
            "pass datetimes, or multiplier=1"
        )
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


class StampRule(Enum):
    """How a transport treats the `Timestamp` of a request's bars (`stamp_rule`)."""

    KEEP = auto()
    """Every other request: the stamps are kept as the source sends them."""
    UTC_DAY = auto()
    """Forex 1-day bars: each stamp moves to 00:00 UTC of the day it falls in."""
    UTC_DAY_CHECKED = auto()
    """Crypto 1-day bars: each stamp moves to 00:00 UTC of its day, and a stamp elsewhere than
    the day's first or last millisecond fails, since the day cut is not verified."""
    UTC_MIDNIGHT = auto()
    """Crypto bars of several days or of a week or longer, from REST: each stamp must be
    00:00 UTC, or the bar would cover other days than its label."""


_UTC_DAY_RULES: Final[Mapping[AssetClass | ClassFromPrefix, StampRule]] = {
    AssetClass.FOREX: StampRule.UTC_DAY,
    AssetClass.CRYPTO: StampRule.UTC_DAY_CHECKED,
}
"""The rule of the 1-day bars of each class whose 1-day bars mean the UTC day."""

_DAYS_OR_LONGER: Final = frozenset(
    {Timespan.DAY, Timespan.WEEK, Timespan.MONTH, Timespan.QUARTER, Timespan.YEAR}
)


def stamp_rule(spec: DatasetSpec, request: Request) -> StampRule:
    """Return how the transports treat the stamps of a request's bars.

    Forex and crypto 1-day bars mean the UTC day on every source, but Massive stamps them
    differently by source (00:00 UTC on the aggregates range, 23:59:59.999 UTC in the
    `day_aggs_v1` flat files), so each stamp moves to 00:00 UTC of its day
    (`timestamps.utc_day_stamps`). Crypto's day cut is not verified (the documented REST
    samples sit at midnight New York time), so a crypto 1-day stamp elsewhere than a UTC day
    boundary fails, and so does a crypto bar of several days or longer whose stamp is not
    00:00 UTC (`timestamps.check_utc_midnights`).

    Args:
        spec: The dataset.
        request: The normalized arguments.

    Returns:
        The rule; `StampRule.KEEP` for everything else.
    """
    if not spec.is_bars or request.timespan not in _DAYS_OR_LONGER:
        return StampRule.KEEP
    if request.timespan is Timespan.DAY and request.multiplier == 1:
        return _UTC_DAY_RULES.get(spec.asset_class, StampRule.KEEP)
    if spec.asset_class is AssetClass.CRYPTO:
        return StampRule.UTC_MIDNIGHT
    return StampRule.KEEP


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
        problems.append("flat-file bars are unadjusted; pass adjusted=False")
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
