"""Where `history()` reads from: the partition dates of a window and the `AUTO` source choice.

`HistoryQuery.history()` asks this module which transport serves a request, then calls that
transport; the transports themselves only do I/O. The module is pure (the current time comes
from `_now`, which tests replace) and makes no network or Deephaven call.

- `partition_dates` turns a `history()` window into the dates of the flat files it needs, in
  the asset class's day zone; a futures date names a trading session.
- `rest_history_problem` says why REST cannot serve a request, if it cannot: the dataset has
  no REST history endpoint, or the symbols are `dm.ALL` and the request is not daily bars with
  a grouped-daily endpoint.
- `choose_source` decides between flat files and REST for the whole range. Flat files can serve
  a request when S3 credentials are configured, the dataset has a file for the request (for
  bars only 1-minute and 1-day bars, and 1-session bars for futures, and only unadjusted where
  the dataset has an `adjusted` parameter), and every file the range needs is already
  published. When both sources can, many symbols (`MassiveConfig.flat_file_symbol_threshold`
  or more, or `dm.ALL`) read flat files and fewer use REST.

A day's file counts as published from `MassiveConfig.flat_file_publish_cutoff` (New York time)
after the next day starts in the class's day zone (`dates.publish_time`); futures use their
Chicago session date. Massive says files land "by approximately 11:00 AM ET the following day".
"""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, time, timedelta
from typing import TYPE_CHECKING, Final, NamedTuple

from deephaven_massive._datasets.dates import (
    CHICAGO,
    DAY_ZONES,
    NEW_YORK,
    FuturesSessions,
    flat_file_published,
    futures_session,
    futures_session_before,
    publish_time,
)
from deephaven_massive._enums import AssetClass, HistorySource, Timespan
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence
    from datetime import tzinfo

    from deephaven_massive._config import MassiveConfig
    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.dates import TimeRange
    from deephaven_massive._datasets.request import Request

__all__ = [
    "NOT_PUBLISHED",
    "PUBLICATION_GRACE",
    "SourceChoice",
    "choose_source",
    "flat_file_prefix",
    "partition_dates",
    "rest_history_problem",
    "within_publication_grace",
]

logger = logging.getLogger(__name__)

NOT_PUBLISHED: Final = "not yet published; flat files land ~11:00 ET next day"
"""The wording for a flat file that cannot be expected to exist yet."""

PUBLICATION_GRACE: Final = timedelta(days=1)
"""How long after its publication time a missing file may still just be late."""

_PARTITION_ZONES: Final[Mapping[AssetClass | None, tzinfo]] = dict(
    [*DAY_ZONES.items(), (AssetClass.FUTURES, CHICAGO), (None, NEW_YORK)]
)
"""The zone whose calendar dates name a class's files and start its publication clock.

Futures files are named by trading session, and Massive standardizes futures data in Central
Time. A dataset without an asset class uses New York, as `dates.bounds` does."""


def _now() -> datetime:
    """Return the current instant: the one clock of the source choice, which tests replace.

    Returns:
        An aware UTC `datetime`.
    """
    return datetime.now(UTC)


def partition_dates(spec: DatasetSpec, window: TimeRange | FuturesSessions) -> tuple[date, ...]:
    """Return the dates of the flat files a `history()` window needs, oldest first.

    For a `TimeRange` they are the dates of `start` and of `end - 1 ns` in the class's day zone
    and every date between: New York for stocks, options, and indices, UTC for forex and
    crypto. For futures they are trading sessions: from `first` (or, for a `start` instant,
    the session that instant belongs to) to `last` (or, for an `end` instant, the session of
    the last instant before it, and never before `first`); a session opens at 17:00 Chicago
    time on the day before its `session_end_date` (`dates.futures_session`). An instant side
    is then filtered on `Timestamp`. A holiday's trading lies in the next session's file,
    which the flat-file transport reads when the last session's file is missing.

    Args:
        spec: The dataset; its asset class picks the day zone.
        window: The normalized `history()` bounds (`dates.bounds`).

    Returns:
        The partition dates; never empty.
    """
    if isinstance(window, FuturesSessions):
        first = window.first or futures_session(window.window.start)
        # A date start names its session even when the end instant falls before that
        # session's regular open: a holiday opened it early (2025-01-21 from Sunday 17:00).
        last = window.last or max(first, futures_session_before(window.window.end))
    elif spec.asset_class is AssetClass.FUTURES:
        first, last = futures_session(window.start), futures_session_before(window.end)
    else:
        dates = window.partition_dates(_PARTITION_ZONES[spec.asset_class])
        first, last = dates[0], dates[-1]
    return tuple(first + timedelta(days=n) for n in range((last - first).days + 1))


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
