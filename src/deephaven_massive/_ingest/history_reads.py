"""History reads from the flat files, with REST serving the days and files they cannot.

`history()` has chosen the flat files as a call's source (`choose_source` in
`_datasets/history_source.py`) when this module's `read_history` runs, through the flat-file
transport (`FlatFileTransport.read_history`). It reads the files (`FlatFileHistory.history`)
and decides what the call returns when they cannot serve all of it:

- Under `AUTO`, when REST can serve the request, nothing is inferred from a missing file: each
  trading day whose file is missing is read from REST, one day at a time and clipped to the
  range, and merged with the files' days in time order; a day REST has no rows for was a day
  without trading. When S3 refuses the flat files, the whole call is read from REST. Either
  way one WARNING names the days REST served (or the refusal) and the columns REST leaves null
  that the flat files fill, and the table's description says so.
- Under `FLAT_FILES`, or when REST cannot serve the request, the flat-file read infers
  holidays (a missing trading day with a later file), a day whose file is not out yet raises,
  and so do the days at the range's end that have no file a week or more later. The table's
  description names each day taken for a holiday, since Massive's file for a trading day may
  simply be missing, and the library never fills or hides a gap in Massive's data. A refusal
  names REST when REST could serve the read. A futures venue the request's `refused_venues`
  skips instead is named in the table's description.
- A futures read by dates whose files held rows of other sessions warns that the range's
  first or last session may be incomplete, and says so on the table.
- The call is one read: its files and its REST reads count their rows in one `ReadTally`,
  checked once after the last of them, so the call fails only when the library could read
  none of the rows of its symbols and window whole, wherever they came from. When S3 refuses
  the files and REST serves the whole call, the rows of files read before the refusal are
  discarded, and so is their count.
- A read by symbol (`read_history_by_symbol`, for a table that follows a live symbol table)
  follows the same rules, but reads the days the files lack from REST symbol by symbol
  (`RestTransport.history_by_symbol`), so one symbol's REST failure fails only that symbol. A
  failure of the files themselves, which serve every symbol at once, is every symbol's.

Runs on the calling thread (a read pool thread for a background read); the REST reads go
through the session's REST transport (`SessionCore.rest_transport`).
"""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Final, Protocol

import pyarrow as pa
from deephaven import arrow as dh_arrow
from deephaven import merge

from deephaven_massive._datasets.bar_periods import intraday
from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._datasets.dates import DaySpan, day_start
from deephaven_massive._datasets.history_source import (
    PUBLICATION_NOTE,
    history_window,
    rest_history_problem,
)
from deephaven_massive._datasets.row_times import time_column
from deephaven_massive._errors import (
    AuthenticationError,
    EntitlementError,
    MassiveError,
    required_instance,
)
from deephaven_massive._ingest.refused_venues import refused_note, refused_source
from deephaven_massive._ingest.symbol_reads import split_by_symbol, symbol_outcomes
from deephaven_massive._log_limits import LoggedKeys, bounded_list
from deephaven_massive._massive import Transport
from deephaven_massive._tables.columns import dataset_table
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.read_tally import ReadTally
from deephaven_massive._tables.transports import SymbolFailure

if TYPE_CHECKING:
    import threading
    from collections.abc import Mapping, Sequence

    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._tables.transports import FlatFileRead, SymbolOutcome

__all__ = ["FlatFileScan", "read_history", "read_history_by_symbol"]

logger = logging.getLogger(__name__)

_ONE_DAY: Final = timedelta(days=1)


class FlatFileScan(Protocol):
    """The call's flat-file transport, as these reads scan it: a `FlatFileHistory`."""

    def history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        /,
        *,
        dates: Sequence[date],
        call: int,
        infer_holidays: bool,
        logged: LoggedKeys,
        tally: ReadTally,
        stop: threading.Event | None,
    ) -> FlatFileRead:
        """Return the rows of `window` from the dataset's flat files, and the days they lack.

        Args:
            spec: The dataset.
            request: The request.
            window: The call's window.
            dates: The partition dates of the files the window needs.
            call: The call id.
            infer_holidays: True to take a missing weekday file for a holiday.
            logged: The facts the call has logged, each logged once per call.
            tally: The call's count of its rows and of those withheld, which the files add to.
            stop: A read's stop token, or None.

        Returns:
            The files' read (`FlatFileHistory.history`).
        """
        ...


def read_history(
    spec: DatasetSpec,
    request: Request,
    window: HistoryWindow,
    *,
    core: SessionCore,
    files: FlatFileScan,
    dates: Sequence[date],
    call: int,
    stop: threading.Event | None,
    forced: bool,
) -> tuple[Table, str | None, str | None]:
    """Read `history()` from the flat files, and from REST what they cannot serve.

    See the module docstring for the rules.

    Args:
        spec: The dataset.
        request: The request with static symbols and the parsed bounds.
        window: The call's window, which holds rows.
        core: The session's core: its REST transport, catalog, and execution context.
        files: The call's flat-file transport.
        dates: The partition dates of the files the window needs.
        call: The call id.
        stop: A background read's stop token, or None for a blocking read.
        forced: True for `source=FLAT_FILES`, which never reads REST.

    Returns:
        The static table; the description's source sentence when REST served any of it,
        which replaces the sentence of the source choice (None when only the flat files did);
        and the sentences the description adds on what the files may lack (`_files_lack_note`:
        the days taken for holidays, the sessions that may be incomplete, the futures venues
        skipped because S3 refused them), or None.

    Raises:
        AuthenticationError: When S3 rejects the S3 credentials under `FLAT_FILES` or when
            REST cannot serve the request.
        EntitlementError: The refusal of `_refused_flat_files`, from the transport's own,
            under `FLAT_FILES` or when REST cannot serve the request.
        MassiveError: If a day's file is not published yet and the call may not or cannot
            read it from REST: `"<dataset>: the flat file for <date> is not published yet.
            Massive usually publishes ..., but the time varies. Retry later, ..."`; or if the
            days at the range's end have no file and none follows them, a week or more before
            today (`FlatFileRead.absent`): `"<dataset>: no flat file exists for <dates>, ..."`.
        RowDataError: If the library could read none of the rows of the call's symbols and
            window whole, across its files and REST reads (`ReadTally.check`), whatever its
            policy.
    """
    problem = rest_history_problem(spec, request)
    tally = ReadTally()
    read = _scan(
        spec,
        request,
        window,
        core=core,
        files=files,
        dates=dates,
        call=call,
        logged=LoggedKeys(),  # a one-time call reads once, so the read keeps what it logged
        tally=tally,
        stop=stop,
        problem=problem,
        forced=forced,
    )
    if read is None:
        tally = ReadTally()  # the files read before the refusal are discarded
        rest = core.rest_transport()
        rows = rest.history(spec, request, window, call=call, tally=tally, stop=stop)
        tally.check(spec.name)
        return rows, "Read from REST: AUTO chose flat files, which S3 refused.", None
    _check_published(spec, read, problem=problem, forced=forced)
    lacks = _files_lack_note(spec, read)
    if not read.unread:
        tally.check(spec.name)
        with entered(core.exec_ctx):
            return merge([read.empty, *(part for _, part in read.parts)]), None, lacks
    filled = _rest_days(
        spec, request, window, core=core, days=read.unread, call=call, tally=tally, stop=stop
    )
    tally.check(spec.name)
    with entered(core.exec_ctx):
        # The files' days and REST's tile (the files are trimmed to their own days), so
        # sorting by the time column puts every row in time order across the sources.
        merged = merge([read.empty, *(part for _, part in read.parts), *filled])
        table = merged.sort(time_column(spec.times))
    days = ", ".join(day.isoformat() for day in read.unread)
    missing = bounded_list(day.isoformat() for day in read.unread)
    _warn_rest_served(core, spec, f"the flat files are missing for these days: {missing}")
    flat = TRANSPORT_LABELS[Transport.FLAT_FILES]
    served = f"Read from {flat}, and from REST for {days}, whose files are missing."
    return table, served, lacks


def _scan(
    spec: DatasetSpec,
    request: Request,
    window: HistoryWindow,
    *,
    core: SessionCore,
    files: FlatFileScan,
    dates: Sequence[date],
    call: int,
    logged: LoggedKeys,
    tally: ReadTally,
    stop: threading.Event | None,
    problem: str | None,
    forced: bool,
) -> FlatFileRead | None:
    """Read the flat files of a call, or learn that REST must serve all of it instead.

    Under `FLAT_FILES`, or when REST cannot serve the request (`problem`), holidays are
    inferred and a refusal raises, unless the request's `refused_venues` skips a refused
    futures venue (`FlatFileHistory.history`); otherwise a refusal of the files logs the WARNING of
    `_warn_rest_served` and the whole call is read from REST.

    Args:
        spec: The dataset.
        request: The request with static symbols and the parsed bounds.
        window: The call's window.
        core: The session's core.
        files: The call's flat-file transport.
        dates: The partition dates of the files the window needs.
        call: The call id.
        logged: The facts the call has logged (`FlatFileHistory.history`).
        tally: The call's count of its rows and of those withheld, which the files add to.
        stop: A read's stop token, or None.
        problem: Why REST cannot serve the request (`rest_history_problem`), or None.
        forced: True for `source=FLAT_FILES`.

    Returns:
        The files' read, or None when S3 refused them and REST serves the whole call.

    Raises:
        AuthenticationError: When S3 rejects the credentials and REST may not serve the call.
        EntitlementError: The refusal of `_refused_flat_files`, when REST may not serve the
            call.

    Any other error is the flat-file transport's (`FlatFileHistory.history`).
    """
    infer = forced or problem is not None
    try:
        return files.history(
            spec,
            request,
            window,
            dates=dates,
            call=call,
            infer_holidays=infer,
            logged=logged,
            tally=tally,
            stop=stop,
        )
    except (EntitlementError, AuthenticationError) as error:
        if infer:
            if isinstance(error, EntitlementError):
                refusal = _refused_flat_files(spec, error, request)
                raise refusal from error
            raise
        _warn_rest_served(core, spec, f"S3 refused the flat files ({error})")
        return None


def _check_published(
    spec: DatasetSpec, read: FlatFileRead, *, problem: str | None, forced: bool
) -> None:
    """Refuse a read whose files are missing where REST may not serve the days.

    Args:
        spec: The dataset.
        read: The files' read.
        problem: Why REST cannot serve the request, or None.
        forced: True for `source=FLAT_FILES`.

    Raises:
        MassiveError: The errors `read_history` lists for an absent or unpublished file.
    """
    infer = forced or problem is not None
    if read.absent:
        days = ", ".join(day.isoformat() for day in read.absent)
        remedy = (
            "read REST with source=dm.HistorySource.REST, or end the range before them"
            if problem is None
            else f"end the range before them; REST cannot serve this read: {problem}"
        )
        raise MassiveError(
            f"{spec.name}: no flat file exists for {days}, nor for any later day up to "
            f"a week after the range; Massive publishes a day's file the next day, so "
            f"these files will not appear. To read them, {remedy}"
        )
    if read.unread and infer:
        first = read.unread[0].isoformat()
        remedy = (
            "Retry later, or read REST with source=dm.HistorySource.REST"
            if problem is None
            else f"Retry later; REST cannot serve this read: {problem}"
        )
        raise MassiveError(
            f"{spec.name}: the flat file for {first} is not published yet. "
            f"{PUBLICATION_NOTE}. {remedy}",
            is_retryable=True,
        )


def _files_lack_note(spec: DatasetSpec, read: FlatFileRead) -> str | None:
    """Return the description's sentences on what the flat files may lack, or None.

    Args:
        spec: The dataset.
        read: The flat-file read.

    Returns:
        The sentence on the days taken for holidays (`_skipped_days`), the one on the sessions
        that may be incomplete (`_uncertain_sessions`), then the one on the futures venues
        skipped because S3 refused them (`refused_venues.refused_note`), joined by spaces;
        None when none applies.
    """
    notes = [
        _skipped_days(read.skipped),
        _uncertain_sessions(spec, read.uncertain),
        refused_note(read.refused),
    ]
    return " ".join(note for note in notes if note is not None) or None


def _skipped_days(days: Sequence[date]) -> str | None:
    """Name the days a flat-file read took for holidays, for the table's description.

    A trading day's missing file with a later day's file after it is taken for a day without
    trading and skipped (`FlatFileRead.skipped`); the read has logged one WARNING per file.
    The inference may be wrong, so the table says which days hold no rows and why.

    Args:
        days: The skipped days, oldest first; empty for none.

    Returns:
        The description's sentence, or None when no day was skipped.
    """
    if not days:
        return None
    named = ", ".join(day.isoformat() for day in days)
    one = len(days) == 1
    return (
        f"No flat file exists for {named}, though a later day's does, so "
        f"{'that day was' if one else 'those days were'} taken for "
        f"{'a day' if one else 'days'} without trading (a holiday?) and "
        f"{'has' if one else 'have'} no rows here."
    )


def _uncertain_sessions(spec: DatasetSpec, sessions: Sequence[date]) -> str | None:
    """Warn that a futures read by dates may lack rows of some requested sessions.

    A futures flat file held rows of a session other than its own day's, so rows of the range's
    first or last session may sit in the file before or after the range, which a read by dates
    does not open (`FlatFileRead.uncertain`). The read never mislabels a row; it may only miss
    some, and says so on the table and in the log, on every such call.

    Args:
        spec: The dataset.
        sessions: The sessions that may be incomplete, oldest first; empty for none.

    Returns:
        The table description's sentence naming them, or None when there are none.
    """
    if not sessions:
        return None
    named = ", ".join(day.isoformat() for day in sessions)
    logger.warning(
        "%s history: the flat files hold rows of sessions other than their own day's, so "
        "rows of session(s) %s may sit in a file outside the range and be missing; read a "
        "wider range of dates, or by datetimes, to include them",
        spec.name,
        named,
    )
    return (
        f"Session(s) {named} may be incomplete: Massive's flat files held rows of other "
        "sessions, so some of their rows may sit in a file outside the range."
    )


def _rest_day_reads(
    spec: DatasetSpec, request: Request, window: HistoryWindow, *, days: Sequence[date]
) -> list[tuple[Request, HistoryWindow, Table | None]]:
    """Plan the REST reads of the days the flat files could not serve, within the call's range.

    Bars of a day or longer are read with the call's own request and window, which keep a bar
    only when its whole window fits the range, and then only the days asked for are kept, by
    their `Date` (for futures, the session label): an index day bar ends at 01:00 New York the
    next day and a futures session bar starts before its session opens, so a window of the
    day's own instants would drop them. Every other dataset is read one day at a time, clipped
    to the range: a range of days reads the whole day, and a range of instants the day's
    instants inside it (for futures, its session's hours), which tile with the files' days.
    Each day's window is built for the call's instant (`HistoryWindow.now`), never the clock's,
    so a day read later keeps exactly the rows the call's own window would.

    Args:
        spec: The dataset.
        request: The request with static symbols and the parsed bounds.
        window: The call's window: whole days, or a range of instants.
        days: The days (for futures, sessions), oldest first.

    Returns:
        Each read's request and window, and the `Date` labels of the rows to keep from it as a
        static key table (a static table, not a formula, whose query scope would capture the
        caller's frame and with it the session), or None to keep every row.
    """
    timespan = request.timespan
    if spec.is_bars and timespan is not None and not intraday(timespan):
        labels = pa.table({"Date": [day.isoformat() for day in days]})
        return [(request, window, dh_arrow.to_table(labels))]
    reads: list[tuple[Request, HistoryWindow, Table | None]] = []
    for day in days:
        start: date | datetime = day
        end: date | datetime | None = day
        if not isinstance(window.span, DaySpan):
            opens = day_start(spec.asset_class, day)
            closes = day_start(spec.asset_class, day + _ONE_DAY)
            first = required_instance(request.start, datetime, f"{spec.name}'s history start")
            start = max(first, opens)
            if request.end is not None:
                end = min(required_instance(request.end, datetime, f"{spec.name}'s end"), closes)
            else:
                end = closes
        one = replace(request, start=start, end=end)
        reads.append((one, history_window(spec, one, start=start, end=end, now=window.now), None))
    return reads


def _rest_days(
    spec: DatasetSpec,
    request: Request,
    window: HistoryWindow,
    *,
    core: SessionCore,
    days: Sequence[date],
    call: int,
    tally: ReadTally,
    stop: threading.Event | None,
) -> list[Table]:
    """Read the days the flat files could not serve from REST (`_rest_day_reads`).

    Args:
        spec: The dataset.
        request: The request with static symbols and the parsed bounds.
        window: The call's window: whole days, or a range of instants.
        core: The session's core.
        days: The days (for futures, sessions), oldest first.
        call: The call id.
        tally: The call's count of its rows and of those withheld, which every REST read adds
            to; the caller checks it.
        stop: A background read's stop token, or None for a blocking read.

    Returns:
        The days' rows inside the range; none for a day without trading.
    """
    rest = core.rest_transport()
    with entered(core.exec_ctx):
        reads = _rest_day_reads(spec, request, window, days=days)
    tables = []
    for one, one_window, keep in reads:
        if keep is None:
            tables.append(rest.history(spec, one, one_window, call=call, tally=tally, stop=stop))
            continue
        # `days` limits an all-symbol read of bars to one grouped-daily request per missing
        # day instead of one per day of the range.
        rows = rest.history(spec, one, one_window, call=call, tally=tally, stop=stop, days=days)
        with entered(core.exec_ctx):
            tables.append(rows.where_in(keep, "Date"))
    return tables


def read_history_by_symbol(
    spec: DatasetSpec,
    request: Request,
    window: HistoryWindow,
    *,
    core: SessionCore,
    files: FlatFileScan,
    symbols: Sequence[str],
    dates: Sequence[date],
    call: int,
    logged: LoggedKeys,
    stop: threading.Event | None,
    forced: bool,
) -> dict[str, SymbolOutcome]:
    """Read `history()` from the flat files for each symbol, each with its own outcome.

    The files are scanned once for every symbol, by the rules of `read_history`, and their rows
    split by `Symbol` (`symbol_reads.split_by_symbol`). A failure of the scan (an absent or
    unpublished file where REST may not serve it, a refusal, a failed read) is every symbol's.
    The days the files lack are read from REST for each symbol (`RestTransport.
    history_by_symbol`, over day windows built for the call's instant), so a symbol fails
    alone when its own REST read fails, and a symbol that failed is not read for later days.
    When S3 refuses the files and REST may serve the call, the whole call is read from REST by
    symbol. When the request's `refused_venues` skips a refused futures venue instead, each
    symbol's source names it (`refused_venues.refused_source`), since the followed table's
    description was fixed before the read. An empty window reads nothing.

    Args:
        spec: The dataset.
        request: The normalized arguments, with the parsed bounds; its symbols are ignored.
        window: The call's window.
        core: The session's core.
        files: The call's flat-file transport.
        symbols: The canonical symbols; a repeated one is read once.
        dates: The partition dates of the files the window needs.
        call: The call id.
        logged: The facts the call has logged, kept across its reads, so a refused futures
            venue is logged once per call (`refused_venues.RefusedVenues.report`).
        stop: A read's stop token, or None.
        forced: True for `source=FLAT_FILES`, which never reads REST.

    Returns:
        Each symbol, in the order given, with its outcome.

    Errors are those of `symbol_reads.symbol_outcomes`, and the `AuthenticationError` of
    `RestTransport.history_by_symbol`. When the library could read none of the rows of the
    read's symbols and window whole, across its files and REST reads, each symbol the read gave
    rows fails with that `RowDataError` (`ReadTally.judge`).
    """
    ordered = tuple(dict.fromkeys(symbols))
    flat = TRANSPORT_LABELS[Transport.FLAT_FILES]
    if window.empty:
        return _no_rows(core, spec, symbols=ordered, call=call, source=flat)
    asked = replace(request, symbols=tuple(sorted(ordered)))
    problem = rest_history_problem(spec, asked)
    tally = ReadTally()
    try:
        read = _scan(
            spec,
            asked,
            window,
            core=core,
            files=files,
            dates=dates,
            call=call,
            logged=logged,
            tally=tally,
            stop=stop,
            problem=problem,
            forced=forced,
        )
        if read is not None:
            _check_published(spec, read, problem=problem, forced=forced)
    except MassiveError as error:
        shared = dict.fromkeys(ordered, error)
        return symbol_outcomes(
            core, spec, symbols=ordered, tables={}, failures=shared, call=call, source=flat
        )
    if read is None:
        tally = ReadTally()  # the files read before the refusal are discarded
        outcomes = core.rest_transport().history_by_symbol(
            spec, asked, window, symbols=ordered, call=call, tally=tally, stop=stop
        )
        return tally.judge(spec.name, outcomes)
    lacks = _files_lack_note(spec, read)
    if lacks is not None:
        logger.debug("call %d %s history by symbol: %s", call, spec.name, lacks)
    with entered(core.exec_ctx):
        files_rows = merge([read.empty, *(part for _, part in read.parts)])
    # A followed table's description is fixed before its reads, so each symbol's transport in
    # `m.symbol_reads()` names the venues the read skipped.
    flat = refused_source(flat, read.refused)
    tables = split_by_symbol(core, spec, files_rows, symbols=ordered, source=flat)
    if not read.unread:
        outcomes = symbol_outcomes(
            core, spec, symbols=ordered, tables=tables, failures={}, call=call, source=flat
        )
        return tally.judge(spec.name, outcomes)
    merged, failures = _rest_days_by_symbol(
        spec,
        asked,
        window,
        core=core,
        tables=tables,
        days=read.unread,
        call=call,
        tally=tally,
        stop=stop,
    )
    outcomes = symbol_outcomes(
        core,
        spec,
        symbols=ordered,
        tables=merged,
        failures=failures,
        call=call,
        source=f"{flat} and REST",
    )
    return tally.judge(spec.name, outcomes)


def _no_rows(
    core: SessionCore, spec: DatasetSpec, *, symbols: Sequence[str], call: int, source: str
) -> dict[str, SymbolOutcome]:
    """Give each symbol of a read whose window is empty no rows, with no request.

    Args:
        core: The session's core.
        spec: The dataset.
        symbols: The symbols, in order.
        call: The call id.
        source: Where the rows would have come from, for the outcomes.

    Returns:
        Each symbol with an empty table of the dataset's columns.
    """
    schema = core.dataset_schema(spec)
    with entered(core.exec_ctx):
        nothing = dataset_table(schema.arrow_schema().empty_table(), schema.date_columns)
    empty = dict.fromkeys(symbols, nothing)
    return symbol_outcomes(
        core, spec, symbols=symbols, tables=empty, failures={}, call=call, source=source
    )


def _rest_days_by_symbol(
    spec: DatasetSpec,
    request: Request,
    window: HistoryWindow,
    *,
    core: SessionCore,
    tables: Mapping[str, Table],
    days: Sequence[date],
    call: int,
    tally: ReadTally,
    stop: threading.Event | None,
) -> tuple[dict[str, Table], dict[str, MassiveError]]:
    """Read the days the flat files lack from REST for each symbol, and merge each symbol's rows.

    Logs the WARNING of `_warn_rest_served` naming the days. A symbol whose REST read of a day
    fails is not read for the later days.

    Args:
        spec: The dataset.
        request: The request with the symbols and the parsed bounds.
        window: The call's window.
        core: The session's core.
        tables: Each symbol's rows from the files.
        days: The days the files lack, oldest first.
        call: The call id.
        tally: The call's count of its rows and of those withheld, which every REST read adds
            to; the caller judges it.
        stop: A read's stop token, or None.

    Returns:
        Each symbol that did not fail with its rows from both sources in time order, and each
        symbol that failed with its error.
    """
    missing = bounded_list(day.isoformat() for day in days)
    _warn_rest_served(core, spec, f"the flat files are missing for these days: {missing}")
    rest = core.rest_transport()
    pieces = {symbol: [table] for symbol, table in tables.items()}
    failures: dict[str, MassiveError] = {}
    with entered(core.exec_ctx):
        reads = _rest_day_reads(spec, request, window, days=days)
    for one, one_window, keep in reads:
        left = [symbol for symbol in tables if symbol not in failures]
        outcomes = rest.history_by_symbol(
            spec, one, one_window, symbols=left, call=call, tally=tally, stop=stop
        )
        _gather(core, outcomes, keep=keep, pieces=pieces, failures=failures)
    with entered(core.exec_ctx):
        # The files' days and REST's tile, so sorting by the time column puts every row of a
        # symbol in time order across the sources.
        merged = {
            symbol: merge(parts).sort(time_column(spec.times))
            for symbol, parts in pieces.items()
            if symbol not in failures
        }
    return merged, failures


def _gather(
    core: SessionCore,
    outcomes: Mapping[str, SymbolOutcome],
    *,
    keep: Table | None,
    pieces: dict[str, list[Table]],
    failures: dict[str, MassiveError],
) -> None:
    """Add one REST day read's outcomes to each symbol's rows or failure.

    Args:
        core: The session's core.
        outcomes: The read's outcome of each symbol.
        keep: The `Date` labels of the rows to keep, or None to keep every row.
        pieces: Each symbol's tables so far, which receive the read's rows.
        failures: Each failed symbol's error, which receives the read's failures.
    """
    for symbol, outcome in outcomes.items():
        if isinstance(outcome, SymbolFailure):
            failures[symbol] = outcome.error
            continue
        rows = outcome.table
        if keep is not None:
            with entered(core.exec_ctx):
                rows = rows.where_in(keep, "Date")
        pieces[symbol].append(rows)


def _warn_rest_served(core: SessionCore, spec: DatasetSpec, why: str) -> None:
    """Warn that REST served part or all of an `AUTO` read meant for the flat files.

    Names why and the columns REST leaves null that the flat files fill, from the generated
    schemas (`Catalog.history_columns`), so a reader knows which rows may lack them. Logged on
    every such call: the days differ from call to call.

    Args:
        core: The session's core, whose catalog names the columns.
        spec: The dataset.
        why: Why REST served the read, such as `"the flat files are missing for these days:
            1 [2025-03-04]"`.
    """
    catalog = core.catalog
    lost = catalog.history_columns(spec, Transport.FLAT_FILES) - catalog.history_columns(
        spec, Transport.REST
    )
    columns = (
        f"; those rows leave {', '.join(sorted(lost))} null, which the flat files fill"
        if lost
        else ""
    )
    logger.warning("%s history: AUTO read REST because %s%s", spec.name, why, columns)


def _refused_flat_files(
    spec: DatasetSpec, error: EntitlementError, request: Request
) -> EntitlementError:
    """Name REST as the other transport when the plan refuses the flat files a read needs.

    Used under `FLAT_FILES`, and under `AUTO` when REST cannot serve the read either (then the
    refusal is returned as it is); `AUTO` otherwise reads REST instead.

    Args:
        spec: The dataset.
        error: The flat-file refusal, which names the files and Massive's text.
        request: The request, with its symbols read.

    Returns:
        The same error when REST cannot serve the request either; otherwise a new
        `EntitlementError` with the same status and request id whose message ends by naming
        `source=dm.HistorySource.REST`.
    """
    if rest_history_problem(spec, request) is not None:
        return error
    return EntitlementError(
        f"{error}; REST serves this read too: pass source=dm.HistorySource.REST to "
        "history() (it can fill other columns; columns() shows which)",
        status=error.status,
        request_id=error.request_id,
    )
