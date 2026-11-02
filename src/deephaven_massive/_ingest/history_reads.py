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
  names REST when REST could serve the read.
- A futures read by dates whose files held rows of other sessions warns that the range's
  first or last session may be incomplete, and says so on the table.

Runs on the calling thread (a background read's own thread for `wait=False`); the REST reads go
through the session's REST transport (`SessionCore.rest_transport`).
"""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import date, datetime, timedelta
from typing import TYPE_CHECKING, Final, cast

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
from deephaven_massive._errors import AuthenticationError, EntitlementError, MassiveError
from deephaven_massive._log_limits import bounded_list
from deephaven_massive._massive import Transport
from deephaven_massive._tables.execution_context import entered

if TYPE_CHECKING:
    import threading
    from collections.abc import Sequence

    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._ingest.flatfiles import FlatFileHistory
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._tables.transports import FlatFileRead

__all__ = ["read_history"]

logger = logging.getLogger(__name__)

_ONE_DAY: Final = timedelta(days=1)


def read_history(
    spec: DatasetSpec,
    request: Request,
    window: HistoryWindow,
    *,
    core: SessionCore,
    files: FlatFileHistory,
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
        the days taken for holidays, the sessions that may be incomplete), or None.

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
    """
    problem = rest_history_problem(spec, request)
    infer = forced or problem is not None
    try:
        read = files.history(
            spec, request, window, dates=dates, call=call, infer_holidays=infer, stop=stop
        )
    except (EntitlementError, AuthenticationError) as error:
        if infer:
            if isinstance(error, EntitlementError):
                refusal = _refused_flat_files(spec, error, request)
                raise refusal from error
            raise
        _warn_rest_served(core, spec, f"S3 refused the flat files ({error})")
        rows = core.rest_transport().history(spec, request, window, call=call, stop=stop)
        return rows, "Read from REST: AUTO chose flat files, which S3 refused.", None
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
            f"{PUBLICATION_NOTE}. {remedy}"
        )
    lacks = _files_lack_note(spec, read)
    if not read.unread:
        with entered(core.exec_ctx):
            return merge([read.empty, *(part for _, part in read.parts)]), None, lacks
    rest = _rest_days(spec, request, window, core=core, days=read.unread, call=call, stop=stop)
    with entered(core.exec_ctx):
        # The files' days and REST's tile (the files are trimmed to their own days), so
        # sorting by the time column puts every row in time order across the sources.
        merged = merge([read.empty, *(part for _, part in read.parts), *rest])
        table = merged.sort(time_column(spec.times))
    days = ", ".join(day.isoformat() for day in read.unread)
    missing = bounded_list(day.isoformat() for day in read.unread)
    _warn_rest_served(core, spec, f"the flat files are missing for these days: {missing}")
    flat = TRANSPORT_LABELS[Transport.FLAT_FILES]
    served = f"Read from {flat}, and from REST for {days}, whose files are missing."
    return table, served, lacks


def _files_lack_note(spec: DatasetSpec, read: FlatFileRead) -> str | None:
    """Return the description's sentences on what the flat files may lack, or None.

    Args:
        spec: The dataset.
        read: The flat-file read.

    Returns:
        The sentence on the days taken for holidays (`_skipped_days`), then the one on the
        sessions that may be incomplete (`_uncertain_sessions`), joined by a space; None when
        neither applies.
    """
    notes = [_skipped_days(read.skipped), _uncertain_sessions(spec, read.uncertain)]
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


def _rest_days(
    spec: DatasetSpec,
    request: Request,
    window: HistoryWindow,
    *,
    core: SessionCore,
    days: Sequence[date],
    call: int,
    stop: threading.Event | None,
) -> list[Table]:
    """Read the days the flat files could not serve from REST, each within the call's range.

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
        core: The session's core.
        days: The days (for futures, sessions), oldest first.
        call: The call id.
        stop: A background read's stop token, or None for a blocking read.

    Returns:
        The days' rows inside the range; none for a day without trading.
    """
    rest = core.rest_transport()
    timespan = request.timespan
    if spec.is_bars and timespan is not None and not intraday(timespan):
        # The window keeps whole bars; `days` limits an all-symbol read to one grouped-daily
        # request per missing day instead of one per day of the range.
        rows = rest.history(spec, request, window, call=call, stop=stop, days=days)
        # A static key table, not a formula: a formula's query scope would capture this
        # frame, and with it the session.
        labels = pa.table({"Date": [day.isoformat() for day in days]})
        with entered(core.exec_ctx):
            return [rows.where_in(dh_arrow.to_table(labels), "Date")]
    tables = []
    for day in days:
        start: date | datetime = day
        end: date | datetime | None = day
        if not isinstance(window.span, DaySpan):
            opens = day_start(spec.asset_class, day)
            closes = day_start(spec.asset_class, day + _ONE_DAY)
            start = max(cast("datetime", request.start), opens)
            end = closes if request.end is None else min(cast("datetime", request.end), closes)
        one = replace(request, start=start, end=end)
        one_window = history_window(spec, one, start=start, end=end, now=window.now)
        tables.append(rest.history(spec, one, one_window, call=call, stop=stop))
    return tables


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
