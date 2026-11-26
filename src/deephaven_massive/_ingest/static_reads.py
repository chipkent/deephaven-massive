"""A static `fetch()` or REST `history()` of symbols in code: one table, each failure its own.

The read makes the requests a read by symbol makes (`RestReader.read_each`) and gives each
request's rows to the symbols it named (`symbol_reads.attribute_units`), so one symbol's failure
never takes another's rows: its read goes on, and the failed symbol is left out of the table
and listed in `m.problems()`:

- **Massive refused it** (`symbol_retry.is_symbol_refusal`): a `refused_symbols` problem, whose
  setting decides only whether the call raises.
- **Any other failure of its own** (a 5xx after the client's retries, a timeout, data cut
  short, a crypto pair whose lookup failed): a `symbol_read_failed` problem. A static call
  has no later read to try again, so the failure is final for that call.

Both are spans of the call that end when it returns. The call raises instead of returning:

- when every symbol it named failed (`every_symbol_failed`): no symbol got an answer. Each
  is listed first, since the error gives only the first symbol's reason;
- for a failure of the whole read (`symbol_reads.fails_followed_read`: a rejected key, a closed
  session), which the reader raises at once;
- for a failure once the read's stop token is set: its table is gone, so the failure is the
  read's, not its symbol's, and no symbol is listed;
- for a refusal of the feed (`FeedRefusalError`: a `DELAYED` label on the subscription the
  request is charged to), which no symbol escapes;
- for a failure under a cause set to `RAISE`: `refused_symbols`, `unavailable_symbols`
  (`UnavailableSymbolError`), and a withheld or unrequested row's cause (`RowDataError`), after
  every failed symbol is listed.

A read whose every row was withheld fails at its entry point (`ReadTally.check`).
"""

from __future__ import annotations

import functools
import logging
import time
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._errors import (
    FeedRefusalError,
    RowDataError,
    UnavailableSymbolError,
    every_symbol_failed,
    failure_text,
)
from deephaven_massive._ingest.rest_requests import capped_call, caps_each_symbol
from deephaven_massive._ingest.symbol_reads import attribute_units, fails_followed_read
from deephaven_massive._log_limits import bounded_list
from deephaven_massive._on_error import effect_of
from deephaven_massive._tables.problem_events import ProblemCause, ProblemSpans, SpanTracker
from deephaven_massive._tables.symbol_retry import is_symbol_refusal

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._errors import MassiveError
    from deephaven_massive._ingest.rest import RestReader
    from deephaven_massive._ingest.symbol_reads import ReadEach, RequestRows, RestUnit
    from deephaven_massive._massive import Cancel, Patience
    from deephaven_massive._tables.read_tally import ReadTally

__all__ = ["read_symbols"]

logger = logging.getLogger(__name__)


def read_symbols(
    reader: RestReader,
    spec: DatasetSpec,
    request: Request,
    *,
    symbols: Sequence[str],
    units: Sequence[RestUnit],
    failed: Mapping[str, MassiveError],
    window: HistoryWindow | None,
    current: bool,
    limit: int | None,
    cancel: Cancel | None,
    patience: Patience,
    tally: ReadTally,
) -> tuple[pa.Table, int]:
    """Make a static read's planned requests and return the rows of every symbol that did not fail.

    `limit` caps each symbol's rows where every request names one symbol; otherwise it caps
    the total, and the requests are sent one at a time, each asking only for the rows still
    wanted, until Massive has sent `limit` rows (`_send_capped`).

    Args:
        reader: The reader of the session's paths, with the call's policy and stop token.
        spec: The dataset.
        request: The normalized arguments.
        symbols: The canonical symbols the call named.
        units: The planned requests (`symbol_reads.fetch_units`, `history_units`).
        failed: The symbols that failed before any request, with their errors.
        window: The `history()` window whose rows to keep, or None for `fetch()`.
        current: True when the pages are checked for a `DELAYED` label.
        limit: `Request.limit` for `fetch()`, None for `history()`.
        cancel: The read's stop token, or None.
        patience: The patience of the whole read (`RestReader.patience`), shared by every
            request.
        tally: The whole read's count of its rows and of those withheld, which the read's
            entry point checks (`ReadTally.check`).

    Returns:
        The rows of every symbol that did not fail, an Arrow table of the dataset's schema in
        plan order and then page order, a failed symbol's rows from its other requests left
        out too, and the number of requests Massive answered.

    Raises:
        MassiveError: As the module docstring lists.
    """
    ordered = tuple(dict.fromkeys(symbols))
    read = functools.partial(
        reader.read_each,
        spec,
        request=request,
        window=window,
        current=current,
        cancel=cancel,
        patience=patience,
        fails_read=functools.partial(_fails_static_read, cancel=cancel),
    )
    per_symbol = limit is None or caps_each_symbol(spec.name, [call for _, call in units])
    if per_symbol:
        leaves, results, requests = read(units, limit=limit)[:3]
    else:
        assert limit is not None  # a total cap is the only other case
        leaves, results, requests = _send_capped(spec, units, read, limit=limit)
    attributed = attribute_units(
        reader, spec, leaves, results=results, symbols=ordered, tally=tally
    )
    failures = {**attributed.failures, **failed}  # a planning failure comes first
    if failures and cancel is not None and cancel.is_set():
        # A crypto pair's lookup the stop ended: the read is over, not the pair.
        stopped = next(iter(failures.values()))
        raise stopped
    _report_failures(reader, spec, symbols=ordered, failures=failures)
    if ordered and all(symbol in failures for symbol in ordered):
        logger.debug("%s: every symbol failed: %s", spec.name, bounded_list(ordered))
        error = every_symbol_failed([failures[symbol] for symbol in ordered])
        raise error from failures[ordered[0]]
    gone = pa.array(list(failures), pa.string())
    pieces = [
        rows.filter(pc.invert(pc.is_in(rows[column], value_set=gone))) if failures else rows
        for column, rows in attributed.answered
    ]
    schema = reader.catalog.dataset_schema(spec).arrow_schema()
    rows = pa.concat_tables(pieces) if pieces else schema.empty_table()
    if not per_symbol:
        # Rows listed under several named tickers can outnumber the rows Massive capped.
        rows = rows.slice(0, limit)
    logger.debug(
        "%s: %d rows of %d symbols from %d requests; %d symbols failed",
        spec.name,
        rows.num_rows,
        len(ordered),
        requests,
        len(failures),
    )
    return rows, requests


def _fails_static_read(error: MassiveError, *, cancel: Cancel | None) -> bool:
    """Tell whether a request's failure fails a static read whole.

    Those of a followed read (`fails_followed_read`) do, and so does any failure once the
    read's stop token is set: the read's table is gone, and the failure is the stop's.

    Args:
        error: The request's failure.
        cancel: The read's stop token, or None.

    Returns:
        True for a failure of the whole read.
    """
    return fails_followed_read(error) or (cancel is not None and cancel.is_set())


def _send_capped(
    spec: DatasetSpec,
    units: Sequence[RestUnit],
    read: functools.partial[ReadEach],
    *,
    limit: int,
) -> tuple[list[RestUnit], list[RequestRows | MassiveError | None], int]:
    """Send the requests one at a time until Massive has sent `limit` rows: a capped read.

    `limit` caps a call's total rows, and Massive stops sending them only per request, so each
    request asks only for the rows still wanted (`capped_call`), and none is sent once
    Massive has sent `limit` rows. The count is of the rows Massive sent, before any was
    withheld or listed under its tickers. One DEBUG line names the requests left unsent; their
    symbols get no rows, and no failure.

    Args:
        spec: The dataset.
        units: The planned requests; none names one symbol.
        read: Reads requests (`RestReader.read_each` bound to the read's arguments), given
            the requests and the cap on each one's rows as `limit`.
        limit: The cap on the read's rows.

    Returns:
        The requests answered, each one's result, and the request count, as `ReadEach` gives
        them.
    """
    leaves: list[RestUnit] = []
    results: list[RequestRows | MassiveError | None] = []
    requests = 0
    remaining = limit
    for index, (rest_path, call) in enumerate(units):
        if remaining <= 0:
            logger.debug(
                "%s: limit=%d rows arrived after %d of %d planned requests; the other %d are "
                "not sent",
                spec.name,
                limit,
                index,
                len(units),
                len(units) - index,
            )
            break
        one = read(
            [(rest_path, capped_call(spec, rest_path, call, cap=remaining))], limit=remaining
        )
        leaves.extend(one.units)
        results.extend(one.results)
        requests += one.requests
        remaining -= one.received
    return leaves, results, requests


def _report_failures(
    reader: RestReader,
    spec: DatasetSpec,
    *,
    symbols: Sequence[str],
    failures: Mapping[str, MassiveError],
) -> None:
    """List each failed symbol in `m.problems()`, and raise a failure the call must not survive.

    A refused symbol is a `refused_symbols` span; any other failure of its own, but one that
    raises, a `symbol_read_failed` span. Each span starts and ends now: it is the call's, and
    the call is over. A symbol Massive has no data for was listed when its request was
    answered (`RestReader.read_each`), and a withheld or unrequested row when its page was.

    Args:
        reader: The reader, whose problem sink, policy, and call id the spans take.
        spec: The dataset.
        symbols: The call's symbols, in the order the spans follow.
        failures: Each failed symbol's error.

    Raises:
        MassiveError: Once every failed symbol is listed, the first symbol's failure, in the
            call's order, that the call must not survive: a refusal of the feed
            (`FeedRefusalError`), a refused symbol under `refused_symbols=RAISE`, an
            `UnavailableSymbolError` (only under `unavailable_symbols=RAISE`), or a
            `RowDataError` (only under its cause's `RAISE`).
    """
    refused_fails = effect_of(reader.policy.refused_symbols).fails
    spans: dict[ProblemCause, dict[str, tuple[str | None, str | None, str, int]]] = {
        ProblemCause.REFUSED_SYMBOLS: {},
        ProblemCause.SYMBOL_READ_FAILED: {},
    }
    fatal: list[MassiveError] = []
    for symbol in symbols:
        error = failures.get(symbol)
        if error is None:
            continue
        if isinstance(error, (FeedRefusalError, UnavailableSymbolError, RowDataError)):
            fatal.append(error)
            continue
        refused = is_symbol_refusal(error)
        cause = ProblemCause.REFUSED_SYMBOLS if refused else ProblemCause.SYMBOL_READ_FAILED
        spans[cause][symbol] = (symbol, None, failure_text(error), 0)
        if refused and refused_fails:
            fatal.append(error)
    at = time.time_ns()
    found: list[ProblemSpans] = []
    for cause, held in spans.items():
        if not held:
            continue
        policy = reader.policy.refused_symbols if cause is ProblemCause.REFUSED_SYMBOLS else None
        tracker = SpanTracker(
            cause, dataset=spec.name, stream_id=reader.stream_id, where=spec.name, policy=policy
        )
        found.extend(
            snapshot
            for snapshot in (tracker.update(held, at=at), tracker.update({}, at=at))
            if snapshot is not None
        )
    if found:
        reader.problems(found)
    if fatal:
        logger.debug("%s: the read fails for %s", spec.name, fatal[0])
        raise fatal[0]
