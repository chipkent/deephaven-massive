"""Reads by symbol: each symbol's rows from a `fetch()` or `history()` read, or its own failure.

A table that follows a live symbol table reads each symbol as it appears, and a problem confined
to one symbol must fail only that symbol, never the table. The transports' reads by symbol
(`RestEndpoints.fetch_by_symbol`, `RestEndpoints.history_by_symbol`, and
`FlatFileHistory.history_by_symbol`) return, for each symbol, its rows (`SymbolRows`) or its
failure (`SymbolFailure`, `_tables/transports.py`). This module holds what they share:

- **Planning** (`fetch_units`, `history_units`): the requests `fetch()` or `history()` makes
  for the symbols, each with its path. A dataset whose rows carry no `Symbol` column reads
  each symbol in a request of its own, so every row belongs to the one request that returned
  it; a dataset whose rows list their tickers gets its `Symbol` from them, so its requests are
  grouped as `fetch()` groups them. A parameter a path does not take, and a crypto ticker list
  that can't be read, fail their own symbols here, before any request. A crypto pair the list
  gives no split for is planned as a call never sent. A pair the list doesn't hold has no
  data, as an HTTP 404 means. A pair whose currencies can't make up its ticker is skipped, and
  its `m.symbol_reads()` row says why (`_with_skipped`).
- **Attribution** (`read_rest_by_symbol`): a request of one symbol gives all its rows to that
  symbol; a request of several (`ticker.any_of`) gives each its rows by their `Symbol`, and its
  failure is the failure of each of them. A row of such a request with a null `Symbol` (a row
  that lists none of the request's tickers) keeps it and goes with the request's first symbol,
  as a static read keeps it. A row whose `Symbol` names another symbol belongs to no symbol: it
  is left out, with one WARNING per dataset and path a session.
  A flat-file scan reads every symbol at once (`history_reads.read_history_by_symbol`): its
  rows are split by `Symbol` (`split_by_symbol`), and its failure is every symbol's; the days
  its files lack are read from REST symbol by symbol.
- **Reads by value** (`read_rest_by_value`): a table passed where a dataset takes one value (an
  option chain's underlying, a futures product code) is read value by value, each with the
  requests `fetch()` makes for that value alone, so every row belongs to the value whose
  request returned it, and one value's failure fails only that value.
- **Classes of failure** (the error's `is_retryable`): retryable when the cause may pass (a
  rate limit beyond the read's patience, a server error after the client's retries, a network
  failure, a symbol's later page that answered HTTP 404); otherwise the same read would fail
  again for this session's entitlements (a refusal of the plan, which an upgrade of the plan
  lifts; rows that break the schema; a crypto pair Massive does not list; a parameter the path
  does not take; any other HTTP 404, such as a path Massive retired; a defect of the library).
  HTTP 404 on a symbol's first request is no failure: the symbol has no data, and its table is
  empty.
- **Whole-read failures**: a rejected API key (`AuthenticationError`) and a session closed
  during the read would fail every symbol alike, so they raise.
- **Columns**: each symbol's table must have exactly the dataset's columns
  (`SessionCore.dataset_schema`), so tables of different symbols, and of reads made at
  different times, can be merged. A table that does not is a defect of the library: the symbol
  fails with an error that is not retryable, logged at ERROR.

Each symbol's outcome logs one DEBUG line (its rows and source); each failure is logged once
at the level its `is_bug` gives (`_log_limits.log_failure`): DEBUG for an expected one, ERROR for
a defect. Every function runs on the calling thread (a read's own thread), which may be any
thread but an update-graph thread holding no library lock; the REST requests fan out on the
reader's pool (`RestReader.read_each`).
"""

from __future__ import annotations

import functools
import logging
from dataclasses import replace
from typing import TYPE_CHECKING, TypeAlias

import pyarrow as pa
import pyarrow.compute as pc

from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._errors import (
    LIBRARY_BUG,
    NEW_SESSION,
    AuthenticationError,
    ErrorSource,
    MassiveError,
    PermanentError,
    SessionClosedError,
    library_bug,
    required,
    required_instance,
)
from deephaven_massive._ingest.crypto_tickers import recording_split
from deephaven_massive._ingest.rest_requests import (
    fetch_paths,
    history_calls,
    own_symbols,
    plan_calls,
)
from deephaven_massive._log_limits import bounded_list, log_failure
from deephaven_massive._massive import Transport
from deephaven_massive._tables.columns import col_defs
from deephaven_massive._tables.execution_context import entered, holds_update_graph_lock
from deephaven_massive._tables.transports import SymbolFailure, SymbolRows

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from deephaven import dtypes
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._ingest.rest import RestReader
    from deephaven_massive._ingest.rest_requests import Call, CryptoSplit
    from deephaven_massive._massive import Cancel, RestPath
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._tables.transports import SymbolOutcome

__all__ = [
    "RestUnit",
    "UnsupportedParameterError",
    "fetch_units",
    "history_units",
    "read_rest_by_symbol",
    "read_rest_by_value",
    "split_by_symbol",
    "symbol_outcomes",
]

logger = logging.getLogger(__name__)

RestUnit: TypeAlias = "tuple[RestPath, Call]"
"""One planned request of a read by symbol, with the path it reads."""


class UnsupportedParameterError(PermanentError):
    """A parameter the call set that a symbol's path does not take, in a read by symbol.

    A dataset with a path per symbol class can route one symbol to a path that lacks a
    parameter the call set (`adjusted=False` on the crypto indicators). That symbol's read
    fails with this error before any request, and the other symbols are read. The parameter is
    the caller's, so its source is `ErrorSource.USER`, and it is a bug.
    """

    default_source = ErrorSource.USER
    default_is_bug = True


_SYMBOL = "Symbol"
"""The column that names each row's symbol, by which a request of several symbols is split."""


def _ordered(symbols: Sequence[str]) -> tuple[str, ...]:
    """Return the symbols once each, in the order given.

    Args:
        symbols: The symbols, perhaps repeated.

    Returns:
        Each symbol once, at its first place.
    """
    return tuple(dict.fromkeys(symbols))


def _failure_recorder(
    reader: RestReader, failed: dict[str, MassiveError], cancel: Cancel | None
) -> CryptoSplit:
    """Return a crypto split that records a failed lookup instead of raising.

    A failed read of the crypto ticker list fails that pair alone, while the read goes on with
    the other symbols (`recording_split`). A pair the list gives no split for is no failure:
    it is planned as a call with no data, reported as HTTP 404 is (`RestReader.read_each`).

    Args:
        reader: The reader whose session's crypto ticker list splits each pair.
        failed: Receives each pair that cannot be split, with its error.
        cancel: The read's stop token, which the list's read listens to.

    Returns:
        The split.
    """
    pair = functools.partial(reader.crypto_pair, cancel=cancel)
    return recording_split(pair, failed)


def _refused(symbols: Sequence[str], error: ValueError) -> dict[str, MassiveError]:
    """Fail symbols whose path does not take a parameter the call set.

    Args:
        symbols: The symbols of the path.
        error: The planning error, which names the path and the parameter.

    Returns:
        Each symbol with one shared `UnsupportedParameterError`, whose message ends with the
        fix: leave the parameter out, or read these symbols in a call without it.
    """
    refusal = UnsupportedParameterError(
        f"{error}; leave that parameter out, or read these symbols in a call without it"
    )
    return dict.fromkeys(symbols, refusal)


def fetch_units(
    reader: RestReader,
    spec: DatasetSpec,
    request: Request,
    *,
    symbols: Sequence[str],
    columns: Sequence[str],
    cancel: Cancel | None,
) -> tuple[list[RestUnit], dict[str, MassiveError]]:
    """Plan the requests of `fetch()` for the symbols, path by path, for a read by symbol.

    The paths are those `fetch()` reads (`fetch_paths`), and their requests those it makes
    (`plan_calls`), except that a dataset whose rows carry no `Symbol` column plans each
    symbol alone. Makes no request, except that the first crypto
    pair a path takes as two currencies reads the session's crypto ticker list.

    Args:
        reader: The reader of the session's paths.
        spec: The dataset, with a fetch path.
        request: The normalized arguments; its symbols are ignored.
        symbols: The canonical symbols.
        columns: The dataset's columns in this session (`SessionCore.dataset_schema`).
        cancel: The read's stop token, which the crypto ticker list's read listens to.

    Returns:
        The planned requests in plan order, and the symbols that failed before any request
        with their errors: a crypto pair whose lookup failed, and every symbol of a path that
        does not take a parameter the call set (`UnsupportedParameterError`).
    """
    asked = tuple(sorted(_ordered(symbols)))
    alone = _SYMBOL not in columns
    groups = [
        (path, required_instance(own, tuple, f"the symbols of {path}"))
        for path, own in fetch_paths(spec, asked)
    ]
    if alone:
        groups = [(template, (symbol,)) for template, own in groups for symbol in own]
    failed: dict[str, MassiveError] = {}
    split = _failure_recorder(reader, failed, cancel)
    units: list[RestUnit] = []
    for template, own in groups:
        rest_path = reader.rest_path(template)
        try:
            calls = plan_calls(
                spec,
                rest_path,
                request,
                symbols=own,
                dates=(request.start, request.end),
                split=split,
            )
        except ValueError as error:
            failed.update(_refused(own, error))
            continue
        units.extend((rest_path, call) for call in calls if call.symbol not in failed)
    logger.debug(
        "%s fetch by symbol: %d symbols in %d requests (%s); failed before any request: %s",
        spec.name,
        len(asked),
        len(units),
        "one symbol a request" if alone else "as fetch() groups them",
        bounded_list(failed) if failed else "none",
    )
    return units, failed


def history_units(
    reader: RestReader,
    spec: DatasetSpec,
    request: Request,
    *,
    window: HistoryWindow,
    symbols: Sequence[str],
    cancel: Cancel | None,
) -> tuple[list[RestUnit], dict[str, MassiveError]]:
    """Plan the REST requests of `history()` for the symbols over a window, for a read by symbol.

    The requests are those `history()` makes on the history path (`history_calls`): one
    request series per symbol, and for futures named by session dates one per session and
    symbol. An empty window plans none. Makes no request, except that the first crypto pair
    a path takes as two currencies reads the session's crypto ticker list.

    Args:
        reader: The reader of the session's paths.
        spec: The dataset, with a REST history path.
        request: The normalized arguments, with the parsed bounds; its symbols are ignored.
        window: The call's window.
        symbols: The canonical symbols.
        cancel: The read's stop token, which the crypto ticker list's read listens to.

    Returns:
        The planned requests in plan order, and the symbols that failed before any request,
        as `fetch_units` returns them.
    """
    asked = tuple(sorted(_ordered(symbols)))
    failed: dict[str, MassiveError] = {}
    units: list[RestUnit] = []
    if not window.empty:
        rest_path = reader.rest_path(required(spec.history_path, f"{spec.name}'s history path"))
        try:
            calls = history_calls(
                spec,
                rest_path,
                replace(request, symbols=asked),
                window=window,
                split=_failure_recorder(reader, failed, cancel),
            )
        except ValueError as error:
            failed.update(_refused(asked, error))
            calls = []
        units = [(rest_path, call) for call in calls if call.symbol not in failed]
    logger.debug(
        "%s history by symbol: %d symbols in %d requests%s; failed before any request: %s",
        spec.name,
        len(asked),
        len(units),
        " (the window is empty)" if window.empty else "",
        bounded_list(failed) if failed else "none",
    )
    return units, failed


def read_rest_by_symbol(
    reader: RestReader,
    core: SessionCore,
    spec: DatasetSpec,
    *,
    request: Request,
    symbols: Sequence[str],
    units: Sequence[RestUnit],
    failed: Mapping[str, MassiveError],
    limit: int | None,
    window: HistoryWindow | None,
    current: bool,
    call: int,
    cancel: Cancel | None,
    to_table: Callable[[pa.Table], Table],
) -> dict[str, SymbolOutcome]:
    """Make a read by symbol's planned REST requests and return each symbol's outcome.

    Each request's rows go to the symbols it names (see the module docstring), and each
    symbol's rows become one static table, in plan order.

    Args:
        reader: The reader of the session's paths.
        core: The session's core: its schema, its execution context, and whether it closed.
        spec: The dataset.
        request: The normalized arguments.
        symbols: The canonical symbols, in the order the outcomes follow.
        units: The planned requests (`fetch_units`, `history_units`).
        failed: The symbols that failed before any request, with their errors.
        limit: The cap on each request's rows (`RestReader.read_each`).
        window: The `history()` window whose rows to keep, or None for `fetch()`.
        current: True when the pages are checked for a `DELAYED` label
            (`rest._fetch_is_current`).
        call: The verb call's id, for the log lines.
        cancel: The read's stop token, or None.
        to_table: Converts a symbol's Arrow rows to a static Deephaven table.

    Returns:
        Each symbol, in the order given, with its outcome.

    Errors are those of `symbol_outcomes`, and the `AuthenticationError` of
    `RestReader.read_each`.
    """
    ordered = _ordered(symbols)
    patience = reader.patience()  # once for every request, on this thread, before any fan-out
    results, requests = reader.read_each(
        spec,
        units,
        request=request,
        limit=limit,
        window=window,
        current=current,
        cancel=cancel,
        patience=patience,
    )
    logger.debug(
        "call %d %s by symbol: %d of %d requests answered, waiting out at most %.0f s of pauses",
        call,
        spec.name,
        requests,
        len(units),
        patience.seconds,
    )
    pieces: dict[str, list[pa.Table]] = {symbol: [] for symbol in ordered}
    failures = dict(failed)
    for (rest_path, planned), result in zip(units, results, strict=True):
        own = own_symbols(spec, planned.symbol, planned.query)
        if isinstance(result, MassiveError):
            for symbol in own:
                failures.setdefault(symbol, result)
            continue
        _attribute(reader, spec, result, template=rest_path.template, own=own, pieces=pieces)
    empty = core.dataset_schema(spec).arrow_schema().empty_table()
    tables = {
        symbol: to_table(pa.concat_tables(rows) if rows else empty)
        for symbol, rows in pieces.items()
        if symbol not in failures
    }
    outcomes = symbol_outcomes(
        core,
        spec,
        symbols=ordered,
        tables=tables,
        failures=failures,
        call=call,
        source=TRANSPORT_LABELS[Transport.REST],
    )
    return _with_skipped(outcomes, units)


def _with_skipped(
    outcomes: dict[str, SymbolOutcome], units: Sequence[RestUnit]
) -> dict[str, SymbolOutcome]:
    """Name, in each outcome it applies to, why a call left its crypto pair unread.

    A pair whose currencies Massive's crypto ticker list gives but can't be read as its ticker
    is planned as a call the reader never sends (`Call.unsplit`), so its read ends with no
    rows; `unreadable_symbols` already logged it. A stream and a `poll()` name such a pair in
    their `Error`, so a read by symbol names it in its `m.symbol_reads()` row. A pair Massive
    has no data for is not named: it is reported as HTTP 404 is.

    Args:
        outcomes: Each symbol's outcome (`symbol_outcomes`).
        units: The planned requests.

    Returns:
        The outcomes, each skipped pair's rows carrying why (`SymbolRows.skipped`).
    """
    for _, planned in units:
        unsplit, symbol = planned.unsplit, planned.symbol
        if unsplit is None or unsplit.no_data:
            continue
        assert symbol is not None  # a split is planned only for a call of one pair
        outcome = outcomes[symbol]
        assert isinstance(outcome, SymbolRows)  # an unsent call fails no symbol
        why = f"skipped: {unsplit.why}; the pair gets no rows. {LIBRARY_BUG}"
        outcomes[symbol] = replace(outcome, skipped=why)
    return outcomes


def read_rest_by_value(
    reader: RestReader,
    core: SessionCore,
    spec: DatasetSpec,
    *,
    request: Request,
    values: Sequence[str],
    call: int,
    cancel: Cancel | None,
    to_table: Callable[[pa.Table], Table],
) -> dict[str, SymbolOutcome]:
    """Read `fetch()` for each value of a value table, one value after another.

    Each value is read with the requests `fetch()` makes when that value is passed alone
    (`Request.for_value`, `RestReader.read_fetch`), so all of a request's rows belong to its
    value and `Request.limit` caps each value's requests as it would that call's. A value whose
    read fails gets its own failure; a parameter its path does not take fails it with
    `UnsupportedParameterError`. A rejected API key stops the read, since every value would
    be refused.

    Args:
        reader: The reader of the session's paths.
        core: The session's core: its schema, its execution context, and whether it closed.
        spec: The dataset, with a fetch path.
        request: The normalized arguments, with a value table and static symbols.
        values: The values, already checked; a repeated one is read once.
        call: The verb call's id, for the log lines.
        cancel: The read's stop token, or None.
        to_table: Converts a value's Arrow rows to a static Deephaven table.

    Returns:
        Each value, in the order given, with its outcome.

    Errors are those of `symbol_outcomes`.
    """
    ordered = _ordered(values)
    pauses = holds_update_graph_lock(core.exec_ctx)
    tables: dict[str, Table] = {}
    failures: dict[str, MassiveError] = {}
    for value in ordered:
        try:
            rows, requests = reader.read_fetch(
                spec,
                request.for_value(value),
                symbols=request.static_symbols,
                cancel=cancel,
                pauses_live_tables=pauses,
            )
        except MassiveError as error:
            failures[value] = error
            if isinstance(error, AuthenticationError):
                break  # every later value would be refused too
            continue
        except ValueError as error:
            failures.update(_refused((value,), error))
            continue
        logger.debug(
            "call %d %s by value: %s read in %d requests", call, spec.name, value, requests
        )
        tables[value] = to_table(rows)
    return symbol_outcomes(
        core,
        spec,
        symbols=ordered,
        tables=tables,
        failures=failures,
        call=call,
        source=TRANSPORT_LABELS[Transport.REST],
    )


def _attribute(
    reader: RestReader,
    spec: DatasetSpec,
    rows: pa.Table,
    *,
    template: str,
    own: Sequence[str],
    pieces: dict[str, list[pa.Table]],
) -> None:
    """Give one request's rows to the symbols it names.

    A request of one symbol gives that symbol all its rows. A request of several gives each its
    rows by their `Symbol` (a row that lists none of the request's tickers never reaches here:
    `RestReader._listed` leaves it out). A row whose `Symbol` names another symbol (or any row
    of a request that names no symbol) belongs to no symbol and is left out, with one WARNING
    per dataset and path a session.

    Args:
        reader: The reader, whose session remembers the warnings already given.
        spec: The dataset.
        template: The request's path.
        own: The symbols the request names (`own_symbols`).
        rows: The request's rows.
        pieces: Each symbol's rows so far, which receive the request's.
    """
    if len(own) == 1:
        pieces[own[0]].append(rows)
        return
    kept = 0
    for symbol in own:
        mine = rows.filter(pc.equal(rows[_SYMBOL], symbol))
        pieces[symbol].append(mine)
        kept += mine.num_rows
    left = rows.num_rows - kept
    if not left:
        return
    level = logging.WARNING if reader.state.first("unowned", spec.name, template) else logging.DEBUG
    logger.log(
        level,
        "%s: %d of the %d rows Massive returned from %s for %s name none of those symbols, so "
        "no symbol's read owns them and they are left out",
        spec.name,
        left,
        rows.num_rows,
        template,
        bounded_list(own) if own else "no symbol",
    )


def split_by_symbol(
    core: SessionCore, spec: DatasetSpec, table: Table, *, symbols: Sequence[str], source: str
) -> dict[str, Table]:
    """Split one read's static rows of several symbols into each symbol's table.

    A row whose `Symbol` names none of the symbols, or is null, belongs to no symbol: the rows
    the split leaves out are counted against the table's size and logged at WARNING, so no row
    Massive sent disappears without a trace.

    Args:
        core: The session's core, whose execution context the split runs in.
        spec: The dataset.
        table: The rows, a static table with a `Symbol` column.
        symbols: The symbols, in the order the result follows.
        source: Where the rows came from, for the log line, such as `"flat files"`.

    Returns:
        Each symbol with its rows; a symbol without rows has an empty table with the same
        columns.
    """
    with entered(core.exec_ctx):
        parts = table.partition_by(_SYMBOL)
        none = table.head(0)
        found = {symbol: parts.get_constituent(symbol) for symbol in symbols}
        tables = {symbol: none if part is None else part for symbol, part in found.items()}
        total = table.size
        kept = sum(part.size for part in tables.values())
    if kept != total:
        logger.warning(
            "%s: %d of the %d rows read from %s for %s name none of those symbols (or no "
            "symbol), so no symbol's read owns them and they are left out",
            spec.name,
            total - kept,
            total,
            source,
            bounded_list(symbols),
        )
    return tables


def symbol_outcomes(
    core: SessionCore,
    spec: DatasetSpec,
    *,
    symbols: Sequence[str],
    tables: Mapping[str, Table],
    failures: Mapping[str, MassiveError],
    call: int,
    source: str,
) -> dict[str, SymbolOutcome]:
    """Turn each symbol's table or error into its outcome, and log each one.

    The last step of every read by symbol, REST or flat files.

    Args:
        core: The session's core.
        spec: The dataset.
        symbols: The symbols, in the order the outcomes follow.
        tables: The table of each symbol that did not fail.
        failures: The error of each symbol that failed.
        call: The verb call's id.
        source: Where the rows came from, for the log lines: `"REST"`, `"flat files"`, or
            `"flat files and REST"` when REST served the days the files lack.

    Returns:
        Each symbol with its `SymbolRows` (which carry `source`), or its `SymbolFailure`;
        a table without exactly the dataset's columns fails its symbol as a library defect.

    Raises:
        AuthenticationError: When any symbol failed with it: every symbol would.
        SessionClosedError: `"<session> is closed"` when the session closed during the read.
    """
    refused = next((e for e in failures.values() if isinstance(e, AuthenticationError)), None)
    if refused is not None:
        logger.debug(
            "call %d %s by symbol: the credentials were rejected: %s", call, spec.name, refused
        )
        raise refused
    if core.closed:
        raise SessionClosedError(f"{core.label} is closed; {NEW_SESSION}")
    expected = col_defs(core.dataset_schema(spec))
    outcomes: dict[str, SymbolOutcome] = {}
    for symbol in symbols:
        error = failures.get(symbol)
        if error is None:
            table = tables[symbol]
            problem = _column_problem(table, expected)
            if problem is None:
                outcomes[symbol] = SymbolRows(table, source=source)
                logger.debug(
                    "call %d %s %s: rows read from %s: %d",
                    call,
                    spec.name,
                    symbol,
                    source,
                    table.size,
                )
                continue
            error = MassiveError(
                library_bug(f"{spec.name}: {symbol}'s rows from {source} {problem}"),
                source=ErrorSource.LIBRARY,
            )
        outcomes[symbol] = SymbolFailure(error)
        log_failure(
            logger,
            error,
            "call %d %s %s: the read from %s failed (%s), which fails this symbol alone: %s",
            call,
            spec.name,
            symbol,
            source,
            "retryable" if error.is_retryable else "not retryable",
            expected=logging.DEBUG,
        )
    return outcomes


def _column_problem(table: Table, expected: Mapping[str, dtypes.DType]) -> str | None:
    """Say how a table's columns differ from the dataset's, if they do.

    Args:
        table: A symbol's table.
        expected: The dataset's columns and their types (`columns.col_defs`).

    Returns:
        Such as `"lack Price; have Extra, which the dataset does not; have Size as
        java.lang.String, not long"`, or None when the names and types match, in any order.
    """
    have = {column.name: column.data_type.j_name for column in table.columns}
    missing = [name for name in expected if name not in have]
    extra = [name for name in have if name not in expected]
    wrong = [
        f"{name} as {have[name]}, not {kind.j_name}"
        for name, kind in expected.items()
        if name in have and have[name] != kind.j_name
    ]
    problems = [
        *([f"lack {', '.join(missing)}"] if missing else []),
        *([f"have {', '.join(extra)}, which the dataset does not"] if extra else []),
        *([f"have {', '.join(wrong)}"] if wrong else []),
    ]
    return "; ".join(problems) if problems else None
