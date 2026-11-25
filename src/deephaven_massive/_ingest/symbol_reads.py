"""Reads by symbol: each symbol's rows from a `fetch()` or `history()` read, or its own failure.

A table that follows a live symbol table reads each symbol as it appears, and a problem confined
to one symbol must fail only that symbol, never the table. The transports' reads by symbol
(`RestEndpoints.fetch_by_symbol`, `RestEndpoints.history_by_symbol`, and
`FlatFileHistory.history_by_symbol`) return, for each symbol, its rows (`SymbolRows`) or its
failure (`SymbolFailure`, `_tables/transports.py`). This module holds what they share:

- **Planning** (`fetch_units`, `history_units`): the requests `fetch()` or `history()` makes
  for the symbols, each with its path, grouped as the verb groups them. A crypto ticker list
  that can't be read, and, for `history()`, a parameter its path does not take, fail their own
  symbols here, before any request; a `fetch()` refused every such parameter at the call. A
  crypto pair the list gives no split for is planned as a call never sent. A pair the list
  doesn't hold has no data, as an HTTP 404 means. A pair whose currencies can't make up its
  ticker is skipped, and its `m.symbol_reads()` row says why (`_with_skipped`).
- **Attribution** (`read_rest_by_symbol`, `_attribute`): one rule for every request, whatever
  its form (a symbol's own path, a one-value parameter, a list parameter) or how many symbols
  it names. Each row goes to the asked symbol that its asked-symbol column names
  (`asked_column`): `Symbol`, or `Underlying` where the path fills its fan-out key from the
  request (`reference.related_companies`, whose `Symbol` is the peer). A row naming no asked
  symbol, another symbol or a null, belongs to no symbol: it is left out as an
  `unrequested_symbols` problem, one event per symbol such rows name, by the symbol as
  Massive sent it (`ProblemEvent`; `SPX` for a row the table would name `I:SPX`), which
  `m.problems()` lists and the session's reporter alarms once per table; under `RAISE` the
  request's symbols fail (`RowDataError`). A request's failure is the failure of each symbol
  it names. A dataset whose rows list their tickers fills `Symbol` with the asked ticker each
  row lists before this step (`rest._listed_rows`), so its rows follow the same rule. A
  flat-file scan reads every symbol at once (`history_reads.read_history_by_symbol`): its rows
  are split by `Symbol` (`split_by_symbol`) under the same rule, and its failure is every
  symbol's; the days its files lack are read from REST symbol by symbol.
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
import time
from collections import Counter
from dataclasses import replace
from typing import TYPE_CHECKING, TypeAlias

import pyarrow as pa
import pyarrow.compute as pc
from deephaven import arrow as dh_arrow

from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._datasets.source_columns import key_column
from deephaven_massive._enums import OnError
from deephaven_massive._error_policy import policy_hint
from deephaven_massive._errors import (
    AuthenticationError,
    ErrorSource,
    MassiveError,
    PermanentError,
    RowDataError,
    SessionClosedError,
    library_bug,
    required,
    required_instance,
    session_closed,
)
from deephaven_massive._ingest.crypto_tickers import recording_split
from deephaven_massive._ingest.rest_requests import (
    fetch_paths,
    history_calls,
    plan_calls,
)
from deephaven_massive._log_limits import bounded_list, counted, log_failure, prose_list
from deephaven_massive._massive import Transport
from deephaven_massive._on_error import effect_of
from deephaven_massive._tables.columns import col_defs
from deephaven_massive._tables.execution_context import entered, holds_update_graph_lock
from deephaven_massive._tables.problem_events import ProblemAction, ProblemCause, ProblemEvent
from deephaven_massive._tables.read_tally import ReadTally
from deephaven_massive._tables.transports import SymbolFailure, SymbolRows
from deephaven_massive._tables.withheld import SYMBOL_SENT

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

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
    "asked_column",
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

    A `history()` or value-table read by symbol can meet a path that lacks a parameter the
    call set when the call did not check it. That symbol's read fails with this error before
    any request, and the other symbols are read. The parameter is
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
        f"{error} Leave that argument out, or read these symbols in a call without it."
    )
    return dict.fromkeys(symbols, refusal)


def fetch_units(
    reader: RestReader,
    spec: DatasetSpec,
    request: Request,
    *,
    symbols: Sequence[str],
    cancel: Cancel | None,
) -> tuple[list[RestUnit], dict[str, MassiveError]]:
    """Plan the requests of `fetch()` for the symbols, path by path, for a read by symbol.

    The paths are those `fetch()` reads (`fetch_paths`), and their requests those it makes
    (`plan_calls`). Makes no request, except that the first crypto pair a path takes as two
    currencies reads the session's crypto ticker list.

    Args:
        reader: The reader of the session's paths.
        spec: The dataset, with a fetch path.
        request: The normalized arguments; its symbols are ignored.
        symbols: The canonical symbols.
        cancel: The read's stop token, which the crypto ticker list's read listens to.

    Returns:
        The planned requests in plan order, and the symbols that failed before any request
        with their errors: a crypto pair whose lookup failed. A path that does not take a
        parameter the call set raises the `ValueError` of `plan_calls`, which the call has
        already checked for (`RestEndpoints.check_fetch`).
    """
    asked = tuple(sorted(_ordered(symbols)))
    groups = [
        (path, required_instance(own, tuple, f"the symbols of {path}"))
        for path, own in fetch_paths(spec, asked)
    ]
    failed: dict[str, MassiveError] = {}
    split = _failure_recorder(reader, failed, cancel)
    units: list[RestUnit] = []
    for template, own in groups:
        rest_path = reader.rest_path(template)
        calls = plan_calls(
            spec,
            rest_path,
            request,
            symbols=own,
            dates=(request.start, request.end),
            split=split,
        )
        units.extend((rest_path, call) for call in calls if call.symbol not in failed)
    logger.debug(
        "%s fetch by symbol: %d symbols in %d requests; failed before any request: %s",
        spec.name,
        len(asked),
        len(units),
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
    tally: ReadTally,
    to_table: Callable[[pa.Table], Table],
) -> dict[str, SymbolOutcome]:
    """Make a read by symbol's planned REST requests and return each symbol's outcome.

    Each request's rows go to the symbols it names (see the module docstring), and each
    symbol's rows become one static table, in plan order. A symbol whose every request Massive
    had no data for is marked so (`SymbolRows.no_data`). The rows count in `tally`, which the
    read's entry point judges (`ReadTally.judge`); this read never does.

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
        tally: The whole read's count of its rows and of those withheld.
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
        tally=tally,
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
    absent: set[str] = set()
    answered: set[str] = set()
    for (rest_path, planned), result in zip(units, results, strict=True):
        own = planned.symbols
        if isinstance(result, MassiveError):
            for symbol in own:
                failures.setdefault(symbol, result)
        elif result is None:
            absent.update(own)
        else:
            answered.update(own)
            unrequested = _attribute(
                reader,
                spec,
                result,
                template=rest_path.template,
                planned=planned,
                pieces=pieces,
                tally=tally,
            )
            if unrequested is not None:
                for symbol in own:
                    failures.setdefault(symbol, unrequested)
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
    # A symbol with no data from every request that named it; one that failed keeps its failure.
    no_data = absent - answered
    outcomes = {
        symbol: replace(outcome, no_data=True)
        if symbol in no_data and isinstance(outcome, SymbolRows)
        else outcome
        for symbol, outcome in outcomes.items()
    }
    return _with_skipped(outcomes, units)


def _with_skipped(
    outcomes: dict[str, SymbolOutcome], units: Sequence[RestUnit]
) -> dict[str, SymbolOutcome]:
    """Name, in each outcome it applies to, why a call left its crypto pair unread.

    A pair whose currencies Massive's crypto ticker list gives but can't be read as its ticker
    is planned as a call the reader never sends (`Call.unsplit`), so its read ends with no
    rows; `unreadable_symbols` already logged it, and the read lists it in `m.problems()`. A
    read by symbol also names it in its `m.symbol_reads()` row. A pair Massive
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
        why = f"skipped: {unsplit.why}, so the pair gets no rows"
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
    be refused. The values are one read: their rows count in one tally, judged after the last
    value (`ReadTally.judge`), so a value whose rows are all withheld, beside others read
    whole, gets no rows, and when the library could read none of the rows of every value
    whole, each value that read fails.

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
    tally = ReadTally()
    for value in ordered:
        try:
            rows, requests = reader.read_fetch(
                spec,
                request.for_value(value),
                symbols=request.static_symbols,
                cancel=cancel,
                pauses_live_tables=pauses,
                tally=tally,
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
    outcomes = symbol_outcomes(
        core,
        spec,
        symbols=ordered,
        tables=tables,
        failures=failures,
        call=call,
        source=TRANSPORT_LABELS[Transport.REST],
    )
    return tally.judge(spec.name, outcomes)


def asked_column(spec: DatasetSpec, template: str) -> str:
    """Return the column of a path's rows that names the symbol each row was asked for.

    Each row of a request belongs to the asked symbol this column names. It is `Symbol`, the
    row's instrument, unless the path fills its fan-out key from the request
    (`KeySpec.from_request`): then that key's column, `Underlying`, holds the asked symbol and
    `Symbol` holds what the row is about, as on `reference.related_companies`, whose rows
    name the related company. Declared on the dataset's keys, so it depends on the path alone,
    never on the form or size of the request.

    Args:
        spec: The dataset.
        template: The path, as `DatasetSpec.keys` names it.

    Returns:
        The column.
    """
    fanout = spec.keys_for(Transport.REST, template).fanout
    if fanout is not None and fanout.from_request:
        return key_column(spec, "fanout", fanout)
    return _SYMBOL


def _attribute(
    reader: RestReader,
    spec: DatasetSpec,
    rows: pa.Table,
    *,
    template: str,
    planned: Call,
    pieces: dict[str, list[pa.Table]],
    tally: ReadTally,
) -> RowDataError | None:
    """Give one request's rows to the symbols it named, and report the rows of any other.

    Each row goes to the asked symbol its asked-symbol column names (`asked_column`), whatever
    the request's form or how many symbols it named. A row naming a symbol the request did
    not, or no symbol, is left out: the read's `unrequested_symbols` problem, one event per
    symbol such rows name, by the symbol as Massive sent it (`SYMBOL_SENT`; `ProblemEvent`),
    with the table's spelling in the detail where it differs, handed to the session's
    reporter, which alarms once per table and lists each in `m.problems()`. Its page counted
    it among the read's rows (`ReadTally.add`), so it is counted out again, as no row of the
    read's symbols. A request that failed has no rows to give, so none of its rows is left
    out: its failure is its symbols' outcome.

    Args:
        reader: The reader, whose policy and problem sink the read uses.
        spec: The dataset.
        rows: The request's rows, with `SYMBOL_SENT` after the dataset's columns
            (`RestReader.read_each`); the symbols get them without it.
        template: The request's path, for the events.
        planned: The request, whose symbols are those it named (`Call.symbols`).
        pieces: Each symbol's rows so far, which receive the request's.
        tally: The read's count of its rows, which the rows left out are counted out of.

    Returns:
        The failure of the request's symbols when a row was left out under
        `unrequested_symbols=RAISE`; None otherwise.
    """
    own = planned.symbols
    column = asked_column(spec, template)
    sent = rows[SYMBOL_SENT]
    rows = rows.drop_columns([SYMBOL_SENT])
    symbols = rows[column]
    for symbol in own:
        mine = pc.equal(symbols, symbol)
        # The rows are filtered only when some are another symbol's: a copy of a large read
        # whose every row is the symbol's would double its memory for nothing.
        every = pc.all(mine, skip_nulls=False).as_py()
        pieces[symbol].append(rows if every else rows.filter(mine))
    asked = pc.fill_null(pc.is_in(symbols, value_set=pa.array(own, pa.string())), fill_value=False)
    left = pc.invert(asked)
    others = Counter(
        zip(pc.filter(sent, left).to_pylist(), pc.filter(symbols, left).to_pylist(), strict=True)
    )
    if not others:
        return None
    tally.exclude(sum(others.values()))
    return _report_unrequested(reader, spec, others, own=own, column=column, template=template)


def _report_unrequested(
    reader: RestReader,
    spec: DatasetSpec,
    others: Mapping[tuple[str | None, str | None], int],
    *,
    own: Sequence[str],
    column: str,
    template: str,
) -> RowDataError | None:
    """Report the rows a request left out, and return its symbols' failure under `RAISE`.

    Args:
        reader: The reader, whose policy and problem sink the read uses.
        spec: The dataset.
        others: The rows left out, counted by their symbol as Massive sent it and the
            spelling the table gives it.
        own: The symbols the request named.
        column: The asked-symbol column (`asked_column`), the events' field.
        template: The request's path, for the events.

    Returns:
        The failure of the request's symbols when `unrequested_symbols` is `RAISE` in the
        read's policy; None otherwise.
    """
    setting = reader.policy.unrequested_symbols
    at = time.time_ns()
    asked_for = prose_list(own, "symbols")
    events: list[ProblemEvent] = []
    for (spelling, symbol), count in others.items():
        spelled = _spellings([(spelling, symbol)])
        events.append(
            ProblemEvent(
                cause=ProblemCause.UNREQUESTED_SYMBOLS,
                action=ProblemAction.WITHHELD,
                dataset=spec.name,
                stream_id=reader.stream_id,
                where=f"{Transport.REST.value} GET {template}",
                symbol=spelling,
                field=column,
                rows=count,
                detail=f"Massive answered the request for {asked_for} with rows of "
                f"{_no_symbol(spelling)}, which the read did not ask for, so they are left out"
                f"{f'. {spelled}' if spelled else ''}",
                at=at,
                policy=setting,
            )
        )
    reader.problems(events)
    if not effect_of(setting).fails:
        return None
    total = sum(others.values())
    named = prose_list([_no_symbol(spelling) for spelling, _ in others], "symbols")
    spelled = _spellings(others)
    return RowDataError(
        f"{spec.name}: Massive answered the request for {asked_for} with "
        f"{counted(total, 'row')} of {named}, which the read did not ask for, so the library "
        f"left {'it' if total == 1 else 'them'} out. {f'{spelled} ' if spelled else ''}"
        f"{policy_hint(OnError.LOG, 'unrequested_symbols', purpose='To go on without such rows')}"
    )


def _no_symbol(symbol: str | None) -> str:
    """Return a symbol for a message, or `"no symbol"` for None."""
    return "no symbol" if symbol is None else symbol


def _spellings(pairs: Iterable[tuple[str | None, str | None]]) -> str:
    """Return the sentence naming the table's spelling of each symbol Massive spelled otherwise.

    Args:
        pairs: Each symbol as Massive sent it with the spelling the table gives it.

    Returns:
        `"The table spells SPX as I:SPX and ETH-USD as X:ETHUSD."`, or `""` when the table
        spells each symbol as Massive sent it.
    """
    differ = [f"{sent} as {symbol}" for sent, symbol in pairs if sent != symbol]
    return f"The table spells {prose_list(differ, 'symbols')}." if differ else ""


def split_by_symbol(
    core: SessionCore,
    spec: DatasetSpec,
    table: Table,
    *,
    symbols: Sequence[str],
    source: str,
    policy: OnError,
    call: int,
) -> dict[str, Table]:
    """Split one read's static rows of several symbols into each symbol's table.

    Each row goes to the asked symbol its `Symbol` names, as `_attribute` gives a request's
    rows. A row naming none of the symbols, or null, belongs to no symbol: it is left out as
    an `unrequested_symbols` problem, one event per symbol such rows name, handed to the
    session's reporter, which alarms once per table and lists each in `m.problems()`. The
    scan counted only the asked symbols' rows (`ReadTally.add`), so none is counted out.

    Args:
        core: The session's core, whose execution context the split runs in and whose
            reporter takes the problems.
        spec: The dataset.
        table: The rows, a static table with a `Symbol` column.
        symbols: The symbols, in the order the result follows.
        source: Where the rows came from, for the events and the log, such as `"flat files"`.
        policy: The read's `unrequested_symbols` setting.
        call: The read's call id, which its problems carry as their `StreamId`.

    Returns:
        Each symbol with its rows; a symbol without rows has an empty table with the same
        columns.

    Raises:
        RowDataError: If a row was left out and `policy` is `RAISE`: the read's failure, which
            is every symbol's.
    """
    with entered(core.exec_ctx):
        parts = table.partition_by(_SYMBOL)
        none = table.head(0)
        found = {symbol: parts.get_constituent(symbol) for symbol in symbols}
        tables = {symbol: none if part is None else part for symbol, part in found.items()}
        counts = dh_arrow.to_arrow(table.count_by("Rows", by=_SYMBOL)).to_pylist()
    others = {row[_SYMBOL]: row["Rows"] for row in counts if row[_SYMBOL] not in tables}
    if not others:
        return tables
    total = sum(others.values())
    at = time.time_ns()
    core.problems(
        [
            ProblemEvent(
                cause=ProblemCause.UNREQUESTED_SYMBOLS,
                action=ProblemAction.WITHHELD,
                dataset=spec.name,
                stream_id=call,
                where=source,
                symbol=symbol,
                field=_SYMBOL,
                rows=count,
                detail=f"The read of {source} for {prose_list(symbols, 'symbols')} returned "
                f"rows of {'no symbol' if symbol is None else symbol}, which the read did not "
                "ask for, so they are left out",
                at=at,
                policy=policy,
            )
            for symbol, count in others.items()
        ]
    )
    if not effect_of(policy).fails:
        return tables
    named = prose_list(["no symbol" if s is None else s for s in others], "symbols")
    raise RowDataError(
        f"{spec.name}: the read of {source} for {prose_list(symbols, 'symbols')} returned "
        f"{counted(total, 'row')} of {named}, which the read did not ask for, so the library "
        f"left {'it' if total == 1 else 'them'} out. "
        f"{policy_hint(OnError.LOG, 'unrequested_symbols', purpose='To go on without such rows')}"
    )


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
        SessionClosedError: `"This session is closed. ..."` when the session closed during the
            read.
    """
    refused = next((e for e in failures.values() if isinstance(e, AuthenticationError)), None)
    if refused is not None:
        logger.debug(
            "call %d %s by symbol: the credentials were rejected: %s", call, spec.name, refused
        )
        raise refused
    if core.closed:
        raise SessionClosedError(session_closed())
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
            "call %d %s %s failed alone, in its read from %s: %s",
            call,
            spec.name,
            symbol,
            source,
            expected=logging.DEBUG,
            named=spec.name,
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
