"""The base class of every query, and the machinery its methods share.

A query is what a dataset method returns: an immutable description of a read, which touches
nothing until one of its methods turns it into a Deephaven table. `Query` holds what every
query has (its session, its dataset, its normalized request, and the call that built it) and
gives every query `columns()` and its `repr`. The subclasses in `_fetch.py`, `_stream.py`, and
`_history.py` add the methods that read.

This module also holds what those methods share. `_verb` wraps every method: it refuses a
Deephaven update-graph thread and a closed session before any argument is checked, and reports
a long call on the console's thread. `Query._logged_call` gives each static call (`fetch()`,
`history()`) an id and logs its start and outcome; `Query._result` either runs the read
on the caller's thread (`wait=True`) or starts it in the background (`open_read` in
`_tables/background_read.py`); and `_read_symbol_table` reads a table of symbols once, under the
session's rule for a row that is not a ticker.
"""

from __future__ import annotations

import copy
import functools
import logging
import time
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Concatenate, Final, ParamSpec, TypeVar

from typing_extensions import override

from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._errors import (
    MassiveError,
    every_symbol_failed,
    failure_text,
    raises_through,
    user_defect,
)
from deephaven_massive._on_error import effect_of
from deephaven_massive._tables import follow_coordinator
from deephaven_massive._tables.background_read import open_read
from deephaven_massive._tables.columns import (
    column_table,
    describe,
)
from deephaven_massive._tables.execution_context import entered, refuse_update_graph_thread
from deephaven_massive._tables.follow_coordinator import FirstRead
from deephaven_massive._tables.refused_values import refused_values_of
from deephaven_massive._tables.symbol_retry import is_symbol_refusal
from deephaven_massive._tables.transports import (
    SymbolFailure,
)
from deephaven_massive._tables.watchlist import (
    SKIPPED_ROWS_FIX,
    CurrentSymbols,
    SymbolRowRule,
    read_symbols,
    skipped_rows_text,
)

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable, Sequence

    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._enums import OnError
    from deephaven_massive._massive import Transport
    from deephaven_massive._tables.followed_read import FollowPlan
    from deephaven_massive._tables.problem_events import ProblemEvent
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive.queries._call import QueryCall

__all__ = ["Query", "made_query", "shown_as"]

logger = logging.getLogger(__name__)

_FAILED_SHOWN: Final = 5
"""The most failed call-time symbols the WARNING of a followed `history(wait=True)` names."""

_Q = TypeVar("_Q", bound="Query")
_P = ParamSpec("_P")
_R = TypeVar("_R")


def _verb(method: Callable[Concatenate[_Q, _P], _R]) -> Callable[Concatenate[_Q, _P], _R]:
    """Make a query method a verb, run through `SessionCore.run_verb`.

    So every verb raises `ValueError` on an update-graph thread and `MassiveError` on a closed
    session before it checks a single argument, and one that runs long on a thread that holds
    the update graph's lock (a Deephaven console command) is reported once, as the query's call
    and the verb, such as `"m.stocks.bars('AAPL', dm.Timespan.DAY).history()"`.

    Args:
        method: The verb's body.

    Returns:
        The verb, with the body's name, docstring, and signature.
    """

    @functools.wraps(method)
    def verb(self: _Q, /, *args: _P.args, **kwargs: _P.kwargs) -> _R:
        call = f"{self!r}.{method.__name__}()"
        return self._core.run_verb(call, functools.partial(method, self, *args, **kwargs))

    return verb


def _wait(value: object) -> bool:
    """Validate the `wait` argument of `fetch()` and `history()`.

    Args:
        value: The argument as passed.

    Returns:
        The value.

    Raises:
        TypeError: If `value` is not a `bool`.
    """
    if not isinstance(value, bool):
        raise TypeError(f"wait must be a bool, not {type(value).__name__}")
    return value


def _read_symbol_table(
    core: SessionCore,
    spec: DatasetSpec,
    symbols: SymbolTable,
    *,
    query: str,
    noncanonical: OnError,
    live: bool = False,
    call: int | None = None,
) -> tuple[str, ...]:
    """Read a symbol table once and return its symbols (`_read_symbol_rows`).

    Args:
        core: The session core.
        spec: The dataset.
        symbols: The boxed table, static or refreshing.
        query: The `repr` of the query whose verb reads it.
        noncanonical: The call's `noncanonical_symbols`.
        live: True for a live verb.
        call: The id of the static verb's call whose symbols these are, or None.

    Returns:
        The table's canonical symbols, sorted and de-duplicated.
    """
    return _read_symbol_rows(
        core, spec, symbols, query=query, noncanonical=noncanonical, live=live, call=call
    ).symbols


def _read_symbol_rows(
    core: SessionCore,
    spec: DatasetSpec,
    symbols: SymbolTable,
    *,
    query: str,
    noncanonical: OnError,
    live: bool = False,
    call: int | None = None,
    listed: bool = False,
) -> CurrentSymbols:
    """Read a symbol table once, under the call's rule for a row that is not a ticker.

    The table is read as one consistent snapshot (`read_symbols` in `_tables/watchlist.py`),
    on the caller thread, inside the session's execution context, with no library lock held.
    A row that is not Massive's exact ticker follows the call's `noncanonical_symbols`: under
    `RAISE` it fails the call; under `LOG` and `IGNORE` it is skipped (and logged once).
    Every live verb calls it before it starts; a static verb (`fetch()`, `history()`) uses the
    symbols it returns.

    Args:
        core: The session core, for its execution context.
        spec: The dataset, whose asset class the rows must follow.
        symbols: The boxed table, static or refreshing.
        query: The `repr` of the query whose verb reads it, which the messages name.
        noncanonical: The call's `noncanonical_symbols` (`Request.on_error`).
        live: True for a live verb, which may start on a table with no row yet and follow
            it as rows arrive.
        call: The id of the static verb's call whose symbols these are: each skipped value,
            and each value `dm.canonicalize_symbols` refused in the table it returned
            (`RefusedValues.events`), is listed in `m.problems()` as a `noncanonical_symbols`
            problem of the call (`SymbolRowRule.skipped_events`), with the call's query,
            before any error below.
            None for a read the call
            makes only to check the table, or a live verb's, whose table lists them.
        listed: True when the caller lists the skipped values itself once its table has an
            id (`table_problems`), as a stream of a static table does.

    Returns:
        The table's canonical symbols, sorted and de-duplicated, and its skipped values; null
        rows are left out.

    Raises:
        ValueError: If the table has rows and none of them is Massive's ticker, so the result
            would follow nothing (`LOG` and `IGNORE`), or, for a static verb, if the table
            holds no symbol at all, as an empty symbol list does; a table with rows, all
            null, is named as such, since `dm.canonicalize_symbols` nulls what it cannot
            convert.

    Under `RAISE`, a bad row raises the `ValueError` of `read_symbols`, which names the row
    and how to fix it.
    """
    rule = SymbolRowRule.of(spec, noncanonical)
    with entered(core.exec_ctx):
        reported = live or listed or call is not None
        rows = read_symbols(rule, symbols.table, stream_id=None, reported=reported)
        has_rows = symbols.table.size > 0
    if call is not None:
        events = table_problems(
            spec, symbols, rule, skipped=rows.skipped, stream_id=call, query=query
        )
        if events:
            # Named first: a call that raises below never reaches the read that names it.
            core.diagnostics.name_read(call, query)
            core.problems(events)
    if rows.skipped and not rows.symbols:
        raise ValueError(
            user_defect(
                f"{query}: No row of the symbol table is a ticker this dataset takes, so the "
                f"result would follow nothing. The table holds {skipped_rows_text(rows.skipped)}",
                SKIPPED_ROWS_FIX,
            )
        )
    if not rows.symbols and not live:
        if has_rows:
            raise ValueError(
                user_defect(
                    f"{query}: Every row of the symbol table has a null {rule.column}. The usual "
                    "cause is dm.canonicalize_symbols, which nulls each value it cannot convert",
                    "Fix the values that m.problems() lists for this call",
                )
            )
        raise ValueError(
            user_defect(
                f"{query}: The symbol table has no rows", "Pass a table with at least one ticker"
            )
        )
    return rows


def table_problems(
    spec: DatasetSpec,
    symbols: SymbolTable,
    rule: SymbolRowRule,
    *,
    skipped: Sequence[str],
    stream_id: int,
    query: str,
) -> list[ProblemEvent]:
    """Return the `noncanonical_symbols` problems of one read of a static symbol table.

    Each value the read skipped, and each value `dm.canonicalize_symbols` refused in the table
    it returned (`RefusedValues.events`), is one problem: the table never changes, so it is
    listed once.

    Args:
        spec: The dataset.
        symbols: The boxed table.
        rule: The rule the read followed.
        skipped: The values it skipped.
        stream_id: The call's id, or the stream's that read the table.
        query: The `repr` of the query whose verb read it.

    Returns:
        The problems; none when the read skipped nothing and nothing was refused.
    """
    where = f"the symbol table of {query}"
    events = rule.skipped_events(skipped, stream_id=stream_id, where=where, field=None)
    refused = refused_values_of(symbols.table, rule.column)  # a `canonicalize_symbols` table
    if refused is not None:
        events += refused.events(dataset=spec.name, stream_id=stream_id, where=where, field=None)
    return events


def _log_failure(call: int, elapsed: float, error: BaseException) -> None:
    """Log a failed static call: a defect at ERROR, anything else at DEBUG.

    The caller receives the exception, so an expected failure (a refusal, a rate limit, a
    network failure, a closed session) is logged only at DEBUG, with its text, which names
    the status, the path, and the `request_id`. A `MassiveError` that is a defect (a row
    Massive sent that the library cannot read, a symbol table of the user's that failed) is
    logged at ERROR with whose defect it is, so that a broad `except` never hides it; its
    traceback stays with the exception the caller receives. Anything else (a bad argument's
    `ValueError`, an interrupt) is the caller's to handle, so it is logged at DEBUG with its
    traceback.

    Args:
        call: The call's id.
        elapsed: Seconds since the call started.
        error: What the call raised.
    """
    if isinstance(error, MassiveError):
        level = logging.DEBUG if not error.is_bug else logging.ERROR
        logger.log(level, "call %d failed after %.3f s: %s", call, elapsed, failure_text(error))
    else:
        logger.debug(
            "call %d failed after %.3f s: %s: %s",
            call,
            elapsed,
            type(error).__name__,
            error,
            exc_info=error,
        )


# Why a query has a refusing constructor, and how the library builds one instead.
#
# Every type a public signature declares must be public, and `help()` and the API reference
# render each public class's constructor. The `__init__` a dataclass generates would take the
# query's four private parts (its session core, its dataset spec, its request, and the call
# that built it), so it would declare four private types in a public signature. A user never
# builds a query: a dataset method does. So the dataclass generates no `__init__`
# (`init=False`), and the public one refuses, saying where queries come from.
#
# The library builds a query in `made_query` and re-labels one in `shown_as`, below, through
# `__new__` and `object.__setattr__`. That is what a frozen dataclass's own generated `__init__`
# does, since plain assignment to a frozen instance raises `FrozenInstanceError`. Other
# libraries do the same: pyarrow's `Table` constructor refuses and points to its factory
# functions, and polars and pydantic build objects through `__new__`. Only this module touches
# the private fields.
#
# Rejected: exempting this constructor from the check that public signatures declare only
# public types (a rule with an exception stops being a rule); documenting the constructor as
# internal (`help()` still renders the private types); a public handle type wrapping the four
# parts (a public name no user needs); and a `ContextVar` that hands the call to the factory so
# nothing re-labels a query (hidden state whose only job is to avoid this construction).
@dataclass(frozen=True, repr=False, slots=True, init=False)
class Query:
    """Any query: the common type of what every dataset method returns.

    A query describes one read of a dataset (its symbols, its range, its parameters) and reads
    nothing until you call one of its methods, each of which returns a Deephaven table. Which
    methods it has depends on its subclass: `FetchQuery`, `StreamQuery`, `NoSymbolStreamQuery`,
    `HistoryQuery`, `StreamHistoryQuery`, or `NoSymbolStreamHistoryQuery`. Annotate with
    `Query` when your code takes any query and uses only what every query has: `columns()` and
    its `repr`, which is the call that built it, such as `m.stocks.trades(['AAPL', 'MSFT'])`.

    Import it as `from deephaven_massive.queries import Query`. You get a query from a dataset
    method, such as `m.stocks.trades("AAPL")`; calling a query class raises `TypeError`. A query
    is immutable and hashable, safe to share between threads, and equal to another query of the
    same class and session that asks for the same data.

    Example:
        ::

            from deephaven.table import Table

            from deephaven_massive.queries import Query


            def column_names(query: Query) -> Table:
                return query.columns().view("Name")


            names = column_names(m.stocks.trades("AAPL"))
    """

    _core: SessionCore
    _spec: DatasetSpec
    _request: Request
    _call: QueryCall = field(compare=False)

    def __init__(self) -> None:
        """Refuse to build a query: a dataset method builds every query.

        Raises:
            TypeError: Always. Call a dataset method instead, such as `m.stocks.trades("AAPL")`.
        """
        raise TypeError(
            f"{type(self).__name__}() cannot be called. Build a query with a dataset method, "
            "such as m.stocks.trades('AAPL')."
        )

    @raises_through(refuse_update_graph_thread)
    def columns(self) -> Table:
        """List the dataset's columns, their types, and what each one means.

        Every method of the query returns these columns, in this order. A column that the call's
        source doesn't fill is null. The call reads nothing from Massive and works on a closed
        session.

        Returns:
            A static table with one row per column, and these columns:

            - `Name`: the column's name.
            - `Type`: its Deephaven type, such as `double`, `long`, `String`, or `Instant`. A
              calendar date, such as `Date`, is `yyyy-MM-dd` text.
            - `Description`: what it holds, empty where Massive gives no description.
            - `SuppliedBy`: the sources that fill it: `REST`, `flat files`, or `WebSocket`.
              `fetch()` and `poll()` read REST, the stream methods read the WebSocket, and
              `history()` reads REST or flat files.

        Raises:
            ValueError: If called on a Deephaven update-graph thread, such as inside a table
                listener.

        Example:
            ::

                cols = m.stocks.trades("AAPL").columns()
                # The columns a stream leaves null.
                not_streamed = cols.where("!SuppliedBy.contains(`WebSocket`)")
        """
        refuse_update_graph_thread(self._core.exec_ctx)
        schema = self._core.dataset_schema(self._spec)
        with entered(self._core.exec_ctx):
            return column_table(schema)

    @override
    def __repr__(self) -> str:
        """Show the call that built the query, with its non-default arguments.

        Returns:
            For example `m.stocks.trades(['AAPL', 'MSFT'])` (`QueryCall.text`).
        """
        return self._call.text(self._request)

    def _described(self, table: Table, *, note: str | None = None) -> Table:
        """Describe the static table a transport returned, as the verb's result (`describe`).

        Static results need no liveness scope: they never refresh. The copy is made on the
        caller thread, inside the session's execution context.

        Args:
            table: The static table of `fetch()` or `history()`.
            note: A sentence for the table description after the summary (`history()`'s
                source), or None.

        Returns:
            Its copy that carries the dataset's column and table descriptions.
        """
        spec = self._spec
        schema = self._core.dataset_schema(spec)
        with entered(self._core.exec_ctx):
            return describe(table, spec, schema, note=note)

    def _logged_call(
        self, verb: str, run: Callable[[int], Table], *, background: bool = False
    ) -> Table:
        """Run a static verb as one call: a new call id, and its start and outcome at DEBUG.

        Args:
            verb: The verb and its arguments, for the start line, such as `"fetch()"`.
            run: Runs the verb's work, given the call id; the id names the transport's worker
                threads.
            background: True for a call that does not wait: `run` returns the table a
                background read fills, and the read logs the call's outcome, so no `done` line
                is logged here.

        Returns:
            What `run` returns.

        Whatever `run` raises propagates after a `call <id> failed` line (`_log_failure`). The
        verb's guard (`_verb`) runs before this, so a call on an update-graph thread raises
        `ValueError` before it takes an id, and logs nothing.
        """
        call = self._core.next_stream_id()
        logger.debug("call %d start: %r.%s", call, self, verb)
        started = time.monotonic()
        try:
            table = run(call)
        except BaseException as error:
            _log_failure(call, time.monotonic() - started, error)
            raise
        if not background and logger.isEnabledFor(logging.DEBUG):
            elapsed = time.monotonic() - started
            logger.debug("call %d done: %d rows in %.3f s", call, table.size, elapsed)
        return table

    def _static_request(self, call: int) -> Request:
        """Return the request with a symbol table read once, for a static verb.

        Args:
            call: The verb's call id, whose problems list the table's skipped values.

        Returns:
            The request; for a symbol table, with its symbols (`_read_symbol_table`).
        """
        request = self._request
        if isinstance(request.symbols, SymbolTable):
            symbols = _read_symbol_table(
                self._core,
                self._spec,
                request.symbols,
                query=repr(self),
                noncanonical=self._request.on_error.noncanonical_symbols,
                call=call,
            )
            return replace(request, symbols=symbols)
        return request

    def _result(
        self,
        call: int,
        request: Request,
        *,
        wait: bool,
        transport: Transport,
        note: str | None,
        run: Callable[[threading.Event | None], Table],
    ) -> Table:
        """Run a static verb's transport call now, or start it as a background read.

        Args:
            call: The call id.
            request: The request with static symbols, for the read's `m.streams()` row.
            wait: True to run `run` on this thread and return its described static table;
                False to return at once the refreshing table a background read fills
                (`open_read`).
            transport: The source `run` reads, for the read's row.
            note: The sentence the table description adds after the summary, or None.
            run: Runs the transport call, given a background read's stop token or None.

        Returns:
            The static result, or the refreshing one a background read fills.
        """
        if wait:  # no `m.streams()` row names its query: its problems take it from here
            self._core.diagnostics.name_read(call, repr(self))
            return self._described(run(None), note=note)
        return open_read(
            self._core,
            spec=self._spec,
            call=call,
            query=repr(self),
            symbols=request.symbols,
            transport=transport,
            note=note,
            run=run,
        )

    def _first_read(self, plan: FollowPlan, call_time: tuple[str, ...], *, verb: str) -> FirstRead:
        """Read the call-time symbols of a followed `history()` or `fetch()` now, as one read.

        The read of `wait=True`, on the caller thread.

        Args:
            plan: How the symbols are read.
            call_time: The call-time symbols; at least one.
            verb: The verb, `"history"` or `"fetch"`, for the WARNING.

        Returns:
            Each symbol's outcome, with when the read began and how long it took.

        Raises:
            MassiveError: The first symbol's error, retryable only when every symbol's is,
                when every symbol failed. A failure of every symbol alike (a rejected key, a
                closed session) propagates from the read.
        """
        began_at = int(follow_coordinator.CLOCK.time() * 1e9)
        started = time.monotonic()
        outcomes = plan.read(call_time, None)  # its caller waits: no stop token
        failed = {
            name: outcome
            for name, outcome in outcomes.items()
            if isinstance(outcome, SymbolFailure)
        }
        if len(failed) == len(call_time):
            first = every_symbol_failed([failed[name].error for name in call_time])
            raise first from failed[call_time[0]].error
        if not effect_of(plan.refused_symbols).fails:
            # A refused symbol's alarm is its refused_symbols span, which the session's problem
            # reporter logs once per table under LOG, and only at DEBUG under IGNORE.
            refused = [name for name, o in failed.items() if is_symbol_refusal(o.error)]
            if refused:
                logger.debug(
                    "%s %s: Massive refused %s; each is a refused_symbols problem",
                    self._spec.name,
                    verb,
                    refused,
                )
            failed = {name: o for name, o in failed.items() if name not in refused}
        if failed:
            logger.warning(
                "%s %s: %d of the %d symbols failed and are left out of the result for now. %s "
                "Check m.symbol_reads() for whether each one is retrying or has failed.",
                self._spec.name,
                verb,
                len(failed),
                len(call_time),
                " ".join(
                    _failure_sentence(name, outcome.error)
                    for name, outcome in list(failed.items())[:_FAILED_SHOWN]
                ),
            )
        return FirstRead(outcomes, began_at, time.monotonic() - started)


def _failure_sentence(symbol: str, error: BaseException) -> str:
    """Return one symbol's failure as a sentence of a log record that lists several.

    Args:
        symbol: The symbol whose read failed.
        error: Its failure.

    Returns:
        Such as `"MSFT: Massive answered GET /v3/trades/MSFT on 127.0.0.1 with HTTP 403:
        "not in the plan"."`, ended with a period when the failure's text has none.
    """
    text = failure_text(error)
    return f"{symbol}: {text}" if text.endswith(".") else f"{symbol}: {text}."


def made_query(
    cls: type[_Q], *, core: SessionCore, spec: DatasetSpec, request: Request, call: QueryCall
) -> _Q:
    """Build a query of class `cls` from its four parts; the dataset factory's only way to.

    `Query.__init__` refuses every caller (see the comment above `Query`), so the factory builds
    the instance through `__new__` and sets each frozen field with `object.__setattr__`, as the
    `__init__` a frozen dataclass generates does.

    Args:
        cls: The query class to build, `Query` or a subclass.
        core: The session core the query runs on.
        spec: The dataset.
        request: The normalized request.
        call: The call that built the query, which is its `repr`.

    Returns:
        The query.
    """
    query = cls.__new__(cls)
    object.__setattr__(query, "_core", core)
    object.__setattr__(query, "_spec", spec)
    object.__setattr__(query, "_request", request)
    object.__setattr__(query, "_call", call)
    return query


def shown_as(query: _Q, call: QueryCall) -> _Q:
    """Return a copy of `query` whose `repr` is `call`, the call a user typed.

    The dataset factory builds a query whose call names only its dataset; the wrapper of each
    dataset method (`shows_call` in `_call.py`) then records the call as made. `Query.__init__`
    refuses every caller (see the comment above `Query`), so the copy is made with `copy.copy`
    and its frozen `_call` set with `object.__setattr__`, as a frozen dataclass's own `__init__`
    sets fields. The call is no part of a query's equality, so the copy equals `query`.

    Args:
        query: The query the dataset method returned.
        call: The call as made.

    Returns:
        The copy.
    """
    shown = copy.copy(query)
    object.__setattr__(shown, "_call", call)
    return shown
