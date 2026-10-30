"""The four query classes and their verbs.

A dataset method such as `m.stocks.trades(["AAPL"])` returns a *query*: an immutable,
hashable description of what to fetch. Nothing touches the network until a *verb* is called,
and each verb returns a Deephaven `Table` of a particular shape:

| Class                | Verbs                                                     |
|----------------------|-----------------------------------------------------------|
| `Query`              | `fetch()`, `poll(interval)`                               |
| `StreamQuery`        | `blink()`, `append_only()`, `ring(capacity)`, `last_by()` |
| `HistoryQuery`       | `history(start, end=None)`                                |
| `StreamHistoryQuery` | the stream verbs and `history()`                          |

A verb that cannot work for a dataset does not exist on its query class, so tab completion and
`hasattr` are the contract; calling one raises an `AttributeError` that names the verbs the
query has and, where one exists, the dataset that has the verb (`QueryBase.__getattr__`): a
one-row endpoint, such as each symbol's last trade, is a `Query` dataset of its own
(`m.stocks.last_trade`), never a `fetch()` of the stream. Every query also has `columns()`,
which is not a verb: it lists the dataset's columns,
their types, and what each one means, as a static table built without any network call.
Verbs validate their parameters synchronously on the calling thread: a wrong type raises
`TypeError` and an invalid value raises `ValueError`. Every verb also raises `ValueError` on a
Deephaven update-graph thread (for example inside a table listener), because verbs create tables and
take the session's registry lock. A thread that only holds the update graph's lock is not one: the
Deephaven console runs every command holding it, and every verb works there.

The verbs delegate the transport work to three interfaces, one per transport, defined in
`_tables/transports.py` and provided by the session core's factory hooks: `RestTransport` (`fetch`,
`poll`, REST `history`), `StreamTransport` (opening a WebSocket stream), and
`FlatFileTransport` (flat-file `history`).
`history()` chooses its source itself (`choose_source` in `_datasets/history_source.py`) and
then calls one transport, so the transports only do I/O.
Each static verb (`fetch()`, `history()`) is one *call*: it takes a process-unique id from the
session's stream-id counter and logs `call 7 start: <query>.<verb>`, then `call 7 done: <rows>
rows in <s> s` or `call 7 failed after <s> s: <error>`, at DEBUG; the transports name their
worker threads after it (`massive #1 call-7 rest_0`), so every line of one call can be found.
Live stream results are shaped and tracked by the session (`SessionCore.stream_result`), so
every stream verb shares one stream per (dataset, channel, symbol set).

Users import the classes for type annotations from the public module
`deephaven_massive.queries`, and never construct them.
"""

from __future__ import annotations

import functools
import logging
import time
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Concatenate, Final, ParamSpec, TypeVar

from deephaven import arrow as dh_arrow
from deephaven.stream import blink_to_append_only
from deephaven.table_factory import ring_table

from deephaven_massive._config import require_positive_int, require_positive_number
from deephaven_massive._datasets.bar_periods import intraday
from deephaven_massive._datasets.catalog import timespan_key
from deephaven_massive._datasets.dates import parse_date_like
from deephaven_massive._datasets.history_source import (
    choose_source,
    history_window,
    partition_dates,
    rest_history_problem,
)
from deephaven_massive._datasets.symbols import SymbolTable, symbol_class
from deephaven_massive._enums import (
    HistorySource,
    PollMode,
    _require_enum,
)
from deephaven_massive._errors import MassiveError
from deephaven_massive._tables.columns import column_table, describe
from deephaven_massive._tables.execution_context import entered, refuse_update_graph_thread
from deephaven_massive._tables.watchlist import read_symbols
from deephaven_massive._types import override

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._types import DateLike

__all__ = [
    "HistoryQuery",
    "Query",
    "QueryBase",
    "StreamHistoryQuery",
    "StreamQuery",
    "verbs",
]

logger = logging.getLogger(__name__)

MAX_RING_CAPACITY: Final = 2**31 - 1
"""The largest `ring()` capacity Deephaven accepts (a Java `int`)."""

_VERBS: Final = ("fetch", "poll", "blink", "append_only", "ring", "last_by", "history")
"""Every verb's name, in the order a missing verb's message lists the verbs a query has."""

_STREAM_VERBS: Final = frozenset({"blink", "append_only", "ring", "last_by"})
"""The verbs a dataset with WebSocket channels serves."""

_Q = TypeVar("_Q", bound="QueryBase")
_P = ParamSpec("_P")
_R = TypeVar("_R")


def _verb(method: Callable[Concatenate[_Q, _P], _R]) -> Callable[Concatenate[_Q, _P], _R]:
    """Make a query method a verb, run through `SessionCore.run_verb`.

    So every verb raises `ValueError` on an update-graph thread and `MassiveError` on a closed
    session before it checks a single argument, and one that runs long on a thread that holds
    the update graph's lock (a Deephaven console command) is reported once, as
    `"<dataset> <verb>()"`.

    Args:
        method: The verb's body.

    Returns:
        The verb, with the body's name, docstring, and signature.
    """

    @functools.wraps(method)
    def verb(self: _Q, /, *args: _P.args, **kwargs: _P.kwargs) -> _R:
        call = f"{self._spec.name} {method.__name__}()"
        return self._core.run_verb(call, functools.partial(method, self, *args, **kwargs))

    return verb


def _interval(value: object) -> timedelta:
    """Validate a `poll()` interval.

    Args:
        value: Seconds as an `int` or `float`, or a `timedelta`.

    Returns:
        The interval as a `timedelta`.

    A number that is not finite and positive, or not a number at all, raises the `ValueError`
    or `TypeError` of `require_positive_number`.

    Raises:
        ValueError: If the interval overflows `timedelta`, or is not positive once rounded to
            microseconds.
    """
    if isinstance(value, timedelta):
        period = value
    else:
        seconds = require_positive_number("interval", value)
        try:
            period = timedelta(seconds=seconds)
        except OverflowError:  # beyond timedelta's range
            raise ValueError(f"interval must be a positive, finite time, got {value!r}") from None
    if period <= timedelta(0):  # also an interval that rounds to zero microseconds
        raise ValueError(f"interval must be a positive, finite time, got {value!r}")
    return period


def _serves(spec: DatasetSpec, verb: str) -> bool:
    """Tell whether a dataset's query has a verb, from the transports its spec declares.

    The query classes follow from the same transports (`query_class` in
    `_namespaces/base.py`): a fetch path gives `fetch()` and `poll()`, channels the stream
    verbs, and a history source `history()`. A `bars` dataset streams at some timespans only,
    which is enough to name it as the dataset that streams.

    Args:
        spec: The dataset.
        verb: A verb's name, one of `_VERBS`.

    Returns:
        True when the dataset serves the verb.
    """
    if verb in _STREAM_VERBS:
        return spec.has_channels
    if verb == "history":
        return spec.has_history
    return spec.has_fetch


def verbs(spec: DatasetSpec) -> tuple[str, ...]:
    """Return the verbs a dataset's queries can have, in the order `_VERBS` lists them.

    A `bars` dataset streams at some timespans only, and its stream verbs are listed all the
    same; the verbs of one `bars()` call follow its timespan (`bar_query_class` in
    `_namespaces/base.py`). `dm.datasets()` shows the result.

    Args:
        spec: The dataset.

    Returns:
        The verbs' names, such as `("fetch", "poll")` or `("blink", "append_only", "ring",
        "last_by", "history")`.
    """
    return tuple(verb for verb in _VERBS if _serves(spec, verb))


def _read_symbol_table(
    core: SessionCore, spec: DatasetSpec, symbols: SymbolTable
) -> tuple[str, ...]:
    """Read a symbol table once, refusing a row that is not Massive's exact ticker.

    The table is read as one consistent snapshot (`read_symbols` in `_tables/watchlist.py`),
    on the caller thread, inside the session's execution context, with no library lock held.
    Every live verb calls it before it starts, so a bad row fails the call; a static verb
    (`fetch()`, `history()`) uses the symbols it returns.

    Args:
        core: The session core, for its execution context.
        spec: The dataset, whose asset class the rows must follow.
        symbols: The boxed table, static or refreshing.

    Returns:
        The table's symbols, sorted and de-duplicated; null rows are ignored.

    Raises:
        ValueError: If the table holds no symbol, as an empty symbol list does.

    A row that is not canonical raises the `ValueError` of `read_symbols`, which names
    `dm.canonicalize_symbols`.
    """
    with entered(core.exec_ctx):
        found = read_symbols(spec, symbols.table)
    if not found:
        raise ValueError(f"{spec.name}: the symbol table holds no symbols")
    return found


def _log_failure(call: int, elapsed: float, error: BaseException) -> None:
    """Log a failed static call: the library's failures at ERROR, anything else at DEBUG.

    A `MassiveError` (a request, a row, a rate limit, a closed session) is the failure a user
    must see, so it is logged at ERROR with its text, which names the status, the path, and
    the `request_id`; its traceback stays with the exception the caller receives. Anything
    else (a bad argument's `ValueError`, an interrupt) is the caller's to handle, so it is
    logged at DEBUG with its traceback.

    Args:
        call: The call's id.
        elapsed: Seconds since the call started.
        error: What the call raised.
    """
    if isinstance(error, MassiveError):
        logger.error("call %d failed after %.3f s: %s", call, elapsed, error)
    else:
        logger.debug(
            "call %d failed after %.3f s: %s: %s",
            call,
            elapsed,
            type(error).__name__,
            error,
            exc_info=error,
        )


@dataclass(frozen=True, repr=False, slots=True)
class QueryBase:
    """The state every query class shares: the session, the dataset, and the arguments.

    Frozen and hashable; equality includes the concrete class, so a `StreamQuery` never equals
    a `HistoryQuery`. Queries have no public attributes: the verbs and `columns()` are the
    whole API, and the `repr` names the class, the dataset, and the arguments that differ from
    their defaults.
    """

    _core: SessionCore
    _spec: DatasetSpec
    _request: Request

    def columns(self) -> Table:
        """List this dataset's columns, their types, and what each one means.

        Every verb of the dataset returns these columns in this order, whichever transport
        serves it; a column the serving transport does not supply is null. The table is built
        from the committed generated schemas: no request is made, no stream is opened, and it
        works on a closed session.

        Returns:
            A static table with one row per column, in result column order:
            `Name` (String), the column name; `Type` (String), its Deephaven type (`long`,
            `int`, `double`, `boolean`, `String`, `Instant`, or `int[]`; every calendar date,
            such as `Date`, is ISO `yyyy-MM-dd` text, a `String`);
            `Description` (String), what the column holds, empty when Massive gives no
            description; and `SuppliedBy` (String), the transports whose sources fill the
            column for this dataset, spelled `REST`, `flat files`, and `WebSocket`, in that
            order, joined by ", " (`fetch()` and `poll()` are served by REST, the stream verbs
            by WebSocket, and `history()` by REST or flat files); empty for a column that only
            other datasets of the same kind supply, which this dataset always leaves null.

        Like every verb, it raises `ValueError` on a Deephaven update-graph thread, for example
        from a table listener.

        Example:
            ::

                cols = m.stocks.trades(["AAPL"]).columns()
                stream_only = cols.where("SuppliedBy = `WebSocket`")
        """
        refuse_update_graph_thread(self._core.exec_ctx)
        schema = self._core.dataset_schema(self._spec)
        with entered(self._core.exec_ctx):
            return column_table(schema)

    @override
    def __repr__(self) -> str:
        """Show the query class, the dataset, and the non-default arguments.

        Returns:
            For example `<StreamHistoryQuery stocks.trades symbols=('AAPL', 'MSFT')>`.
        """
        described = self._request.describe()
        suffix = f" {described}" if described else ""
        return f"<{type(self).__name__} {self._spec.name}{suffix}>"

    def __getattr__(self, name: str) -> object:
        """Explain a verb this query does not have, and name the dataset that has it.

        Python calls this only when the normal lookup fails, so the query's own verbs,
        `columns()`, and every other attribute resolve as usual, and `hasattr(q, "fetch")` is
        False on a query without `fetch()`. The return type is `object`, not `NoReturn`, so a
        type checker still refuses a call of a verb the query's class lacks.

        Args:
            name: The attribute that was not found.

        Returns:
            Never: it always raises.

        Raises:
            AttributeError: Always. For a verb's name, the message names the dataset, the verbs
                its query has, and where one exists the dataset that has the verb, such as
                `"stocks.trades has no fetch(); its verbs are blink(), append_only(), ring(),
                last_by(), history(); use m.stocks.last_trade(...).fetch()"`
                (`_missing_verb`); for any other name, Python's own message.
        """
        if name not in _VERBS:
            raise AttributeError(
                f"{type(self).__name__!r} object has no attribute {name!r}", name=name, obj=self
            )
        raise AttributeError(self._missing_verb(name), name=name, obj=self)

    def _missing_verb(self, verb: str) -> str:
        """Say why this query has no `verb`, and what to call instead.

        Reads only the catalog: no request, no Deephaven call.

        Args:
            verb: The missing verb's name, one of `_VERBS`.

        Returns:
            The `AttributeError` message: the dataset, the verbs its query has, and the
            alternative of `_alternative`, where there is one.
        """
        has = ", ".join(f"{name}()" for name in _VERBS if hasattr(type(self), name))
        text = f"{self._spec.name} has no {verb}(); its verbs are {has}"
        alternative = self._alternative(verb)
        return text if alternative is None else f"{text}; {alternative}"

    def _alternative(self, verb: str) -> str | None:
        """Name what serves a verb this query lacks.

        In order: for `fetch()` or `poll()` of intraday bars, `history(start, end)`, then the
        previous day's bar; the other datasets of the same kind and asset class that serve the
        verb (a stream's one-row dataset, such as `stocks.last_trade` for `fetch()` of
        `stocks.trades`, and back); for `fetch()` or `poll()`, the class's `snapshots` dataset,
        the current state on every plan; for `history()` of a dataset with a time axis, its
        `start=` and `end=` arguments; for a stream verb of bars that do not stream, the
        timespans that do.

        Args:
            verb: The missing verb's name, one of `_VERBS`.

        Returns:
            The alternative, such as `"use m.stocks.last_trade(...).fetch()"`; None when
            nothing else serves the verb.
        """
        spec = self._spec
        by_name = self._core.catalog.by_name
        siblings = " or ".join(
            f"m.{other.name}(...).{verb}()"
            for other in by_name.values()
            if other is not spec
            and spec.kind is not None
            and (other.kind, other.group) == (spec.kind, spec.group)
            and _serves(other, verb)
        )
        timespan = self._request.timespan
        intraday_bars = spec.is_bars and timespan is not None and intraday(timespan)
        if verb in ("fetch", "poll") and intraday_bars:
            # Intraday bars are read over a range; the previous day's bar is another shape.
            days = f", or {siblings} for each symbol's previous day" if siblings else ""
            return f"use history(start, end) for these bars{days}"
        if siblings:
            return f"use {siblings}"
        snapshots = by_name.get(f"{spec.name.partition('.')[0]}.snapshots")
        if verb in ("fetch", "poll") and snapshots is not None:
            return f"for each symbol's current state use m.{snapshots.name}(...).{verb}()"
        if verb == "history" and spec.time_axis is not None:
            return f"pass start= and end= to m.{spec.name}(...) instead"
        if verb in _STREAM_VERBS and spec.is_bars and spec.has_channels:
            spans = " and ".join(f"dm.Timespan.{span.name}" for span in spec.channels)
            return f"only bars of {spans} with multiplier=1 stream"
        return None

    def _described(self, table: Table) -> Table:
        """Describe the static table a transport returned, as the verb's result (`describe`).

        Static results need no liveness scope: they never refresh. The copy is made on the
        caller thread, inside the session's execution context.

        Args:
            table: The static table of `fetch()` or `history()`.

        Returns:
            Its copy that carries the dataset's column and table descriptions.
        """
        spec = self._spec
        schema = self._core.dataset_schema(spec)
        with entered(self._core.exec_ctx):
            return describe(table, spec, schema)

    def _logged_call(self, verb: str, run: Callable[[int], Table]) -> Table:
        """Run a static verb as one call: a new call id, and its start and outcome at DEBUG.

        Args:
            verb: The verb and its arguments, for the start line, such as `"fetch()"`.
            run: Runs the verb's work, given the call id; the id names the transport's worker
                threads.

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
        if logger.isEnabledFor(logging.DEBUG):
            elapsed = time.monotonic() - started
            logger.debug("call %d done: %d rows in %.3f s", call, table.size, elapsed)
        return table

    def _static_request(self) -> Request:
        """Return the request with a symbol table read once, for a static verb.

        Returns:
            The request; for a symbol table, with its symbols (`_read_symbol_table`).
        """
        request = self._request
        if isinstance(request.symbols, SymbolTable):
            symbols = _read_symbol_table(self._core, self._spec, request.symbols)
            return replace(request, symbols=symbols)
        return request


class Query(QueryBase):
    """A REST list or snapshot dataset: a static `fetch()` or a live `poll()`.

    Built by a dataset method, such as `m.stocks.dividends(...)` or `m.stocks.snapshots(...)`;
    immutable and reusable.

    Example:
        ::

            divs = m.stocks.dividends(["AAPL", "MSFT"], start="2024-01-01").fetch()
            live = m.stocks.snapshots(["AAPL", "MSFT"]).poll(5.0)  # every 5 seconds
    """

    __slots__ = ()

    @_verb
    def fetch(self) -> Table:
        """Return every row the dataset's endpoint returns, fully paginated, as a static table.

        The requests run on the calling thread, or for an endpoint that takes one symbol per
        request on up to `max_concurrency` threads. `limit`, when the dataset was built with
        one, caps the total number of rows; the page size is internal. Columns are the
        dataset's schema, ending with `TimestampInsert`, when the row's page arrived. A
        reactive symbol table is read once, when the verb is called. HTTP 401 raises
        `AuthenticationError`, HTTP 403 `EntitlementError`, and any other failure
        `MassiveError`; a symbol with no data (HTTP 404 on its first request) returns no rows,
        with a WARNING, while HTTP 404 on a later page of its pages raises `MassiveError`,
        since its data was cut short.

        Before any request it raises `ValueError` if a path the call reads does not take a
        parameter or filter the query set (an explicit `adjusted` on a path without it), or a
        symbol table holds no symbol or a row that is not canonical. A failed request raises
        `AuthenticationError` on HTTP 401, `EntitlementError` on HTTP 403, and `MassiveError`
        for a rate limit that outlived the retries, a row that breaks the dataset's schema, or
        any other failure.

        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        Returns:
            A static table.
        """
        return self._logged_call("fetch()", self._fetch)

    def _fetch(self, call: int) -> Table:
        """Run `fetch()` as call `call`.

        Args:
            call: The call id.

        Returns:
            The static table.
        """
        return self._described(
            self._core.rest_transport().fetch(self._spec, self._static_request(), call=call)
        )

    @_verb
    def poll(self, interval: float | timedelta, *, mode: PollMode | None = None) -> Table:
        """Return a live table that re-runs `fetch()` every `interval`.

        A reactive `Table` passed as `symbols` is re-read once per cycle on the poller thread.
        Every non-null row must be Massive's exact ticker: a bad row present at the call raises
        `ValueError`, and one added later fails the table, naming `dm.canonicalize_symbols`.
        Fan-out endpoints issue one request per symbol per cycle, at most `max_concurrency` at a
        time. The poller runs while the table, or a table derived from it, is in use; releasing
        the table (`m.release`, garbage collection) stops it once nothing derived from it is
        left, and `m.close()` stops it at once. A table derived in the console stays in use
        until the JVM next collects it, and until then the poll keeps making its requests,
        which on the Basic tier come out of the five a minute every session of the key shares.
        A
        non-numeric `interval` or a `mode` that is not a `PollMode` raises `TypeError`, and a
        non-positive `interval` raises `ValueError`.

        Before the table exists it raises `ValueError` if a path does not take a parameter or
        filter the query set: with static symbols any path the call reads, and with a reactive
        table every fetch path of the dataset. Later, a cycle that fails with
        `AuthenticationError`, `EntitlementError`, a row that breaks the dataset's schema, a row
        with a null or NaN value in one of the dataset's poll keys (in every mode: such a key
        would merge unrelated rows; the error names the column and shows the row), or a
        parameter the path a reactive table's new symbol routes to does not take, fails the
        table (its `m.streams()` row shows `FAILED`) and ends the poller. Any other failure is
        logged at ERROR and shown in the row's `Error`; the table keeps its rows and the next
        cycle runs.

        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        Args:
            interval: Seconds between cycles, or a `timedelta`; must be positive.
            mode: How cycles combine (`PollMode`); None uses the dataset's default:
                `APPEND` for news and filings (not the filing taxonomies, insider activity, or
                insider holdings), `UPSERT` for IPOs, and `REPLACE` otherwise.

        Every row's `TimestampInsert` is when its page arrived, so in a keyed table it is the
        time of the latest cycle that returned the row; it is never part of a row's identity.

        Returns:
            A live table: keyed for `REPLACE` and `UPSERT`, append-only for `APPEND`. A
            `REPLACE` table removes keys absent from the latest cycle, but its hidden state
            keeps every key it has ever held, as `last_by()` does; over a key set that keeps
            changing (an options chain, the day's movers) its memory grows with the keys seen.
            An `APPEND` table adds only rows whose identity it does not hold yet: the
            dataset's poll keys, followed on the snapshot and movers datasets by their update
            time, so an explicit `APPEND` on a snapshot records each new state of a symbol.
            A second row with the same identity in one cycle is skipped too, and a row is
            never appended twice, also after an absence (a symbol removed from a reactive
            table and added back). A later revision of an appended row that changes none of
            those columns is not appended; use `UPSERT` to follow revisions. The poller keeps
            one identity per appended row, so its memory grows with the table and no faster.
            A cycle that fails to publish appends nothing and remembers nothing. On a dataset
            with neither unique poll keys nor an update time (one keyed by the default
            `Symbol`), `APPEND` adds one row per symbol.
        """
        period = _interval(interval)
        chosen = self._spec.poll_mode if mode is None else _require_enum(mode, PollMode, "mode")
        if isinstance(self._request.symbols, SymbolTable):
            with entered(self._core.exec_ctx):
                read_symbols(self._spec, self._request.symbols.table)
        return self._core.rest_transport().poll(
            self._spec, self._request, interval=period, mode=chosen
        )


class StreamQuery(QueryBase):
    """A WebSocket dataset: four live shapes over one shared stream.

    Every verb of an equal query reuses one stream (one subscription, one publisher); each call
    returns a new, distinct table held in its own liveness scope. The stream stays subscribed
    while any result, or any table derived from one, is alive.

    The first verb of a stream decides its host (one REST request the first time a class and
    channel family streams in the session), connects if no connection to that host exists, and
    subscribes; the verb returns once the stream's tables exist, without waiting for the
    server's acknowledgement. A failure the server reports later (a refused topic, a rejected
    key, a row that does not decode) fails the stream's tables and shows in `m.streams()`; a
    verb whose stream fails while the verb opens it raises `MassiveError` naming the failure,
    and once a stream has failed the next verb of an equal query opens a fresh one. Explicit
    options contracts that would put more than 1,000 topics on the session's `/options`
    connection raise `ValueError` at once; those of a reactive symbol table are deferred
    instead (`Deferred` in `m.streams()`).

    A refreshing `Table` passed as symbols is reactive: the stream subscribes and unsubscribes
    as its rows change. A static (non-refreshing) `Table` is a fixed symbol list instead, read
    once when the verb is called, as `fetch()` and `history()` read one: the stream is the one
    an equal symbol list opens. A row that is not Massive's exact ticker raises `ValueError`
    in either kind; a static table with no symbol raises `ValueError` too, while an empty
    refreshing table opens its stream and subscribes as rows arrive.

    Built by a dataset method, such as `m.stocks.luld(...)`; immutable and reusable.

    Example:
        ::

            bands = m.stocks.luld(dm.ALL).last_by()  # the latest band of every ticker
            recent = m.stocks.imbalances(["AAPL", "MSFT"]).ring(1000)
    """

    __slots__ = ()

    @property
    def _channel(self) -> str:
        """The WebSocket channel: per timespan for bars, otherwise the dataset's channel.

        A stream query exists only for a timespan with a channel (`bar_query_class`), so the
        lookup always finds one.
        """
        return self._spec.channels[timespan_key(self._request.timespan)]

    def _stream_request(self) -> Request:
        """Return the request the stream is opened for, with a static symbol table read once.

        A static table cannot be listened to, so it is read now, like `fetch()` reads one
        (`_read_symbol_table`), and becomes the sorted tuple of its symbols; a refreshing table
        stays reactive. A static table with no symbol, or with a row that is not canonical,
        raises the `ValueError` of `_read_symbol_table`.

        Returns:
            The request.
        """
        request = self._request
        symbols = request.symbols
        if isinstance(symbols, SymbolTable) and not symbols.table.is_refreshing:
            return replace(request, symbols=_read_symbol_table(self._core, self._spec, symbols))
        return request

    def _stream(self, shape: Callable[[Table], Table], request: Request) -> Table:
        """Shape a live result from this query's stream, creating the stream if needed.

        The result is the shape, described last (`describe`): an append-only or ring table
        copies no attributes, so describing the blink table first would lose them. The schema
        is read before the stream is acquired, so the result is built under no library lock.
        A reactive symbol table is read first, so a row that is not Massive's exact ticker
        fails the call before any stream starts. A crypto pair takes its split from Massive's
        crypto ticker list when the stream starts (read once per session); a failed read, or a
        pair the list does not hold, raises `MassiveError` before any topic is subscribed.

        Args:
            shape: Turns the stream's blink table into the result's shape; the identity for
                `blink()`, whose described copy is then a new table of its own.
            request: The request from `_stream_request`.

        Returns:
            The tracked live result.

        A bad row of a symbol table raises the `ValueError` of `read_symbols` in
        `_tables/watchlist.py`.
        """
        spec = self._spec
        symbols = request.symbols  # a static table is already read: never read it twice
        if isinstance(symbols, SymbolTable):
            with entered(self._core.exec_ctx):
                read_symbols(spec, symbols.table)
        schema = self._core.dataset_schema(spec)
        return self._core.stream_result(
            spec,
            request,
            channel=self._channel,
            build=lambda blink: describe(shape(blink), spec, schema),
        )

    @_verb
    def blink(self) -> Table:
        """Return the stream as a blink table: each update cycle holds only that cycle's rows.

        Each call returns a new table, a described copy of the stream's blink table, with its
        own liveness scope, so two `blink()` results are released independently.

        A static symbol table is read once, now (see the class); one that holds no symbol, or a
        row that is not Massive's exact ticker, raises `ValueError`.

        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        Returns:
            A live blink table in the dataset's schema.
        """
        return self._stream(lambda blink: blink, self._stream_request())

    @_verb
    def append_only(self) -> Table:
        """Return every row received since the call, in arrival order.

        Memory grows with the stream; use `ring()` to bound it.

        A static symbol table is read once, now (see the class); one that holds no symbol, or a
        row that is not Massive's exact ticker, raises `ValueError`.

        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        Returns:
            A live append-only table in the dataset's schema.
        """
        return self._stream(blink_to_append_only, self._stream_request())

    @_verb
    def ring(self, capacity: int) -> Table:
        """Return the most recent `capacity` rows received.

        A `capacity` that is not an `int` raises `TypeError`, and one outside 1 to 2**31 - 1
        raises `ValueError` (`require_positive_int`).

        A static symbol table is read once, now (see the class); one that holds no symbol, or a
        row that is not Massive's exact ticker, raises `ValueError`.

        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        Args:
            capacity: How many rows to keep, from 1 to 2**31 - 1.

        Returns:
            A live ring table in the dataset's schema.
        """
        rows = require_positive_int("capacity", capacity, maximum=MAX_RING_CAPACITY)
        return self._stream(lambda blink: ring_table(blink, rows), self._stream_request())

    @_verb
    def last_by(self, by: str | Sequence[str] | None = None) -> Table:
        """Return the latest row per key, by default one row per `Symbol`.

        The underlying state keeps every key ever seen. With a reactive symbol table and
        `Symbol` among the keys, rows of symbols removed from it are filtered out; a symbol
        removed and re-added shows its last row until it ticks again. With keys that span
        symbols (`by="Exchange"`), rows are filtered before the aggregation, so each group's
        row is the latest from a symbol that was watched when it arrived; a group keeps that
        row after the symbol is removed, until a watched symbol ticks in the group. Every row
        of the table must hold Massive's exact ticker (`C:EURUSD`, not `EUR/USD`); a later row
        that does not fails the result (convert such a table with `dm.canonicalize_symbols`
        first). `last_by()` never seeds
        from REST: use `snapshots(symbols).poll(n)` for current state on every plan. A `by`
        that is not a `str` or a sequence of `str` raises `TypeError`; an empty `by` or an
        unknown column raises `ValueError`.

        A static symbol table is read once, now (see the class); one that holds no symbol, or a
        row that is not Massive's exact ticker, raises `ValueError`.

        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        Args:
            by: The key column or columns, positionally or by keyword as in Deephaven's
                `Table.last_by` (`q.last_by("Exchange")`); None means `"Symbol"`. Each column
                may appear once.

        Returns:
            A live table with one row per key, in the dataset's schema.
        """
        keys = self._last_by_keys(by)
        request = self._stream_request()
        symbols = request.symbols
        if not isinstance(symbols, SymbolTable):
            return self._stream(lambda blink: blink.last_by(list(keys)), request)
        watch = symbols.table
        if "Symbol" in keys:
            # One row per symbol: filtering after the aggregation evicts a removed symbol's rows.
            return self._stream(
                lambda blink: blink.last_by(list(keys)).where_in(watch, "Symbol"), request
            )
        # Groups span symbols: filter first, so a group's row is the latest of a watched symbol.
        return self._stream(
            lambda blink: blink.where_in(watch, "Symbol").last_by(list(keys)), request
        )

    def _last_by_keys(self, by: object) -> tuple[str, ...]:
        """Validate the `by` argument of `last_by`.

        Args:
            by: The argument as passed.

        Returns:
            The key columns.

        Raises:
            TypeError: If `by` or an element is not a `str`.
            ValueError: If no keys remain or a key is not a column of the dataset.
        """
        if by is None:
            keys: tuple[object, ...] = ("Symbol",)
        elif isinstance(by, str):
            keys = (by,)
        elif isinstance(by, list | tuple):
            keys = tuple(by)
        else:
            raise TypeError(f"by must be a str or a sequence of str, not {type(by).__name__}")
        if not keys:
            raise ValueError("by must name at least one column")
        if not all(isinstance(key, str) for key in keys):
            raise TypeError("by must contain only column names (str)")
        repeated = sorted({str(key) for key in keys if keys.count(key) > 1})
        if repeated:
            raise ValueError(f"by names {repeated} more than once")
        columns = self._core.dataset_schema(self._spec).column_names
        unknown = [key for key in keys if key not in columns]
        if unknown:
            raise ValueError(
                f"{self._spec.name} has no columns {unknown}; columns: {list(columns)}"
            )
        return tuple(str(key) for key in keys)


class HistoryQuery(QueryBase):
    """A dataset with a historical retrieval path: `history(start, end=None)`.

    Built by a dataset method, such as `m.options.bars(...)` with a timespan that has no
    stream; immutable and reusable. Every call of `history()` reads its range afresh.

    Example:
        ::

            q = m.options.bars("O:AAPL250117C00150000", dm.Timespan.HOUR)
            week = q.history("2025-01-06", "2025-01-10")  # whole days, both inclusive
    """

    __slots__ = ()

    @_verb
    def history(
        self,
        start: DateLike,
        end: DateLike | None = None,
        *,
        source: HistorySource = HistorySource.AUTO,
    ) -> Table:
        """Return the dataset's rows over a range of whole days or of exact instants.

        The two bounds are of one kind, never mixed:

        - **Dates** (`"2025-03-03"`, a `date`, a Java `LocalDate` such as `dh_today()`, or a
          day-precision `numpy.datetime64`) are whole days, inclusive at both ends:
          `history("2025-03-03", "2025-03-03")` is all of March 3. A day is a calendar day in
          the asset class's day zone: New York for stocks, options, and indices, UTC for forex
          and crypto. For futures a date names the trading session whose `Date`
          (`session_end_date`) it is, which opens at 17:00 Chicago time the evening before
          (earlier after a holiday).
        - **Instants** (an aware `datetime`, ISO text with an offset, or a Java `Instant` or
          `ZonedDateTime` such as `dh_now()` or a cell of an `Instant` column) are exact
          instants, half-open: rows with `start <= Timestamp < end` (for bars,
          `TimestampStart`). Equal instants give an empty table.

        `end=None`, the default, reads through now. A bar is returned only when its whole window
        lies inside the range and has ended: its `TimestampStart` lies in the range, and its
        `TimestampEnd` is no later than now or than the range's end (for dates, the end of the
        last day: the next midnight, 01:00 New York time for indices, 17:00 Chicago time on the
        last session's date for futures). Its window is an intraday or 1-day bar's own interval;
        for stocks, options, and futures, a week Monday to Friday and a month, quarter, or year
        its calendar period less its leading and trailing weekend days; for indices, which can
        update around the clock, the same periods as whole calendar periods, weekends included
        at both ends; for forex and crypto, which trade at weekends, whole calendar weeks
        (Monday to Sunday), months, quarters, and years; a bar of several units all of them,
        where several days or sessions end as late as holidays could push them (one weekday
        more, and one more for every five further days); and a futures bar named by session
        dates its sessions. So the current day's, week's, or month's bar is never returned, and
        `history("2025-03-05", "2025-03-20")` of monthly bars is empty. Datetime bounds keep
        only whole calendar days of 1-day bars: Monday 09:30 to Friday 16:00 New York time
        returns Tuesday to Thursday, so pass dates for daily bars. A futures bar of several
        sessions, weeks, or months named by dates is kept only when it fits the range whether
        Massive labels it with its first or its last session, which is not documented. There is
        no holiday calendar: a week whose Friday is a holiday is still that Monday-to-Friday
        week. There is no `limit`: the library pages internally.

        Every row says when it happened, its trading day, and when it arrived: a tick its
        `Timestamp`; a bar its window, `TimestampStart` and the exclusive `TimestampEnd`
        (join on the end, `aj(..., on=["Symbol", "Timestamp>=TimestampEnd"])`, so no bar is
        attached before it closed); its `Date`, ISO `yyyy-MM-dd` text, null for a bar that
        spans more than one date; and `TimestampInsert`, when its page or file block arrived
        (the download time). A bar of a day or longer starts at 00:00 of its window's first
        day in the class day zone (New York for stocks, options, and indices; UTC for forex
        and crypto), whatever the source, and ends at 00:00 after its last day (an hour later
        for indices, whose day is not verified); a futures bar keeps Massive's window start and
        ends at 17:00 Chicago time on its session date, or for a bar of several sessions, a week,
        or longer at the latest end its label allows, since Massive does not document whether
        it labels such a bar with its first or its last session.

        One call never mixes split bases: with `adjusted=True`, the default of the stock and
        option bars, it reads REST only and checks every page's `adjusted` echo; flat files
        are as traded, so they serve only `adjusted=False`. Massive adjusts for splits only,
        never for cash dividends. A table that joins adjusted history to live bars, which are
        as traded, jumps at the next split's execution date: build it again after a split.

        Flat files (S3) can serve a request when S3 credentials are configured, the dataset
        has files for it (bars: only 1-minute and 1-day bars, and 1-session bars for futures,
        unadjusted where the dataset has `adjusted`), and every day's file is published (from
        `flat_file_publish_cutoff`, New York time, after the day ends). REST can serve it when
        the dataset has a REST history endpoint and the symbols are explicit, or for `dm.ALL`
        daily bars with a grouped-daily endpoint. `AUTO` reads flat files when only they can,
        or when both can and the request has `flat_file_symbol_threshold` symbols or more (or
        `dm.ALL`); otherwise it uses REST. A bar of a day or longer stamped at a time of day,
        or on a day, Massive is not known to use for its class fails loudly, whichever source
        serves it, so the source never changes which day a bar is labelled with; `source=`
        picks a source explicitly. A reactive symbol table is read once, when the verb is
        called, and every source receives the same sorted canonical symbols; a row that is not
        Massive's exact ticker raises `ValueError`. Flat-file reads skip a missing
        weekend file silently and a missing weekday file (a holiday) with a WARNING. Futures
        trades and quotes named by session dates are read over REST one request series per
        weekday session and symbol; a long one-request-per-day download logs its request count
        first, at INFO.

        Side effects: network requests on the calling thread, and for flat files a pool of up
        to `max_concurrency` reader threads that end before the call returns. It blocks until
        the whole range is read.

        Args:
            start: The first day, or the first instant included: an ISO-8601 string, a `date`,
                a timezone-aware `datetime`, a Java `LocalDate`, `Instant`, or
                `ZonedDateTime`, or a day-precision `numpy.datetime64` (`DateLike`).
            end: The last day included, or the first instant excluded, of the same kind as
                `start`; None (the default) reads through now: the current instant for a
                datetime `start`, today in the day zone (for futures, the current session)
                for a date `start`.
            source: Where to read: `AUTO` (the default) picks flat files or REST, `REST` or
                `FLAT_FILES` forces one.

        Returns:
            A static table in the dataset's schema (see `columns()`), oldest first within each
            symbol over REST, in file order over flat files; empty, with every column, when
            no row lies in the range.

        Errors. `TypeError` if `source` is not a `HistorySource` or a bound is none of the
        `DateLike` types. `ValueError`, synchronously, before any request: a naive or
        unparseable bound, a `numpy.datetime64` finer than a day, or a Java instant finer than
        a microsecond; a date mixed with a datetime; `start` after `end` (or after now when
        `end` is None); a symbol table without a symbol, or with a row that is not canonical;
        futures trades or quotes named by session dates on a query that also filters
        `session_end_date` in any form; a source that cannot serve the request (`REST` without
        REST history or with `dm.ALL` and no grouped-daily endpoint; `FLAT_FILES` when a
        flat-file condition above fails; `AUTO` when neither can, with every violated condition
        listed); or a call on a Deephaven update-graph thread. `MassiveError` when a request
        fails (`EntitlementError` on HTTP 403, which for futures flat files names the venue and
        the `venue=` parameter; `AuthenticationError` when S3 rejects the flat-file
        credentials); when a page of adjusted bars echoes another basis than the request; when a
        symbol's later page answers HTTP 404, its data cut short (a symbol with no data at all
        simply returns no rows); when the flat file of the last requested day is missing within
        a day after its publication time ("... is not yet published (flat files land ~11:00 ET
        next day), or was a market holiday"; after that the day is skipped with a WARNING, as a
        holiday); when a futures range named by instants ends on a holiday and the next
        session's file, which holds the holiday's hours, is not published yet or does not exist
        within 7 days; when a bar of a day or longer is stamped at a time of day, or on a day,
        Massive is not known to use for its class (a crypto 1-day bar at midnight New York time,
        or a crypto week bar on a Sunday), naming the dataset, the source, and what to do
        instead; or once the session is closed.

        Example:
            ::

                q = m.crypto.trades("X:BTCUSD")
                day = q.history("2026-01-02", "2026-01-02")  # all of that UTC day
                hour = q.history(
                    datetime(2026, 1, 2, 14, tzinfo=UTC), datetime(2026, 1, 2, 15, tzinfo=UTC)
                )
                since = q.history("2026-01-02")  # through now
        """
        return self._logged_call(
            f"history({start!r}, {end!r}, source={source!r})",
            lambda call: self._history(start, end, source=source, call=call),
        )

    def _history(
        self, start: DateLike, end: DateLike | None, *, source: HistorySource, call: int
    ) -> Table:
        """Run `history()` as call `call`; see `history` for everything else.

        Args:
            start: As `history` takes it.
            end: As `history` takes it.
            source: As `history` takes it.
            call: The call id.

        Returns:
            The static table.
        """
        chosen = _require_enum(source, HistorySource, "source")
        first = parse_date_like(start, "start")
        last = None if end is None else parse_date_like(end, "end")
        symbols = self._request.symbols
        if isinstance(symbols, SymbolTable):
            symbols = _read_symbol_table(self._core, self._spec, symbols)
        # A dataset with a path per symbol class takes its days from its symbols' class.
        listed = symbols if isinstance(symbols, tuple) else ()
        request = replace(self._request, start=first, end=last, symbols=symbols)
        window = history_window(
            self._spec, request, start=first, end=last, classes=map(symbol_class, listed)
        )
        if chosen is HistorySource.REST:
            self._check_rest_history()
            if window.empty:
                return self._empty()
            return self._described(
                self._core.rest_transport().history(self._spec, request, window, call=call)
            )
        dates = partition_dates(self._spec, window)
        choice = choose_source(
            self._spec, request, dates, requested=chosen, config=self._core.config
        )
        if chosen is HistorySource.AUTO:
            logger.info(
                "%s history: AUTO chose %s (%s)",
                self._spec.name,
                choice.source.name,
                choice.reason,
            )
        if window.empty:
            return self._empty()
        if choice.source is HistorySource.REST:
            return self._described(
                self._core.rest_transport().history(self._spec, request, window, call=call)
            )
        return self._described(
            self._core.flat_file_transport().history(
                self._spec, request, window, dates=dates, call=call
            )
        )

    def _empty(self) -> Table:
        """Return the empty result of a range no row can lie in, without any request.

        Returns:
            A static table with every column of the dataset and no row, described as a
            result is (`_described`).
        """
        schema = self._core.dataset_schema(self._spec)
        with entered(self._core.exec_ctx):
            table = dh_arrow.to_table(schema.arrow_schema().empty_table())
        return self._described(table)

    def _check_rest_history(self) -> None:
        """Reject `history(source=REST)` where REST cannot serve the request.

        Raises:
            ValueError: If the dataset has no REST history endpoint, or the symbols are
                `dm.ALL` and the request is not daily bars with a grouped-daily endpoint
                (`rest_history_problem`); the message names `HistorySource.FLAT_FILES`.
        """
        problem = rest_history_problem(self._spec, self._request)
        if problem is not None:
            raise ValueError(f"{problem}; use source=dm.HistorySource.FLAT_FILES")


class StreamHistoryQuery(StreamQuery, HistoryQuery):
    """A WebSocket dataset that also has history: the stream verbs plus `history()`.

    Built by a dataset method, such as `m.stocks.bars(...)` with a one-second or one-minute
    timespan; immutable and reusable.

    Example:
        ::

            q = m.stocks.bars(["AAPL", "MSFT"])  # one-minute bars
            live = q.append_only()
            day = q.history("2025-03-03", "2025-03-03")
    """

    __slots__ = ()
