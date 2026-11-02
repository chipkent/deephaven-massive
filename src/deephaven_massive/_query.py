"""The four query classes, their verbs, and the choice of a dataset's class.

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
then calls one transport, so the transports only do I/O; under `AUTO` a flat-file read whose
last days are not published yet reads those days from REST too.
Each static verb (`fetch()`, `history()`) is one *call*: it takes a process-unique id from the
session's stream-id counter and logs `call 7 start: <query>.<verb>`, then `call 7 done: <rows>
rows in <s> s` or `call 7 failed after <s> s: <error>`, at DEBUG; the transports name their
worker threads after it (`massive #1 call-7 rest_0`), so every line of one call can be found.
With `wait=False` a static verb checks its arguments on the calling thread, then returns at once
a refreshing table that a library thread of its own fills in one update when the download
completes (`open_read` in `_tables/background_read.py`); that thread logs the call's outcome.
Live stream results are shaped and tracked by the session (`SessionCore.stream_result`), so
every stream verb shares one stream per (dataset, channel, symbol set).

A dataset's class follows from the transports its spec declares (`query_class`), and a
`bars()` call's from its timespan and multiplier (`bar_query_class`), so no spec names a
class and the dataset layer never refers to the query classes.

Users import the classes for type annotations from the public module
`deephaven_massive.queries`, and never construct them.

The module is one subject although it runs past 1,000 lines: the four classes build on one
base and on each other (`StreamHistoryQuery` is a `StreamQuery` and a `HistoryQuery`), so a
verb, and the choice of the class that offers it, read whole only beside all four.
"""

from __future__ import annotations

import functools
import logging
import time
from collections.abc import Set as AbstractSet
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Concatenate, Final, ParamSpec, TypeVar, cast

import numpy as np
from deephaven import arrow as dh_arrow
from deephaven.stream import blink_to_append_only
from deephaven.table_factory import ring_table

from deephaven_massive._config import require_positive_int, require_positive_number
from deephaven_massive._datasets.bar_periods import intraday
from deephaven_massive._datasets.catalog import timespan_key
from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._datasets.dates import bounds, parse_date_like
from deephaven_massive._datasets.history_source import (
    HISTORY_TRANSPORTS,
    choose_source,
    history_window,
    partition_dates,
    rest_history_problem,
    warn_unfilled,
)
from deephaven_massive._datasets.symbols import SymbolTable, symbol_class
from deephaven_massive._enums import (
    AssetClass,
    HistorySource,
    PollMode,
    Timespan,
    _require_enum,
)
from deephaven_massive._errors import (
    ErrorCause,
    MassiveError,
    failure_text,
    raises_through,
)
from deephaven_massive._massive import Transport
from deephaven_massive._tables.background_read import open_read
from deephaven_massive._tables.columns import (
    column_table,
    describe,
    recency_note,
    rest_recency_note,
)
from deephaven_massive._tables.diagnostics import shown_query
from deephaven_massive._tables.execution_context import entered, refuse_update_graph_thread
from deephaven_massive._tables.session_core import SessionCore
from deephaven_massive._tables.time_text import deephaven_time_text
from deephaven_massive._tables.transports import FlatFileTransport, RestTransport, StreamTransport
from deephaven_massive._tables.watchlist import (
    SymbolRowRule,
    read_symbols,
    rows_since_added,
    skipped_rows_text,
    watched_rows,
)
from deephaven_massive._types import ALL, override

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable, Sequence

    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._types import DateLike

__all__ = [
    "HistoryQuery",
    "Query",
    "QueryBase",
    "QueryCall",
    "StreamHistoryQuery",
    "StreamQuery",
    "bar_query_class",
    "query_class",
    "verbs",
]

logger = logging.getLogger(__name__)

MAX_RING_CAPACITY: Final = 2**31 - 1
"""The largest `ring()` capacity Deephaven accepts (a Java `int`)."""

_FROM_REQUEST: Final = frozenset(
    {"symbols", "timespan", "multiplier", "adjusted", "start", "end", "limit"}
)
"""The arguments a query's repr writes as its request holds them, normalized: the symbols sorted
and de-duplicated, the time bounds parsed. They keep these names in every dataset method."""

SYMBOLS_SHOWN: Final = 5
"""The most symbols a query's repr lists; `… (N more)` stands for the rest."""

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


@raises_through(require_positive_number)
def _interval(value: object) -> timedelta:
    """Validate a `poll()` interval.

    Args:
        value: Seconds as an `int` or `float`, or a `timedelta`.

    Returns:
        The interval as a `timedelta`.

    Raises:
        TypeError: If the value is neither a number nor a `timedelta`.
        ValueError: If the number is not finite and positive, or the interval overflows
            `timedelta`, or is not positive once rounded to microseconds.
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


def _serves(spec: DatasetSpec, verb: str) -> bool:
    """Tell whether a dataset's query has a verb, from the transports its spec declares.

    The query classes follow from the same transports (`query_class`, below): a fetch path
    gives `fetch()` and `poll()`, channels the stream verbs, and a history source `history()`. A
    `bars` dataset streams at some timespans only, which is enough to name it as the dataset that
    streams.

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
    `_namespaces/query_building.py`). `dm.datasets()` shows the result.

    Args:
        spec: The dataset.

    Returns:
        The verbs' names, such as `("fetch", "poll")` or `("blink", "append_only", "ring",
        "last_by", "history")`.
    """
    return tuple(verb for verb in _VERBS if _serves(spec, verb))


def _read_symbol_table(
    core: SessionCore, spec: DatasetSpec, symbols: SymbolTable, *, query: str, live: bool = False
) -> tuple[str, ...]:
    """Read a symbol table once, under the session's rule for a row that is not a ticker.

    The table is read as one consistent snapshot (`read_symbols` in `_tables/watchlist.py`),
    on the caller thread, inside the session's execution context, with no library lock held.
    A row that is not Massive's exact ticker follows `MassiveConfig.noncanonical_symbols`:
    under `RAISE` it fails the call; under `LOG` and `IGNORE` it is skipped (and logged once).
    Every live verb calls it before it starts; a static verb (`fetch()`, `history()`) uses the
    symbols it returns.

    Args:
        core: The session core, for its execution context and its `noncanonical_symbols`.
        spec: The dataset, whose asset class the rows must follow.
        symbols: The boxed table, static or refreshing.
        query: The `repr` of the query whose verb reads it, which the messages name.
        live: True for a live verb, which may start on a table with no row yet and follow
            it as rows arrive.

    Returns:
        The table's canonical symbols, sorted and de-duplicated; null and skipped rows are
        left out.

    Raises:
        ValueError: If the table has rows and none of them is Massive's ticker, so the result
            would follow nothing (`LOG` and `IGNORE`), or, for a static verb, if the table
            holds no symbol at all, as an empty symbol list does.

    Under `RAISE`, a bad row raises the `ValueError` of `read_symbols`, which names
    `dm.canonicalize_symbols`.
    """
    rule = SymbolRowRule.of(spec, core.config.noncanonical_symbols)
    with entered(core.exec_ctx):
        rows = read_symbols(rule, symbols.table, stream_id=None)
    if rows.skipped and not rows.symbols:
        raise ValueError(
            f"{query}: no row of the symbol table is a ticker this dataset takes, so the result "
            f"would follow nothing: {skipped_rows_text(rows.skipped)}"
        )
    if not rows.symbols and not live:
        raise ValueError(f"{query}: the symbol table holds no symbols")
    return rows.symbols


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
        level = logging.DEBUG if error.cause is ErrorCause.EXPECTED else logging.ERROR
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


def _set_place(item: object) -> tuple[int, str]:
    """Return where a set's item goes in a query's repr, so a set prints the same every run.

    Args:
        item: An item of a set argument, such as a `dm.FuturesVenue` member.

    Returns:
        An enum member's position in its enum, then the item's text; any other item sorts by
        its text alone.
    """
    place = list(type(item)).index(item) if isinstance(item, Enum) else 0
    return place, repr(item)


def _shown(value: object) -> str:
    """Write one argument value as a query's repr shows it, the way a user would type it.

    Args:
        value: The argument's normalized value.

    Returns:
        `dm.<Enum>.<MEMBER>` for an enum member; a date or time, Python, Java, or numpy, as
        quoted ISO-8601 text (`_shown_time`); a set as braces, enum members in their enum's
        order and other items sorted by their text; a list or tuple as a list; and anything
        else as its `repr`.
    """
    if isinstance(value, Enum):
        return f"dm.{type(value).__name__}.{value.name}"
    if isinstance(value, date | datetime):
        return repr(value.isoformat())
    if isinstance(value, np.datetime64) or hasattr(type(value), "jclassname"):
        return _shown_time(value)
    if isinstance(value, AbstractSet):
        items = sorted(value, key=_set_place)
        return "{" + ", ".join(map(_shown, items)) + "}"
    if isinstance(value, list | tuple):
        return "[" + ", ".join(map(_shown, value)) + "]"
    return repr(value)


def _shown_time(value: object) -> str:
    """Write a Java object or a `numpy.datetime64` as a query's repr shows it.

    A day or time argument the request does not hold parsed, such as a daily summary's `date`
    or a reference dataset's `as_of`, is kept as the caller passed it; a Java `LocalDate`,
    `Instant`, or `ZonedDateTime`, or a `numpy.datetime64`, would otherwise print as a jpy or
    numpy object reference.

    Args:
        value: A Java object (jpy sets `jclassname` on its type) or a `numpy.datetime64`.

    Returns:
        The value as `parse_date_like` reads it, as quoted ISO-8601 text: `'2025-01-02'` for a
        day, an instant with its offset otherwise; or its `repr` when it is no time
        `parse_date_like` reads (a Java object of another class, a `NaT`).
    """
    try:
        parsed = parse_date_like(cast("DateLike", value), "value")
    except (TypeError, ValueError):
        return repr(value)
    return repr(parsed.isoformat())


def _shown_symbols(symbols: object) -> str:
    """Write the `symbols` argument as a query's repr shows it, in its positional place.

    Args:
        symbols: The normalized symbols (a sorted tuple, `ALL`, or a symbol table), or, for a
            request that does not store them, the argument as passed (a string or a tuple).

    Returns:
        One symbol as `'AAPL'`; several as a list of their first `SYMBOLS_SHOWN`, followed by
        `… (N more)` when there are more; `ALL` as `dm.ALL`, and a symbol table, static or
        refreshing, as `<symbol table>`.
    """
    if symbols is ALL:
        return "dm.ALL"
    if isinstance(symbols, str):
        return repr(symbols)
    if not isinstance(symbols, tuple):
        return "<symbol table>"
    if len(symbols) == 1:
        return repr(symbols[0])
    listed = [repr(symbol) for symbol in symbols[:SYMBOLS_SHOWN]]
    if len(symbols) > SYMBOLS_SHOWN:
        listed.append(f"… ({len(symbols) - SYMBOLS_SHOWN} more)")
    return "[" + ", ".join(listed) + "]"


@dataclass(frozen=True, slots=True)
class QueryCall:
    """The dataset-method call that built a query, as its repr shows it.

    Created by the dataset method itself, through `shows_call` in
    `_namespaces/query_building.py`, when it returns its query; a query built any other way
    carries only its dataset's path. Not part of the query's equality: two calls that
    normalize to one request are one query, and since `text` writes the normalized values,
    they print alike. Immutable and thread-safe.

    Attributes:
        path: The method's path from the session, such as `"m.options.contracts"`.
        arguments: The arguments that differ from their defaults, in signature order, as
            `(public name, value as passed)` pairs; a list is kept as a tuple, a set as a
            `frozenset`, and a mapping as the tuple of its items.
        positional: The parameters the method requires, such as a bar's `timespan`, which the
            repr writes by position, as a user types them, never as `name=value`.
    """

    path: str
    arguments: tuple[tuple[str, object], ...] = ()
    positional: frozenset[str] = frozenset()

    def text(self, request: Request) -> str:
        """Write the call as a user would type it, from its query's normalized request.

        An argument in `_FROM_REQUEST` is written as the request holds it, when the dataset
        stored it there, and `filters` as the request's parsed filters (left out when there are
        none, as an empty mapping selects nothing); every other argument as passed.

        Args:
            request: The query's request.

        Returns:
            For example `m.options.contracts(underlying='AAPL')` or
            `m.stocks.bars(['AAPL', 'MSFT'], dm.Timespan.MINUTE, multiplier=5)`: `symbols`
            comes first and positionally (`_shown_symbols`), a required argument by position,
            `filters` as a dictionary, and every other argument as `name=value` (`_shown`).
        """
        parts = []
        for name, value in self.arguments:
            stored = getattr(request, name) if name in _FROM_REQUEST else None
            if name == "symbols":
                parts.append(_shown_symbols(value if stored is None else stored))
            elif name == "filters" and request.filters:
                pairs = ", ".join(f"{key!r}: {_shown(item)}" for key, item in request.filters)
                parts.append(f"filters={{{pairs}}}")
            elif name in self.positional:
                parts.append(_shown(value if stored is None else stored))
            elif name != "filters":
                parts.append(f"{name}={_shown(value if stored is None else stored)}")
        return f"{self.path}({', '.join(parts)})"


@dataclass(frozen=True, repr=False, slots=True)
class QueryBase:
    """The state every query class shares: the session, the dataset, and the arguments.

    Frozen and hashable; equality includes the concrete class, so a `StreamQuery` never equals
    a `HistoryQuery`, and covers the dataset and the normalized request, never the call that
    built it. Queries have no public attributes: the verbs and `columns()` are the whole API,
    and the `repr` is the call that built the query (`QueryCall`).
    """

    _core: SessionCore
    _spec: DatasetSpec
    _request: Request
    _call: QueryCall = field(compare=False)

    @raises_through(refuse_update_graph_thread)
    def columns(self) -> Table:
        """List this dataset's columns, their types, and what each one means.

        Every verb of the dataset returns these columns in this order, whichever transport
        serves it; a column the serving transport does not supply is null. A value Massive sends
        that does not fit its column is null, logged once, and named in the result's
        `SchemaDrift` in `m.streams()`; a row missing its symbol or time is dropped. A symbol
        spelling the library cannot read keeps its row, with a null `Symbol` and the text in
        `RawSymbol`. The table is built
        from the committed generated schemas: no request is made, no stream is opened, and it
        works on a closed session.

        Returns:
            A static table with one row per column, in result column order:
            `Name` (String), the column name; `Type` (String), its Deephaven type (`long`,
            `int`, `double`, `boolean`, `String`, `Instant`, `long[]`, or `String[]`; every
            calendar date, such as `Date`, is ISO `yyyy-MM-dd` text, a `String`);
            `Description` (String), what the column holds, empty when Massive gives no
            description; and `SuppliedBy` (String), the transports whose sources fill the
            column for this dataset, spelled `REST`, `flat files`, and `WebSocket`, in that
            order, joined by ", " (`fetch()` and `poll()` are served by REST, the stream verbs
            by WebSocket, and `history()` by REST or flat files). A dataset has only the
            columns some transport of its own fills, so every `SuppliedBy` names one.

        Raises:
            ValueError: On a Deephaven update-graph thread, for example inside a table
                listener, like every verb.

        Example:
            ::

                cols = m.stocks.trades(["AAPL"]).columns()
                # The columns a live stream leaves null, such as Correction.
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
        the current state on a paid plan; for `history()` of a dataset with a time axis, its
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
            background: True for `wait=False`: `run` returns the table a background read
                fills, and the read's thread logs the call's outcome, so no `done` line is
                logged here.

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

    def _static_request(self) -> Request:
        """Return the request with a symbol table read once, for a static verb.

        Returns:
            The request; for a symbol table, with its symbols (`_read_symbol_table`).
        """
        request = self._request
        if isinstance(request.symbols, SymbolTable):
            symbols = _read_symbol_table(self._core, self._spec, request.symbols, query=repr(self))
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
        if wait:
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
    @raises_through(
        SessionCore.run_verb,
        refuse_update_graph_thread,
        _read_symbol_table,
        RestTransport.fetch,
        _wait,
    )
    def fetch(self, *, wait: bool = True) -> Table:
        """Return every row the dataset's endpoint returns, fully paginated.

        The requests run on the calling thread, or, for an endpoint that takes one symbol per
        request, on up to `max_concurrency` threads. A `limit` the query was built with caps the
        rows (README.md#sort-order-limit); a symbol table is read once, now. A symbol with no
        data (HTTP 404 on its first request) returns no rows, with a WARNING; HTTP 404 on a later
        page raises `MassiveError`, since its data was cut short. Current data of a
        subscription not in `MassiveConfig.delayed_subscriptions` must be real-time: a response
        Massive labels `DELAYED` raises; a listed subscription's data is returned as the plan
        serves it, and the table's description says so (README.md#real-time-or-delayed).

        Args:
            wait: True (the default) blocks and returns a static table; False returns at once a
                refreshing table a background read fills in one update. See
                README.md#verb-arguments.

        Returns:
            A static table in the dataset's schema; with `wait=False`, a refreshing table
            filled once in the background.

        Raises:
            TypeError: If `wait` is not a `bool`.
            ValueError: Before any request: on a Deephaven update-graph thread (inside a table
                listener, say); if a path the call reads does not take a parameter or filter the
                query set; or if a symbol table holds no symbol, only rows that are not
                Massive's exact tickers, or any such row under `noncanonical_symbols=RAISE`.
            AuthenticationError: If Massive rejects the API key (HTTP 401). With `wait=False`
                this and every other request failure fails the table instead.
            EntitlementError: If the plan does not include the endpoint (HTTP 403), or Massive
                labels the data `DELAYED` for a subscription not in
                `MassiveConfig.delayed_subscriptions`; the message names the line to add.
            MassiveError: If the session is closed, a rate limit outlives the retries, every
                row of a page is unreadable, a symbol's later page answers HTTP 404, or a
                request fails in any other way.

        Example:
            ::

                divs = m.stocks.dividends(["AAPL", "MSFT"]).fetch(wait=False)
        """
        background = not _wait(wait)
        return self._logged_call(
            "fetch(wait=False)" if background else "fetch()",
            functools.partial(self._fetch, wait=wait),
            background=background,
        )

    def _fetch(self, call: int, *, wait: bool) -> Table:
        """Run `fetch()` as call `call`.

        Args:
            call: The call id.
            wait: As `fetch` takes it.

        Returns:
            The static table, or the refreshing one a background read fills.
        """
        spec = self._spec
        request = self._static_request()
        rest = self._core.rest_transport()
        if not wait:  # a parameter error raises now, as it would before the first request
            rest.check_fetch(spec, request)
        return self._result(
            call,
            request,
            wait=wait,
            transport=Transport.REST,
            note=rest_recency_note(spec, self._core.config.delayed_subscriptions),
            run=lambda stop: rest.fetch(spec, request, call=call, stop=stop),
        )

    @_verb
    @raises_through(
        SessionCore.run_verb,
        refuse_update_graph_thread,
        _interval,
        _require_enum,
        read_symbols,
        RestTransport.poll,
    )
    def poll(self, interval: float | timedelta, *, mode: PollMode = PollMode.AUTO) -> Table:
        """Return a live table that re-runs `fetch()` every `interval`.

        Each cycle reads the whole query again; a symbol table is re-read each cycle, and a row
        that is not Massive's exact ticker is skipped and listed in `m.streams()` (or fails the
        table under `noncanonical_symbols=RAISE`). A feed (a dataset whose default mode is
        `APPEND`: news and filings) needs a `start` or a `limit`. The poller runs while the
        table or anything derived from it is in use (README.md#live-tables). Once the table
        exists, an authentication, entitlement, or schema failure, a null or NaN poll key, or a
        response Massive labels `DELAYED` for a subscription not in
        `MassiveConfig.delayed_subscriptions` (README.md#real-time-or-delayed), fails it and
        ends the poller; the table's description and its `m.streams()` row name its recency;
        any other failed cycle shows in `m.streams()`'s `Error`, logs a WARNING, and the next
        cycle runs. `TimestampInsert` is the latest cycle's time.

        Args:
            interval: Seconds between cycles, or a `timedelta`; must be positive.
            mode: How cycles combine. `AUTO` (the default) is the dataset's own mode, which its
                page in `docs/datasets` names: `REPLACE` keeps the latest cycle by key,
                `UPSERT` keeps every key ever seen at its latest row, `APPEND` adds only rows
                whose identity (the poll keys, plus the update time on snapshots and movers) is
                new. See
                README.md#verb-arguments.

        Returns:
            A live table: keyed for `REPLACE` and `UPSERT`, append-only for `APPEND`. Its
            hidden state keeps every key or identity it has held, so memory grows with the keys
            seen.

        Raises:
            TypeError: If `interval` is not a number or a `timedelta`, or `mode` is not a
                `dm.PollMode`.
            ValueError: Before the table exists: on a Deephaven update-graph thread; if
                `interval` is not positive; if a feed's query has neither `start` nor `limit`;
                if a symbol table holds only rows that are not Massive's exact tickers, or any
                under `noncanonical_symbols=RAISE`; or if a path does not take a parameter or
                filter the query set.
            AuthenticationError: If Massive rejects the API key while the call reads its
                crypto ticker list, to split a crypto ticker a path takes as two currencies.
            EntitlementError: If the plan does not include that list.
            MassiveError: If the session is closed, or a crypto ticker cannot be split into
                its currencies.
        """
        period = _interval(interval)
        asked = _require_enum(mode, PollMode, "mode")
        chosen = self._spec.poll_mode if asked is PollMode.AUTO else asked
        request = self._request
        if self._spec.is_feed and request.start is None and request.limit is None:
            name = self._spec.name
            logger.debug("%r poll() refused: a feed's poll needs a start or a limit", self)
            raise ValueError(
                f"{self!r}.poll() needs a start or a limit: without either, every cycle "
                f"re-reads the whole archive Massive keeps. Build the query with a start, such as "
                f"m.{name}(..., start=dh_today(to_j_time_zone('America/New_York'))), or with "
                "a limit and a newest-first order, then call poll(). A fixed start keeps "
                "widening the window on a poll that runs for days"
            )
        if isinstance(self._request.symbols, SymbolTable):
            _read_symbol_table(
                self._core, self._spec, self._request.symbols, query=repr(self), live=True
            )
        logger.debug(
            "%s poll(): every %.3f s, mode %s -> %s",
            shown_query(repr(self)),
            period.total_seconds(),
            asked.name,
            chosen.name,
        )
        return self._core.rest_transport().poll(
            self._spec, self._request, interval=period, mode=chosen, query=repr(self)
        )


class StreamQuery(QueryBase):
    """A WebSocket dataset: four live shapes over one shared stream.

    Every verb of an equal query reuses one stream (one subscription, one publisher); each call
    returns a new, distinct table held in its own liveness scope. The stream stays subscribed
    while any result, or any table derived from one, is alive.

    The first verb of a stream decides its host (one REST request the first time a class and
    channel family streams in the session), connects if no connection to that host exists, and
    subscribes; the verb returns once the stream's tables exist, without waiting for the
    server's acknowledgement. A topic Massive never acknowledges is re-sent, and `m.streams()`
    names it in `Error` until it is. A failure the server reports later (a refused topic, a rejected
    key) fails the stream's tables and shows in `m.streams()`, while a row that does not decode
    is dropped and logged and the stream goes on; a
    verb whose stream fails while the verb opens it raises `MassiveError` naming the failure,
    and once a stream has failed the next verb of an equal query opens a fresh one. Explicit
    options contracts that would put more than 1,000 topics on the session's `/options`
    connection raise `ValueError` at once; those of a reactive symbol table are deferred
    instead (`Deferred` in `m.streams()` counts the stream's own deferred topics, one per
    contract, since a stream reads one channel).

    A refreshing `Table` passed as symbols is reactive: the stream subscribes and unsubscribes
    as its rows change. A static (non-refreshing) `Table` is a fixed symbol list instead, read
    once when the verb is called, as `fetch()` and `history()` read one: the stream is the one
    an equal symbol list opens. A row that is not Massive's exact ticker follows
    `MassiveConfig.noncanonical_symbols`: under `LOG`, the default, and `IGNORE` it is never
    subscribed, and the stream's `m.streams()` `Error` lists it until it is fixed or deleted;
    under `RAISE` it raises `ValueError` at the call, or fails the stream when added later. A
    table whose rows are all bad, or a static table with no symbol, raises `ValueError`, while
    an empty refreshing table opens its stream and subscribes as rows arrive.

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
        stays reactive. A static table with no symbol, with only rows that are not canonical,
        or with any such row under `noncanonical_symbols=RAISE`, raises the `ValueError` of
        `_read_symbol_table`; under `LOG` and `IGNORE` such a row is skipped.

        Returns:
            The request.
        """
        request = self._request
        symbols = request.symbols
        if isinstance(symbols, SymbolTable) and not symbols.table.is_refreshing:
            return replace(
                request,
                symbols=_read_symbol_table(self._core, self._spec, symbols, query=repr(self)),
            )
        return request

    def _stream(self, shape: Callable[[Table], Table], request: Request, *, verb: str) -> Table:
        """Shape a live result from this query's stream, creating the stream if needed.

        The result is the shape, described last (`describe`): an append-only or ring table
        copies no attributes, so describing the blink table first would lose them. The schema
        is read before the stream is acquired, so the result is built under no library lock.
        A reactive symbol table is read first, so a row that is not Massive's exact ticker
        fails the call before any stream starts. A crypto pair takes its split from Massive's
        crypto ticker list when the stream starts (read once per session); a failed read, or a
        pair the list does not hold, raises `MassiveError` before any topic is subscribed. A
        pair a reactive table adds later is skipped instead, and listed in `m.streams()`.

        Args:
            shape: Turns the stream's blink table into the result's shape; the identity for
                `blink()`, whose described copy is then a new table of its own.
            request: The request from `_stream_request`.
            verb: The verb's name, such as `"last_by"`, for the log line of a stream's open.

        Returns:
            The tracked live result.

        A symbol table whose rows are all bad, or a bad row under `RAISE`, raises the
        `ValueError` of `_read_symbol_table`.
        """
        spec = self._spec
        symbols = request.symbols  # a static table is already read: never read it twice
        if isinstance(symbols, SymbolTable):
            _read_symbol_table(self._core, spec, symbols, query=repr(self), live=True)
        schema = self._core.dataset_schema(spec)
        return self._core.stream_result(
            spec,
            request,
            channel=self._channel,
            query=repr(self),
            verb=verb,
            build=lambda handle: describe(
                shape(handle.blink),
                spec,
                schema,
                note=recency_note(handle.recency, host=handle.host),
            ),
        )

    @_verb
    @raises_through(
        SessionCore.run_verb,
        refuse_update_graph_thread,
        _read_symbol_table,
        read_symbols,
        SessionCore.stream_result,
        StreamTransport.open_stream,
    )
    def blink(self) -> Table:
        """Return the stream as a blink table: each update cycle holds only that cycle's rows.

        Each call returns a new table, a described copy of the stream's blink table, with its
        own liveness scope, so two `blink()` results are released independently.

        A static symbol table is read once, now (see the class).

        Returns:
            A live blink table in the dataset's schema.

        Raises:
            ValueError: On a Deephaven update-graph thread (for example inside a table
                listener); if a static symbol table holds no symbol, or a symbol table only
                rows that are not Massive's exact tickers, or any such row under
                `noncanonical_symbols=OnError.RAISE`; or if explicit options contracts would put
                more than 1,000 topics on the session's `/options` connection; or if the
                stream is a fair-market-value stream of a subscription in
                `MassiveConfig.delayed_subscriptions` (FMV is real-time only).
            AuthenticationError: If Massive rejects the API key on the request that decides
                the WebSocket host.
            EntitlementError: If the dataset's subscription is not in
                `MassiveConfig.delayed_subscriptions` and Massive shows its plan's data is
                delayed; the message names the line to add (README.md#real-time-or-delayed).
            MassiveError: If the session is closed; if the stream fails while the verb opens
                it, naming the failure; or if Massive's crypto ticker list cannot be read or
                does not hold a crypto pair.
        """
        return self._stream(lambda blink: blink, self._stream_request(), verb="blink")

    @_verb
    @raises_through(
        SessionCore.run_verb,
        refuse_update_graph_thread,
        _read_symbol_table,
        read_symbols,
        SessionCore.stream_result,
        StreamTransport.open_stream,
    )
    def append_only(self) -> Table:
        """Return every row received since the call, in arrival order.

        Memory grows with the stream; use `ring()` to bound it.

        A static symbol table is read once, now (see the class).

        Returns:
            A live append-only table in the dataset's schema.

        Raises:
            ValueError: On a Deephaven update-graph thread (for example inside a table
                listener); if a static symbol table holds no symbol, or a symbol table only
                rows that are not Massive's exact tickers, or any such row under
                `noncanonical_symbols=OnError.RAISE`; or if explicit options contracts would put
                more than 1,000 topics on the session's `/options` connection; or if the
                stream is a fair-market-value stream of a subscription in
                `MassiveConfig.delayed_subscriptions` (FMV is real-time only).
            AuthenticationError: If Massive rejects the API key on the request that decides
                the WebSocket host.
            EntitlementError: If the dataset's subscription is not in
                `MassiveConfig.delayed_subscriptions` and Massive shows its plan's data is
                delayed; the message names the line to add (README.md#real-time-or-delayed).
            MassiveError: If the session is closed; if the stream fails while the verb opens
                it, naming the failure; or if Massive's crypto ticker list cannot be read or
                does not hold a crypto pair.
        """
        return self._stream(blink_to_append_only, self._stream_request(), verb="append_only")

    @_verb
    @raises_through(
        SessionCore.run_verb,
        refuse_update_graph_thread,
        _read_symbol_table,
        read_symbols,
        SessionCore.stream_result,
        StreamTransport.open_stream,
        require_positive_int,
    )
    def ring(self, capacity: int) -> Table:
        """Return the most recent `capacity` rows received.

        A static symbol table is read once, now (see the class).

        Args:
            capacity: How many rows to keep, from 1 to 2**31 - 1.

        Returns:
            A live ring table in the dataset's schema.

        Raises:
            TypeError: If `capacity` is not an `int`.
            ValueError: On a Deephaven update-graph thread (for example inside a table
                listener); if a static symbol table holds no symbol, or a symbol table only
                rows that are not Massive's exact tickers, or any such row under
                `noncanonical_symbols=OnError.RAISE`; or if explicit options contracts would put
                more than 1,000 topics on the session's `/options` connection; or if the
                stream is a fair-market-value stream of a subscription in
                `MassiveConfig.delayed_subscriptions` (FMV is real-time only).
                Also if `capacity` is outside 1 to 2**31 - 1.
            AuthenticationError: If Massive rejects the API key on the request that decides
                the WebSocket host.
            EntitlementError: If the dataset's subscription is not in
                `MassiveConfig.delayed_subscriptions` and Massive shows its plan's data is
                delayed; the message names the line to add (README.md#real-time-or-delayed).
            MassiveError: If the session is closed; if the stream fails while the verb opens
                it, naming the failure; or if Massive's crypto ticker list cannot be read or
                does not hold a crypto pair.
        """
        rows = require_positive_int("capacity", capacity, maximum=MAX_RING_CAPACITY)
        return self._stream(
            lambda blink: ring_table(blink, rows), self._stream_request(), verb="ring"
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
        keys: tuple[object, ...]
        if isinstance(by, str):
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

    @_verb
    @raises_through(
        SessionCore.run_verb,
        refuse_update_graph_thread,
        _read_symbol_table,
        read_symbols,
        SessionCore.stream_result,
        StreamTransport.open_stream,
        _last_by_keys,
    )
    def last_by(self, by: str | Sequence[str] = "Symbol") -> Table:
        """Return the latest row per key, by default one row per `Symbol`.

        The underlying state keeps every key ever seen. With a reactive symbol table and
        `Symbol` among the keys, a symbol's row shows only while the table holds the symbol and
        only once a row has arrived since the symbol last joined it: a symbol removed and added
        again shows no row until it ticks again. With keys that span symbols
        (`by="Exchange"`), each group's row is the latest from a symbol watched when it arrived,
        and removing that symbol does not remove the group's row. A table row that is not
        Massive's exact ticker is skipped and listed in `m.streams()` (convert with
        `dm.canonicalize_symbols`). `last_by()` never seeds from REST: for current state, use
        `snapshots(symbols).poll(n)`, which needs a plan that includes snapshots
        (README.md#plans-and-errors). A static symbol table is read once, now.

        Args:
            by: The key column or columns, positionally or by keyword as in Deephaven's
                `Table.last_by` (`q.last_by("Exchange")`); `"Symbol"` by default. Each column
                may appear once.

        Returns:
            A live table with one row per key, in the dataset's schema.

        Raises:
            TypeError: If `by` is not a `str` or a sequence of `str`.
            ValueError: On a Deephaven update-graph thread (for example inside a table
                listener); if a static symbol table holds no symbol, or a symbol table only
                rows that are not Massive's exact tickers, or any such row under
                `noncanonical_symbols=OnError.RAISE`; or if explicit options contracts would put
                more than 1,000 topics on the session's `/options` connection; or if the
                stream is a fair-market-value stream of a subscription in
                `MassiveConfig.delayed_subscriptions` (FMV is real-time only).
                Also if `by` is empty, repeats a column, or names a column the dataset does
                not have.
            AuthenticationError: If Massive rejects the API key on the request that decides
                the WebSocket host.
            EntitlementError: If the dataset's subscription is not in
                `MassiveConfig.delayed_subscriptions` and Massive shows its plan's data is
                delayed; the message names the line to add (README.md#real-time-or-delayed).
            MassiveError: If the session is closed; if the stream fails while the verb opens
                it, naming the failure; or if Massive's crypto ticker list cannot be read or
                does not hold a crypto pair.
        """
        keys = self._last_by_keys(by)
        request = self._stream_request()
        symbols = request.symbols
        if not isinstance(symbols, SymbolTable):
            return self._stream(lambda blink: blink.last_by(list(keys)), request, verb="last_by")
        watch = symbols.table
        if "Symbol" in keys:
            # One row per symbol: filtering after the aggregation evicts a removed symbol's rows,
            # and hides the row a re-added symbol showed before it left.
            return self._stream(
                lambda blink: rows_since_added(blink.last_by(list(keys)), watch),
                request,
                verb="last_by",
            )
        # Groups span symbols: filter first, so a group's row is the latest of a watched symbol.
        return self._stream(
            lambda blink: watched_rows(blink, watch).last_by(list(keys)),
            request,
            verb="last_by",
        )


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

    @_verb
    @raises_through(
        SessionCore.run_verb,
        refuse_update_graph_thread,
        _require_enum,
        parse_date_like,
        _read_symbol_table,
        bounds,
        choose_source,
        _check_rest_history,
        RestTransport.history,
        FlatFileTransport.history,
        _wait,
    )
    def history(
        self,
        start: DateLike,
        end: DateLike | None = None,
        *,
        source: HistorySource = HistorySource.AUTO,
        wait: bool = True,
    ) -> Table:
        """Return the dataset's rows over a range of whole days or of exact instants.

        Dates (`"2025-03-03"`, a `date`, `dh_today()`) are whole days in the asset class's time
        zone, both ends included; aware datetimes and instants are exact, the end excluded
        (README.md#start-and-end). A bar is returned only when its whole window lies in the
        range and has ended (README.md#bars). Rows come from REST or the flat files; under
        `AUTO`, the days whose flat files are missing, and files the plan lacks, are read
        from REST, so the call fails only when no source can serve it
        (README.md#history-sources). The table's description names the source and why. A
        symbol table is read once, now. A bar with an unexpected stamp is served, with a
        WARNING, never refused. History is past data, so `MassiveConfig.delayed_subscriptions`
        does not affect it (README.md#real-time-or-delayed).

        Args:
            start: The first day, or the first instant included (`DateLike`).
            end: The last day included, or the first instant excluded; None reads through now.
            source: `AUTO` (the default) picks; `REST` or `FLAT_FILES` forces one.
            wait: True (the default) blocks; False returns a table a background read fills.
                See README.md#verb-arguments.

        Returns:
            A static table in the dataset's schema, empty when no row lies in the range; with
            `wait=False`, a refreshing table filled once in the background.

        Raises:
            TypeError: If `source` is not a `dm.HistorySource`, a bound is not `DateLike`, or
                `wait` is not a `bool`.
            ValueError: Before any request: a naive, unparseable, or `NaT` bound; a date mixed
                with a datetime; `start` after `end` or after now; an empty or unreadable symbol
                table; futures session dates with a `session_end_date` filter; a source that
                cannot serve the request (every violated condition is listed); or a call on a
                Deephaven update-graph thread.
            AuthenticationError: If Massive or S3 rejects the credentials. With `wait=False`
                every request error fails the table instead.
            EntitlementError: If the plan does not include the data (HTTP 403, or S3's 403
                under `FLAT_FILES` or when REST cannot serve the read).
            MassiveError: If the session is closed; a request fails; adjusted bars echo the
                other basis; a symbol's later page answers HTTP 404; or a day's flat file is not
                published yet (no later day's file exists), or a week or more old and absent,
                under `FLAT_FILES` or when REST cannot serve the read.

        Example:
            ::

                day = m.crypto.trades("X:BTCUSD").history("2026-01-02", "2026-01-02")
        """
        background = not _wait(wait)
        return self._logged_call(
            f"history({start!r}, {end!r}, source={source!r}{', wait=False' if background else ''})",
            lambda call: self._history(start, end, source=source, call=call, wait=wait),
            background=background,
        )

    def _history(
        self,
        start: DateLike,
        end: DateLike | None,
        *,
        source: HistorySource,
        call: int,
        wait: bool,
    ) -> Table:
        """Run `history()` as call `call`; see `history` for everything else.

        Everything that needs no request is checked here, on the calling thread, before the
        transport call runs or a background read starts.

        Args:
            start: As `history` takes it.
            end: As `history` takes it.
            source: As `history` takes it.
            call: The call id.
            wait: As `history` takes it.

        Returns:
            The static table, or the refreshing one a background read fills.
        """
        chosen = _require_enum(source, HistorySource, "source")
        first = parse_date_like(deephaven_time_text(start, "start"), "start")
        last = (
            None
            if end is None
            else parse_date_like(deephaven_time_text(end, "end"), "end", upper=True)
        )
        symbols = self._request.symbols
        if isinstance(symbols, SymbolTable):
            symbols = _read_symbol_table(self._core, self._spec, symbols, query=repr(self))
        # A dataset with a path per symbol class takes its days from its symbols' class.
        listed = symbols if isinstance(symbols, tuple) else ()
        request = replace(self._request, start=first, end=last, symbols=symbols)
        window = history_window(
            self._spec, request, start=first, end=last, classes=map(symbol_class, listed)
        )
        spec = self._spec
        if chosen is HistorySource.REST:
            self._check_rest_history()
            note = "Read from REST: requested with source=dm.HistorySource.REST."
            return self._rest_history(call, request, window, wait=wait, note=note)
        dates = partition_dates(spec, window)
        choice = choose_source(spec, request, requested=chosen, config=self._core.config)
        label = TRANSPORT_LABELS[HISTORY_TRANSPORTS[choice.source]]
        if chosen is HistorySource.AUTO:
            logger.info(
                "%s history: AUTO chose %s (%s)", spec.name, choice.source.name, choice.reason
            )
            note = f"Read from {label}: AUTO chose it ({choice.reason})."
        else:
            note = f"Read from {label}: requested with source=dm.HistorySource.FLAT_FILES."
        if chosen is HistorySource.AUTO and not window.empty:
            warn_unfilled(spec, choice, catalog=self._core.catalog, warnings=self._core.warnings)
        if choice.source is HistorySource.REST:
            return self._rest_history(call, request, window, wait=wait, note=note)
        forced = chosen is HistorySource.FLAT_FILES
        return self._flat_history(
            call, request, window, dates=dates, wait=wait, note=note, forced=forced
        )

    def _flat_history(
        self,
        call: int,
        request: Request,
        window: HistoryWindow,
        *,
        dates: Sequence[date],
        wait: bool,
        note: str,
        forced: bool,
    ) -> Table:
        """Run a `history()` call that reads the flat files, now or in the background.

        A blocking read's description names the source that served it, REST included, after
        the read (`FlatFileTransport.read_history`). A background read's is written when the
        read starts, so under `AUTO`, when REST can serve the request, it says a day the flat
        files cannot serve is read from REST, and the read's WARNING names the days.

        Args:
            call: The call id.
            request: The request with static symbols and the parsed bounds.
            window: The call's window.
            dates: The partition dates of the files the window needs.
            wait: As `history` takes it.
            note: The description's source sentence of the choice.
            forced: True for `source=FLAT_FILES`.

        Returns:
            The static table, or the refreshing one a background read fills.
        """
        spec = self._spec
        files = self._core.flat_file_transport()
        if window.empty:
            return self._result(
                call,
                request,
                wait=wait,
                transport=Transport.FLAT_FILES,
                note=note,
                run=lambda _stop: self._empty_rows(),
            )
        if wait:
            table, served = files.read_history(
                spec, request, window, dates=dates, call=call, forced=forced
            )
            return self._described(table, note=note if served is None else served)
        if not forced and rest_history_problem(spec, request) is None:
            # The description is written before the read: it can only say what may happen.
            note = f"{note} A day the flat files cannot serve is read from REST; the log names it."
        return self._result(
            call,
            request,
            wait=wait,
            transport=Transport.FLAT_FILES,
            note=note,
            run=lambda stop: files.read_history(
                spec, request, window, dates=dates, call=call, forced=forced, stop=stop
            )[0],
        )

    def _rest_history(
        self, call: int, request: Request, window: HistoryWindow, *, wait: bool, note: str
    ) -> Table:
        """Read `history()` from REST, now or in the background.

        Args:
            call: The call id.
            request: The request with static symbols and the parsed bounds.
            window: The call's window; an empty one reads nothing.
            wait: As `history` takes it.
            note: The sentence that names the source in the table description.

        Returns:
            The static table, or the refreshing one a background read fills.
        """
        spec = self._spec
        rest = self._core.rest_transport()
        if not wait and not window.empty:  # a parameter error raises now
            rest.check_history(spec, request, window)
        return self._result(
            call,
            request,
            wait=wait,
            transport=Transport.REST,
            note=note,
            run=lambda stop: (
                self._empty_rows()
                if window.empty
                else rest.history(spec, request, window, call=call, stop=stop)
            ),
        )

    def _empty_rows(self) -> Table:
        """Return the static rows of a range no row can lie in, without any request.

        Returns:
            A static table with every column of the dataset and no row.
        """
        schema = self._core.dataset_schema(self._spec)
        with entered(self._core.exec_ctx):
            return dh_arrow.to_table(schema.arrow_schema().empty_table())


class StreamHistoryQuery(StreamQuery, HistoryQuery):
    """A WebSocket dataset that also has history: the stream verbs plus `history()`.

    Built by a dataset method, such as `m.stocks.bars(...)` with a one-second or one-minute
    timespan; immutable and reusable.

    Example:
        ::

            q = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.MINUTE)
            live = q.append_only()
            day = q.history("2025-03-03", "2025-03-03")
    """

    __slots__ = ()


def query_class(spec: DatasetSpec) -> type[QueryBase]:
    """Return the query class of a dataset, derived from the transports its spec declares.

    A dataset's verbs are exactly what its transports can serve, so the class follows from the
    spec: channels give the stream verbs, a history source (a REST history or grouped-daily
    path, or flat-file prefixes) gives `history()`, and a fetch path gives `fetch()`. Each
    namespace factory's return annotation is this class, which the namespace tests check.

    | Channels | History | Fetch | Class                |
    |----------|---------|-------|----------------------|
    | no       | no      | yes   | `Query`              |
    | yes      | no      | no    | `StreamQuery`        |
    | no       | yes     | no    | `HistoryQuery`       |
    | yes      | yes     | no    | `StreamHistoryQuery` |

    `DatasetSpec` rejects every other combination when it is created: a fetch path is a
    dataset of its own, such as `stocks.last_trade` beside `stocks.trades`. A `bars` dataset gives
    `HistoryQuery`, the class of its most general overload: `bar_query_class` picks the class
    of each call from its timespan and multiplier.

    Args:
        spec: The dataset.

    Returns:
        The query class its factory returns.
    """
    if spec.is_bars:
        return HistoryQuery
    if not spec.has_channels:
        return Query if spec.has_fetch else HistoryQuery
    return StreamHistoryQuery if spec.has_history else StreamQuery


def bar_query_class(spec: DatasetSpec, timespan: object, multiplier: object) -> type[QueryBase]:
    """Pick the query class of a `bars()` call from its actual timespan and multiplier.

    With `multiplier=1`, a timespan that has a WebSocket channel (`SECOND`, `MINUTE`) gives a
    `StreamHistoryQuery`. Every other combination is history only (`HistoryQuery`); the
    previous session's bar is the `previous_day_bar` dataset, not a verb of `bars()`. A
    `timespan` that is not a `dm.Timespan` raises `TypeError` (`_require_enum`), and a
    `multiplier` that is not a positive `int` raises `TypeError` or `ValueError`
    (`require_positive_int`).

    Args:
        spec: The bars dataset.
        timespan: The `timespan` argument as passed.
        multiplier: The `multiplier` argument as passed.

    Returns:
        The query class to instantiate.

    Raises:
        ValueError: If `Timespan.DAY` is used for futures (the futures endpoint has no day
            unit; use `Timespan.SESSION`), or `Timespan.SESSION` is used outside futures.
    """
    span = _require_enum(timespan, Timespan, "timespan")
    count = require_positive_int("multiplier", multiplier)
    futures = spec.asset_class is AssetClass.FUTURES
    if futures and span is Timespan.DAY:
        raise ValueError("futures bars have no DAY timespan; use dm.Timespan.SESSION")
    if not futures and span is Timespan.SESSION:
        raise ValueError("Timespan.SESSION exists only for futures bars")
    if count == 1 and span in spec.channels:
        return StreamHistoryQuery
    return HistoryQuery
