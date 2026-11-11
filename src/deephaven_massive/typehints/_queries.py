"""The base `Query` and its five query classes, their verbs, and the choice of a dataset's class.

A dataset method such as `m.stocks.trades(["AAPL"])` returns a *query*: an immutable,
hashable description of what to fetch. Nothing touches the network until a *verb* is called,
and each verb returns a Deephaven `Table` of a particular shape:

| Class                | Verbs                                                     |
|----------------------|-----------------------------------------------------------|
| `FetchQuery`         | `fetch()`, `poll(interval)`                               |
| `StreamQuery`        | `blink()`, `append_only()`, `ring(capacity)`              |
| `SymbolStreamQuery`  | the `StreamQuery` verbs and `last_by()`                   |
| `HistoryQuery`       | `history(start, end=None)`                                |
| `StreamHistoryQuery` | the `SymbolStreamQuery` verbs and `history()`             |

`StreamQuery` is the type of every live stream. A stream whose rows name no symbol, such as the
options admin messages, is a plain `StreamQuery`: it has no `last_by()`, whose default key is
`Symbol`. Every stream whose rows name their symbol is a `SymbolStreamQuery`.

A verb that cannot work for a dataset does not exist on its query class, so tab completion and
`hasattr` are the contract; calling one raises an `AttributeError` that names the verbs the
query has and, where one exists, the dataset that has the verb (`Query.__getattr__`): a
one-row endpoint, such as each symbol's last trade, is a `FetchQuery` dataset of its own
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
`poll`, REST `history`), `StreamTransport` (opening a WebSocket stream), and `FlatFileTransport`
(flat-file `history`). `history()` chooses its source itself (`choose_source` in
`_datasets/history_source.py`) and then calls one transport, so the transports only do I/O; under
`AUTO` a flat-file read whose last days are not published yet reads those days from REST too. Each
static verb (`fetch()`, `history()`) is one *call*: it takes a process-unique id from the session's
stream-id counter and logs `call 7 start: <query>.<verb>`, then `call 7 done: <rows> rows in <s> s`
or `call 7 failed after <s> s: <error>`, at DEBUG; the transports name their worker threads after it
(`massive #1 call-7 rest_0`), so every line of one call can be found. By default a static verb
does not wait: it checks its arguments on the calling thread, where an argument error raises,
and so does a crypto pair passed in code that Massive does not list, for a path that takes the
pair as its two currencies; it then returns at once a refreshing
table that a task on the session's read pool fills in one update when the download completes
(`open_read` in `_tables/background_read.py`); that task logs the call's outcome, and an error
Massive answers fails the table. With `wait=True` it reads on the calling thread and returns a
static table, or raises. A `history()` or `fetch()` whose
symbols are a refreshing table follows it instead: the result is always refreshing, each symbol
added to the table is read (by `history()` over the call's fixed window, by `fetch()` as Massive
answers then) and lands whole, and each symbol removed leaves it (`open_followed_read` in
`_tables/followed_read.py`, run by the session's follow coordinator). A table passed where a
dataset takes one value (`Request.value_table`: an option chain's underlying, a futures product
code) is read value by value, each as if passed alone (`RestTransport.fetch_by_value`): a static
one once, a refreshing one followed by `fetch()` the same way, keyed by the value, and re-read by
each `poll()` cycle. Live stream results are shaped
and tracked by the session (`SharedStreams.result`), so every stream verb shares one stream per
(dataset, channel, symbol set).

A dataset's class follows from the transports its spec declares (`query_class`), and a
`bars()` call's from its timespan and multiplier (`bar_query_class`), so no spec names a
class and the dataset layer never refers to the query classes.

Users import the classes for type annotations from the public module
`deephaven_massive.typehints`, and never construct them.

The module is one subject although it runs past 1,000 lines: the five classes build on one
base and on each other (`StreamHistoryQuery` is a `SymbolStreamQuery`, itself a `StreamQuery`,
and a `HistoryQuery`), so a
verb, and the choice of the class that offers it, read whole only beside all five.
"""

from __future__ import annotations

import functools
import logging
import time
from collections.abc import Sequence
from collections.abc import Set as AbstractSet
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Concatenate, Final, ParamSpec, TypeVar, cast

import numpy as np
from deephaven import arrow as dh_arrow
from deephaven import merge
from deephaven.stream import blink_to_append_only
from deephaven.table import Table
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
from deephaven_massive._datasets.symbols import ALL, SymbolTable, symbol_class
from deephaven_massive._enums import (
    AssetClass,
    HistorySource,
    OnError,
    PollMode,
    Reread,
    Timespan,
    _require_enum,
)
from deephaven_massive._errors import (
    ErrorCause,
    MassiveError,
    UnlistedPairError,
    failure_text,
    raises_through,
)
from deephaven_massive._massive import Transport
from deephaven_massive._override import override
from deephaven_massive._tables import follow_coordinator
from deephaven_massive._tables.background_read import open_read
from deephaven_massive._tables.columns import (
    column_table,
    describe,
    recency_note,
    rest_recency_note,
)
from deephaven_massive._tables.diagnostics import shown_query, utc_text
from deephaven_massive._tables.execution_context import entered, refuse_update_graph_thread
from deephaven_massive._tables.follow_coordinator import FirstRead
from deephaven_massive._tables.followed_read import FollowPlan, open_followed_read
from deephaven_massive._tables.session_core import SessionCore
from deephaven_massive._tables.shared_streams import SharedStreams
from deephaven_massive._tables.time_text import deephaven_time_text
from deephaven_massive._tables.transports import (
    FlatFileTransport,
    RestTransport,
    StreamTransport,
    SymbolFailure,
)
from deephaven_massive._tables.watchlist import (
    SymbolRowRule,
    latest_rows_since_added,
    read_symbols,
    skipped_rows_text,
    watched_rows,
)

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable, Mapping

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.dates import DateLike
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.request import Request, ValueTable
    from deephaven_massive._tables.transports import SymbolOutcome

__all__ = [
    "FetchQuery",
    "HistoryQuery",
    "Query",
    "QueryCall",
    "StreamHistoryQuery",
    "StreamQuery",
    "SymbolStreamQuery",
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

_FAILED_SHOWN: Final = 5
"""The most failed call-time symbols the WARNING of a followed `history(wait=True)` names."""

SYMBOLS_SHOWN: Final = 5
"""The most symbols a query's repr lists; `… (N more)` stands for the rest."""

_VERBS: Final = ("fetch", "poll", "blink", "append_only", "ring", "last_by", "history")
"""Every verb's name, in the order a missing verb's message lists the verbs a query has."""

_STREAM_VERBS: Final = frozenset({"blink", "append_only", "ring", "last_by"})
"""The verbs a dataset with WebSocket channels serves; `last_by` only where its rows name their
symbol (`DatasetSpec.names_symbols`)."""

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


MINIMUM_POLL_INTERVAL: Final = timedelta(seconds=1)
"""The shortest interval `poll()` accepts.

A shorter one is almost always a typo, such as `poll(0.01)` for `poll(10)`, and on a plan that
Massive does not pace it would send requests back to back; a stream serves anything faster.
"""


@raises_through(require_positive_number)
def _interval(value: object) -> timedelta:
    """Validate a `poll()` interval.

    Args:
        value: Seconds as an `int` or `float`, or a `timedelta`.

    Returns:
        The interval as a `timedelta`.

    Raises:
        TypeError: If the value is neither a number nor a `timedelta`.
        ValueError: If the number is not finite and positive, if the interval overflows
            `timedelta`, or if it is shorter than `MINIMUM_POLL_INTERVAL`.
    """
    if isinstance(value, timedelta):
        period = value
    else:
        seconds = require_positive_number("interval", value)
        try:
            period = timedelta(seconds=seconds)
        except OverflowError:  # beyond timedelta's range
            raise ValueError(f"interval must be a positive, finite time, got {value!r}") from None
    if period < MINIMUM_POLL_INTERVAL:
        raise ValueError(
            f"interval must be at least {MINIMUM_POLL_INTERVAL.total_seconds():g} second, got "
            f"{value!r}: each poll() cycle sends a request; use a stream for faster updates"
        )
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
    gives `fetch()` and `poll()`, channels the stream verbs (`last_by()` only where the rows name
    their symbol), and a history source `history()`. A `bars` dataset streams at some timespans
    only, which is enough to name it as the dataset that streams.

    Args:
        spec: The dataset.
        verb: A verb's name, one of `_VERBS`.

    Returns:
        True when the dataset serves the verb.
    """
    if verb == "last_by":
        return spec.has_channels and spec.names_symbols
    if verb in _STREAM_VERBS:
        return spec.has_channels
    if verb == "history":
        return spec.has_history
    return spec.has_fetch


def verbs(spec: DatasetSpec) -> tuple[str, ...]:
    """Return the verbs a dataset's queries can have, in the order `_VERBS` lists them.

    A `bars` dataset streams at some timespans only, and its stream verbs are listed all the
    same; the verbs of one `bars()` call follow its timespan (`bar_query_class` in
    `namespaces/_query_building.py`). `dm.datasets()` shows the result.

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


def _read_value_table(
    core: SessionCore, spec: DatasetSpec, values: ValueTable, *, query: str, live: bool = False
) -> tuple[str, ...]:
    """Read a table passed where the dataset takes one value, once, as `_read_symbol_table` does.

    A row that is not a value the dataset takes (an underlying that is not a stock or index
    ticker) follows `MassiveConfig.noncanonical_symbols` (`SymbolRowRule.for_values`).

    Args:
        core: The session core, for its execution context and its `noncanonical_symbols`.
        spec: The dataset.
        values: The boxed table and the argument it stands for.
        query: The `repr` of the query whose verb reads it, which the messages name.
        live: True for a verb that follows the table or re-reads it, which may start on a
            table with no row yet.

    Returns:
        The table's values, sorted and de-duplicated; null and skipped rows are left out.

    Raises:
        ValueError: If the table has rows and none of them is a value the dataset takes, or,
            for a static read, if it holds no value at all; under `RAISE`, the `ValueError` of
            `read_symbols` for a bad row.
    """
    rule = SymbolRowRule.for_values(spec, values, core.config.noncanonical_symbols)
    with entered(core.exec_ctx):
        rows = read_symbols(rule, values.table, stream_id=None)
    if rows.skipped and not rows.symbols:
        raise ValueError(
            f"{query}: no row of the {values.argument} table is a value this dataset takes, so "
            f"the result would read nothing: {skipped_rows_text(rows.skipped)}"
        )
    if not rows.symbols and not live:
        raise ValueError(f"{query}: the {values.argument} table holds no values")
    return rows.symbols


def _merged(core: SessionCore, outcomes: Mapping[str, SymbolOutcome]) -> Table:
    """Join the values' tables of a static read of a value table, or raise its first failure.

    A static table of values is a fixed list in code: like a list call, it fails when any of
    its values does.

    Args:
        core: The session core, for its execution context.
        outcomes: Each value's outcome, in the order read.

    Returns:
        One static table of every value's rows, in value order.

    Raises:
        MassiveError: The first failed value's error.
    """
    tables = []
    for outcome in outcomes.values():
        if isinstance(outcome, SymbolFailure):
            raise outcome.error
        tables.append(outcome.table)
    with entered(core.exec_ctx):
        return merge(tables)


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
        else as its `repr`; a Deephaven table, such as a table of underlyings, as `<table>`.
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
    return "<table>" if isinstance(value, Table) else repr(value)


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


def _window_text(first: date | datetime, last: date | datetime | None, now: datetime) -> str:
    """Write a followed `history()`'s fixed window for its table description.

    Args:
        first: The parsed `start`.
        last: The parsed `end`, or None for the call's instant.
        now: The call's instant (`HistoryWindow.now`).

    Returns:
        Such as `"2026-10-01 through 2026-11-02T14:05:12Z"`: a date as itself, an instant
        as UTC text to the second, and an open end as the call's instant.
    """

    def shown(value: date | datetime) -> str:
        return utc_text(value.timestamp()) if isinstance(value, datetime) else value.isoformat()

    end = utc_text(now.timestamp()) if last is None else shown(last)
    return f"{shown(first)} through {end}"


@dataclass(frozen=True, slots=True)
class QueryCall:
    """The dataset-method call that built a query, as its repr shows it.

    Created by the dataset method itself, through `shows_call` in
    `namespaces/_query_building.py`, when it returns its query; a query built any other way
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
class Query:
    """Any query: the base class of the five query classes, for annotations.

    Import it for annotations::

        from deephaven_massive.typehints import Query

    It types code that takes any query, such as a helper that lists a query's columns. A dataset
    method returns an instance of one of its subclasses; users never construct a query.

    Every query has `columns()`. Frozen and hashable; equality includes the concrete class, so a
    `StreamQuery` never equals a `HistoryQuery`, and covers the dataset and the normalized request,
    never the call that built it. Queries have no public attributes: the verbs and `columns()` are
    the whole API, and the `repr` is the call that built the query (`QueryCall`).
    """

    _core: SessionCore
    _spec: DatasetSpec
    _request: Request
    _call: QueryCall = field(compare=False)

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

    if TYPE_CHECKING:
        # Declared at run time only: a type checker that saw `__getattr__` would accept any
        # attribute of a query, such as a misspelled verb, instead of refusing it.
        pass
    else:

        def __getattr__(self, name: str) -> object:
            """Explain a verb this query does not have, and name the dataset that has it.

            Python calls this only when the normal lookup fails, so the query's own verbs,
            `columns()`, and every other attribute resolve as usual, and `hasattr(q, "fetch")` is
            False on a query without `fetch()`.

            Args:
                name: The attribute that was not found.

            Returns:
                Never: it always raises.

            Raises:
                AttributeError: Always. For a verb's name, the message names the dataset, the
                    methods its query has, and where one exists the dataset that has the verb,
                    such as `"stocks.trades has no fetch(); its methods are blink(),
                    append_only(), ring(), last_by(), history(); use
                    m.stocks.last_trade(...).fetch()"`
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
        text = f"{self._spec.name} has no {verb}(); its methods are {has}"
        alternative = self._alternative(verb)
        return text if alternative is None else f"{text}; {alternative}"

    def _alternative(self, verb: str) -> str | None:
        """Name what serves a verb this query lacks.

        In order: for `fetch()` or `poll()` of intraday bars, `history(start, end)`, then the
        previous day's bar; the other datasets of the same kind and asset class that serve the
        verb (a stream's one-row dataset, such as `stocks.last_trade` for `fetch()` of
        `stocks.trades`, and back); for `fetch()` or `poll()` of a dataset whose rows name their
        symbol, the class's `snapshots` dataset,
        the current state on a paid plan, with its `Fmv` column named for a fair market value
        dataset, since Massive's REST serves fair market value only there; for `history()` of
        a dataset with a time axis, its `start=` and `end=` arguments; for a stream verb of bars
        that do not stream, the timespans that do.

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
        if verb in ("fetch", "poll") and snapshots is not None and spec.names_symbols:
            # Massive's REST serves fair market value only as a column of the snapshot.
            fair_value = (
                spec.kind == "fmv"
                and "Fmv" in self._core.catalog.dataset_schema(snapshots).column_names
            )
            what = "fair market value" if fair_value else "state"
            column = ", its Fmv column" if fair_value else ""
            return f"for each symbol's current {what} use m.{snapshots.name}(...).{verb}(){column}"
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

    @property
    def _in_code(self) -> bool:
        """True when the query's symbols were passed in code, not as a table.

        A crypto pair passed in code that Massive does not list raises at the call; a table's
        follows `MassiveConfig.unlisted_symbols`.
        """
        return not isinstance(self._request.symbols, SymbolTable)

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

    def _first_read(self, plan: FollowPlan, call_time: tuple[str, ...], *, verb: str) -> FirstRead:
        """Read the call-time symbols of a followed `history()` or `fetch()` now, as one read.

        The read of `wait=True`, on the caller thread. A crypto pair Massive's crypto ticker
        list does not hold follows `MassiveConfig.unlisted_symbols`: under `RAISE` the call
        raises its error, and under `IGNORE` the WARNING leaves it out.

        Args:
            plan: How the symbols are read.
            call_time: The call-time symbols; at least one.
            verb: The verb, `"history"` or `"fetch"`, for the WARNING.

        Returns:
            Each symbol's outcome, with when the read began and how long it took.

        Raises:
            MassiveError: The first symbol's error, when every symbol failed; or, under
                `unlisted_symbols=RAISE`, the first unlisted pair's. A failure of every symbol
                alike (a rejected key, a closed session) propagates from the read.
        """
        began_at = int(follow_coordinator.CLOCK.time() * 1e9)
        started = time.monotonic()
        outcomes = plan.read(call_time, self._core.read_stop())
        failed = {
            name: outcome
            for name, outcome in outcomes.items()
            if isinstance(outcome, SymbolFailure)
        }
        unlisted = self._core.config.unlisted_symbols
        pairs = [
            name for name, outcome in failed.items() if isinstance(outcome.error, UnlistedPairError)
        ]
        if pairs and unlisted is OnError.RAISE:
            raise failed[pairs[0]].error
        if len(failed) == len(call_time):
            raise failed[call_time[0]].error
        warned = {
            name: outcome
            for name, outcome in failed.items()
            if not (unlisted is OnError.IGNORE and name in pairs)
        }
        if warned:
            logger.warning(
                "%s %s: %d of the %d symbols of the call failed and are left out of the "
                "result for now; m.symbol_reads() shows each one (retrying, or failed): %s",
                self._spec.name,
                verb,
                len(warned),
                len(call_time),
                "; ".join(
                    f"{name}: {failure_text(outcome.error)}"
                    for name, outcome in list(warned.items())[:_FAILED_SHOWN]
                ),
            )
        return FirstRead(outcomes, began_at, time.monotonic() - started)


def _symbols_per_read(spec: DatasetSpec) -> int:
    """Return how many symbols one read of a followed `fetch()` takes.

    A dataset whose list parameter (`ticker.any_of`) names many symbols in one request reads
    up to `DatasetSpec.symbols_per_request` symbols at once, as a static `fetch()` groups
    them, and a `limit` is sent once on that request, as Massive's cap on its rows. Every
    other dataset reads one symbol a read.

    Args:
        spec: The dataset.

    Returns:
        The most symbols one read takes, at least 1.
    """
    return spec.symbols_per_request or 1


class FetchQuery(Query):
    """A query with `fetch()`, which reads its rows once, and `poll()`, which reads them again.

    Import it for annotations::

        from deephaven_massive.typehints import FetchQuery

    You get the query from a dataset method, such as `m.stocks.dividends(...)`. It reads nothing
    until you call one of its methods. You can call them as often as you like.

    Example:
        ::

            dividends = m.stocks.dividends(["AAPL", "MSFT"], start="2024-01-01").fetch()
            latest = m.stocks.snapshots(["AAPL", "MSFT"]).poll(5)
    """

    __slots__ = ()

    @_verb
    @raises_through(
        SessionCore.run_verb,
        refuse_update_graph_thread,
        _read_symbol_table,
        _read_value_table,
        _merged,
        RestTransport.fetch,
        RestTransport.check_fetch,
        RestTransport.fetch_by_symbol,
        RestTransport.fetch_by_value,
        open_followed_read,
        _wait,
    )
    def fetch(self, *, wait: bool = False) -> Table:
        """Return a table of every row Massive has now, up to the query's `limit`.

        By default the call returns a live table with no rows at once, and the rows arrive in the
        background. Deephaven updates the table, and every table built on it, as they land. For
        symbols passed in code or in a static table, they land all in one update, and
        `m.streams()` shows the read `LOADING`, then `DONE` or `FAILED`. Pass `wait=True` to wait
        for the rows; without a watchlist, you then get a static table.

        A static table of symbols is read once, when you call. A watchlist is followed: a symbol you
        add is read, and one you delete leaves. One symbol's failure leaves the watchlist's other
        symbols in place, and `m.symbol_reads()` shows each symbol's read.

        Args:
            wait: False, the default, returns at once and fills the table in the background. True
                waits for the rows; for a watchlist, until each symbol has been read once.

        Returns:
            A table with the dataset's columns: live, or static for `wait=True` without a
            watchlist.

        Raises:
            TypeError: If `wait` isn't a `bool`.
            ValueError: At the call, before any request, if:
                - it runs on a Deephaven update-graph thread;
                - the endpoint doesn't take one of the query's parameters or filters;
                - a static `symbols`, `underlying=`, or `product_code=` table is empty;
                - no row of a table of symbols is Massive's exact ticker.
            AuthenticationError: With `wait=True`, if Massive rejects your API key. For
                `m.crypto.last_trade` and `m.crypto.daily_open_close`, also at the call, if Massive
                rejects it while the call reads Massive's list of crypto tickers.
            EntitlementError: With `wait=True`, if your plan doesn't include the data, or Massive
                delays current data whose subscription isn't in
                `MassiveConfig.delayed_subscriptions`. The message names the setting to change.
                For those two crypto datasets, also at the call, if your plan doesn't include
                Massive's list of crypto tickers.
            MassiveError: At the call, if:
                - the session is closed;
                - for those two crypto datasets, Massive's list of crypto tickers can't be read,
                  or lacks a crypto pair passed as a `str` or list.
                With `wait=True`, also if:
                - a rate limit outlasts the retries;
                - a request fails in any other way;
                - for a watchlist, every symbol's read failed.

        Without `wait=True`, an error from Massive fails the table instead of raising: it is
        logged and shows in `Error` of `m.streams()`. A watchlist's table fails when every symbol
        it held at the call fails for good, such as on a plan without the dataset.

        Example:
            ::

                dividends = m.stocks.dividends(["AAPL", "MSFT"]).fetch()
                now = m.stocks.dividends(["AAPL", "MSFT"]).fetch(wait=True)
        """
        background = not _wait(wait)
        return self._logged_call(
            "fetch()" if background else "fetch(wait=True)",
            functools.partial(self._fetch, wait=wait),
            background=background,
        )

    def _fetch(self, call: int, *, wait: bool) -> Table:
        """Run `fetch()` as call `call`.

        A refreshing symbol table is followed (`_followed_fetch`). A static one is read once,
        and its crypto pairs that Massive's crypto ticker list does not hold follow
        `MassiveConfig.unlisted_symbols`; pairs passed in code always raise.

        Args:
            call: The call id.
            wait: As `fetch` takes it.

        Returns:
            The static table, or the refreshing one a background read fills, or the one that
            follows a refreshing symbol table.
        """
        spec = self._spec
        symbols = self._request.symbols
        if self._request.value_table is not None:
            return self._value_fetch(call, self._request.value_table, wait=wait)
        if isinstance(symbols, SymbolTable) and symbols.table.is_refreshing:
            return self._followed_fetch(call, symbols, wait=wait)
        unlisted = (
            self._core.config.unlisted_symbols
            if isinstance(symbols, SymbolTable)
            else OnError.RAISE
        )
        request = self._static_request()
        rest = self._core.rest_transport()
        if not wait:  # a parameter error and a pair passed in code raise now, as they would
            rest.check_fetch(spec, request, split_pairs=self._in_code)
        return self._result(
            call,
            request,
            wait=wait,
            transport=Transport.REST,
            note=rest_recency_note(spec, self._core.config.delayed_subscriptions),
            run=lambda stop: rest.fetch(spec, request, call=call, stop=stop, unlisted=unlisted),
        )

    def _followed_fetch(self, call: int, symbols: SymbolTable, *, wait: bool) -> Table:
        """Run a `fetch()` call that follows a refreshing symbol table.

        The table's current rows are checked now, as a live verb checks them, and the request's
        parameters with them. Every symbol, at the call or added later, is read with the
        requests `fetch()` makes for it (`RestTransport.fetch_by_symbol`), as Massive answers
        at that read, and is read again whenever it returns to the table. With `wait=True`
        the call-time symbols are read now, as one read; the call raises only when every one
        of them failed, and logs one WARNING when some did.

        Args:
            call: The call id.
            symbols: The boxed refreshing table.
            wait: As `fetch` takes it.

        Returns:
            The refreshing result (`open_followed_read`).
        """
        spec, core = self._spec, self._core
        call_time = _read_symbol_table(core, spec, symbols, query=repr(self), live=True)
        request = replace(self._request, symbols=call_time)
        rest = core.rest_transport()
        if call_time:  # a parameter error raises now; with none, the first read checks them
            rest.check_fetch(spec, request, split_pairs=False)
        plan = FollowPlan(
            Transport.REST,
            one_scan=False,
            symbols_per_read=_symbols_per_read(spec),
            read=lambda names, stop: rest.fetch_by_symbol(
                spec, request, symbols=names, call=call, stop=stop
            ),
        )
        recency = rest_recency_note(spec, core.config.delayed_subscriptions)
        note = "Follows its symbol table: each symbol's rows are Massive's answer as of its read."
        logger.info("%s fetch: follows its symbol table", spec.name)
        loaded = self._first_read(plan, call_time, verb="fetch") if wait and call_time else None
        return open_followed_read(
            core,
            spec=spec,
            call=call,
            query=repr(self),
            symbol_table=symbols.table,
            rule=SymbolRowRule.of(spec, core.config.noncanonical_symbols),
            reread=Reread.ALWAYS,
            plan=plan,
            note=note if recency is None else f"{note} {recency}",
            first=loaded,
        )

    def _value_fetch(self, call: int, values: ValueTable, *, wait: bool) -> Table:
        """Run a `fetch()` call whose one-value argument is a table (`Request.value_table`).

        Each value is read with the requests `fetch()` makes when it is passed alone
        (`RestTransport.fetch_by_value`). A static table is read once, as a fixed list: the
        result joins every value's rows, and a failed value fails the call. A refreshing one is
        followed (`_followed_value_fetch`). A symbol table passed beside it is read once.

        Args:
            call: The call id.
            values: The boxed table of values.
            wait: As `fetch` takes it.

        Returns:
            The static table, or the refreshing one a background read fills, or the one that
            follows a refreshing table of values.
        """
        if values.table.is_refreshing:
            return self._followed_value_fetch(call, values, wait=wait)
        spec, core = self._spec, self._core
        names = _read_value_table(core, spec, values, query=repr(self))
        request = self._static_request()
        rest = core.rest_transport()
        if not wait:  # a parameter error and a pair passed in code raise now, as they would
            rest.check_fetch(spec, request.for_value(names[0]), split_pairs=self._in_code)
        return self._result(
            call,
            request,
            wait=wait,
            transport=Transport.REST,
            note=rest_recency_note(spec, core.config.delayed_subscriptions),
            run=lambda stop: _merged(
                core, rest.fetch_by_value(spec, request, values=names, call=call, stop=stop)
            ),
        )

    def _followed_value_fetch(self, call: int, values: ValueTable, *, wait: bool) -> Table:
        """Run a `fetch()` call that follows a refreshing table of values.

        As `_followed_fetch` follows a symbol table, keyed by each value: a value added to the
        table is read, as Massive answers then, and lands whole; a value removed leaves the
        result in the same update; a returning value is read again. Each value is one read.

        Args:
            call: The call id.
            values: The boxed refreshing table.
            wait: As `fetch` takes it.

        Returns:
            The refreshing result (`open_followed_read`).

        Raises:
            ValueError: If `symbols` is a refreshing table too: one result follows one table.
        """
        spec, core = self._spec, self._core
        symbols = self._request.symbols
        if isinstance(symbols, SymbolTable) and symbols.table.is_refreshing:
            logger.debug("%r fetch() refused: two refreshing tables to follow", self)
            raise ValueError(
                f"{self!r}.fetch() follows one refreshing table, and both symbols and "
                f"{values.argument} are refreshing tables. Use poll(interval), which reads "
                f"both at every cycle, or pass one of them as a static snapshot, such as "
                f"{values.argument}=table.snapshot()"
            )
        call_time = _read_value_table(core, spec, values, query=repr(self), live=True)
        request = self._static_request()
        rest = core.rest_transport()
        if call_time:  # a parameter error raises now; with none, each read checks them
            rest.check_fetch(spec, request.for_value(call_time[0]), split_pairs=self._in_code)
        plan = FollowPlan(
            Transport.REST,
            one_scan=False,
            symbols_per_read=1,
            read=lambda names, stop: rest.fetch_by_value(
                spec, request, values=names, call=call, stop=stop
            ),
        )
        recency = rest_recency_note(spec, core.config.delayed_subscriptions)
        note = (
            f"Follows its table of {values.argument} values: each value's rows are Massive's "
            "answer as of its read."
        )
        logger.info("%s fetch: follows its table of %s values", spec.name, values.argument)
        loaded = self._first_read(plan, call_time, verb="fetch") if wait and call_time else None
        return open_followed_read(
            core,
            spec=spec,
            call=call,
            query=repr(self),
            symbol_table=values.table,
            rule=SymbolRowRule.for_values(spec, values, core.config.noncanonical_symbols),
            reread=Reread.ALWAYS,
            plan=plan,
            note=note if recency is None else f"{note} {recency}",
            first=loaded,
        )

    @_verb
    @raises_through(
        SessionCore.run_verb,
        refuse_update_graph_thread,
        _interval,
        _require_enum,
        read_symbols,
        _read_value_table,
        RestTransport.poll,
    )
    def poll(self, interval: float | timedelta, *, mode: PollMode = PollMode.AUTO) -> Table:
        """Return a live table that fetches the query again every `interval`.

        Each poll reads the whole query again, so the table follows a watchlist. The table keeps
        polling while anything uses it
        (https://github.com/chipkent/deephaven-massive/blob/main/README.md#stop-live-tables).

        A news or filings feed needs a `start` or a `limit`. Without either, each poll would read
        Massive's whole archive.

        A failed poll logs a WARNING and shows in `Error` of `m.streams()`, and the next poll runs
        as usual. The table fails and stops polling if its polls keep failing for
        `MassiveConfig.max_outage`, 300 seconds by default. It also fails on a failure no later poll
        can fix, such as a rejected key.

        A poll skips a symbol it can't read and lists it in `m.streams()`. To fail the table
        instead, set `noncanonical_symbols`, `unlisted_symbols`, or `unavailable_symbols` in
        `MassiveConfig` to `dm.OnError.RAISE`.

        Args:
            interval: The time between polls, in seconds or as a `timedelta`, at least 1 second. For
                anything faster, use a stream.
            mode: How polls combine. `dm.PollMode.AUTO`, the default, uses the mode the dataset's
                page names. `REPLACE` keeps only the latest poll's rows. `UPSERT` keeps every key
                ever seen, at its latest row. `APPEND` adds only rows it hasn't seen.

        Returns:
            A live table with the dataset's columns: keyed for `REPLACE` and `UPSERT`, append-only
            for `APPEND`. Each row's `TimestampInsert` is the time of the poll that wrote it. The
            table remembers each key or row it has held, so its memory grows with them.

        Raises:
            TypeError: If `interval` isn't a number or a `timedelta`, or `mode` isn't a
                `dm.PollMode`.
            ValueError: At the call, before the table exists, if:
                - it runs on a Deephaven update-graph thread;
                - `interval` is shorter than 1 second or isn't finite;
                - a news or filings feed has neither `start` nor `limit`;
                - no row of a table of symbols is Massive's exact ticker;
                - an endpoint doesn't take one of the query's parameters or filters.
            AuthenticationError: For a crypto dataset, if Massive rejects your API key while the
                call reads Massive's list of crypto tickers.
            EntitlementError: For a crypto dataset, if your plan doesn't include Massive's list of
                crypto tickers.
            MassiveError: If the session is closed, or Massive's list of crypto tickers can't be
                read or lacks a crypto pair passed as a `str` or list.
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
        if self._request.value_table is not None:
            _read_value_table(
                self._core, self._spec, self._request.value_table, query=repr(self), live=True
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


class StreamQuery(Query):
    """A streaming query: `blink()`, `append_only()`, and `ring()`.

    Import it for annotations::

        from deephaven_massive.typehints import StreamQuery

    Every live stream's query is a `StreamQuery`. When the stream's rows have a `Symbol` column, as
    `m.stocks.luld(...)`'s do, its query is a `SymbolStreamQuery`, which adds `last_by()`.
    `m.options.admin_messages()` has no `Symbol` column, so its query has only these three methods.
    You can reuse a query. Its methods return the same stream as different Deephaven table types.
    Identical queries share one stream, and each call returns its own table. The stream stays
    subscribed while any of its tables, or a table made from one, is in use.

    A method returns once its table exists, without waiting for Massive to confirm the subscription.
    Later failures fail the table and show in `m.streams()`. A refused subscription and a rejected
    key are two such failures. The next call on an identical query then opens a new stream.

    Example:
        ::

            recent = m.stocks.imbalances(["AAPL", "MSFT"]).ring(1000)
            notices = m.options.admin_messages().append_only()
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

        The request of a stream whose rows name no symbol (`DatasetSpec.names_symbols` False),
        which has none, gets `ALL`: the stream subscribes its channel's every topic. A static
        table cannot be listened to, so it is read now, like `fetch()` reads one
        (`_read_symbol_table`), and becomes the sorted tuple of its symbols; a refreshing table
        stays reactive. A static table with no symbol, with only rows that are not canonical,
        or with any such row under `noncanonical_symbols=RAISE`, raises the `ValueError` of
        `_read_symbol_table`; under `LOG` and `IGNORE` such a row is skipped. A crypto pair of
        the table that Massive's crypto ticker list does not hold follows
        `unlisted_symbols` (`StreamTransport.tradable_pairs`).

        Returns:
            The request.
        """
        request = self._request
        symbols = request.symbols
        if not self._spec.names_symbols:
            # A stream whose rows name no symbol subscribes its channel's every topic, so its
            # stream is keyed as dm.ALL's would be.
            return replace(request, symbols=ALL)
        if isinstance(symbols, SymbolTable) and not symbols.table.is_refreshing:
            read = _read_symbol_table(self._core, self._spec, symbols, query=repr(self))
            if self._spec.asset_class is AssetClass.CRYPTO:
                read = self._core.stream_transport().tradable_pairs(self._spec, read)
            return replace(request, symbols=read)
        return request

    def _stream(self, shape: Callable[[Table], Table], request: Request, *, verb: str) -> Table:
        """Shape a live result from this query's stream, creating the stream if needed.

        The result is the shape, described last (`describe`): an append-only or ring table
        copies no attributes, so describing the blink table first would lose them. The schema
        is read before the stream is acquired, so the result is built under no library lock.
        A reactive symbol table is read first, so a row that is not Massive's exact ticker
        fails the call before any stream starts. A crypto pair takes its split from Massive's
        crypto ticker list when the stream starts (read once per session); a failed read, or a
        pair the list does not hold, raises `MassiveError` before any topic is subscribed. A pair
        of a symbol table, at the call or added later, follows `MassiveConfig.unlisted_symbols`
        instead: skipped (and, for a reactive table, listed in `m.streams()`) under `LOG` and
        `IGNORE`.

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
        return self._core.streams.result(
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
        SharedStreams.result,
        StreamTransport.open_stream,
        StreamTransport.tradable_pairs,
    )
    def blink(self) -> Table:
        """Return the stream as a blink table, which holds only the latest update cycle's rows.

        A blink table keeps no history, so it suits a Deephaven aggregation that should count every
        row without storing them.

        Returns:
            A live blink table with the dataset's columns.

        Raises:
            ValueError: At the call, before the table exists, if:
                - it runs on a Deephaven update-graph thread;
                - a static table of symbols is empty, or no row of a table of symbols is Massive's
                  exact ticker;
                - option contracts passed as a list or a static table pass the connection's limit;
                - the dataset streams only in real time (fair market value, the options admin
                  messages) and `MassiveConfig.delayed_subscriptions` lists its subscription.
            AuthenticationError: If Massive rejects your API key while the call checks which feed
                your plan streams. A later rejection fails the table instead.
            EntitlementError: If Massive's data for this dataset is delayed and its subscription
                isn't in `MassiveConfig.delayed_subscriptions`. The message names the setting to
                change. For the options admin messages, which stream only in real time, a delayed
                label means your plan lacks the dataset, and the message links the page that
                lists the plans that include it.
            MassiveError: If:
                - the session is closed;
                - the stream fails while it opens;
                - Massive's list of crypto tickers can't be read, or lacks a crypto pair passed as a
                  `str` or list;
                - that list lacks every pair of a table of crypto pairs;
                - that list lacks any pair of such a table, under
                  `unlisted_symbols=dm.OnError.RAISE`.
        """
        return self._stream(lambda blink: blink, self._stream_request(), verb="blink")

    @_verb
    @raises_through(
        SessionCore.run_verb,
        refuse_update_graph_thread,
        _read_symbol_table,
        read_symbols,
        SharedStreams.result,
        StreamTransport.open_stream,
        StreamTransport.tradable_pairs,
    )
    def append_only(self) -> Table:
        """Return every row the stream sends after the call, oldest first.

        The table grows for as long as the stream runs; `ring()` keeps only the latest rows.

        Returns:
            A live append-only table with the dataset's columns.

        Raises:
            ValueError: At the call, before the table exists, if:
                - it runs on a Deephaven update-graph thread;
                - a static table of symbols is empty, or no row of a table of symbols is Massive's
                  exact ticker;
                - option contracts passed as a list or a static table pass the connection's limit;
                - the dataset streams only in real time (fair market value, the options admin
                  messages) and `MassiveConfig.delayed_subscriptions` lists its subscription.
            AuthenticationError: If Massive rejects your API key while the call checks which feed
                your plan streams. A later rejection fails the table instead.
            EntitlementError: If Massive's data for this dataset is delayed and its subscription
                isn't in `MassiveConfig.delayed_subscriptions`. The message names the setting to
                change. For the options admin messages, which stream only in real time, a delayed
                label means your plan lacks the dataset, and the message links the page that
                lists the plans that include it.
            MassiveError: If:
                - the session is closed;
                - the stream fails while it opens;
                - Massive's list of crypto tickers can't be read, or lacks a crypto pair passed as a
                  `str` or list;
                - that list lacks every pair of a table of crypto pairs;
                - that list lacks any pair of such a table, under
                  `unlisted_symbols=dm.OnError.RAISE`.
        """
        return self._stream(blink_to_append_only, self._stream_request(), verb="append_only")

    @_verb
    @raises_through(
        SessionCore.run_verb,
        refuse_update_graph_thread,
        _read_symbol_table,
        read_symbols,
        SharedStreams.result,
        StreamTransport.open_stream,
        StreamTransport.tradable_pairs,
        require_positive_int,
    )
    def ring(self, capacity: int) -> Table:
        """Return the latest `capacity` rows the stream sends.

        Args:
            capacity: How many rows to keep, from 1 to 2**31 - 1.

        Returns:
            A live ring table with the dataset's columns.

        Raises:
            TypeError: If `capacity` isn't an `int`.
            ValueError: At the call, before the table exists, if:
                - `capacity` is out of range;
                - it runs on a Deephaven update-graph thread;
                - a static table of symbols is empty, or no row of a table of symbols is Massive's
                  exact ticker;
                - option contracts passed as a list or a static table pass the connection's limit;
                - the dataset streams only in real time (fair market value, the options admin
                  messages) and `MassiveConfig.delayed_subscriptions` lists its subscription.
            AuthenticationError: If Massive rejects your API key while the call checks which feed
                your plan streams. A later rejection fails the table instead.
            EntitlementError: If Massive's data for this dataset is delayed and its subscription
                isn't in `MassiveConfig.delayed_subscriptions`. The message names the setting to
                change. For the options admin messages, which stream only in real time, a delayed
                label means your plan lacks the dataset, and the message links the page that
                lists the plans that include it.
            MassiveError: If:
                - the session is closed;
                - the stream fails while it opens;
                - Massive's list of crypto tickers can't be read, or lacks a crypto pair passed as a
                  `str` or list;
                - that list lacks every pair of a table of crypto pairs;
                - that list lacks any pair of such a table, under
                  `unlisted_symbols=dm.OnError.RAISE`.
        """
        rows = require_positive_int("capacity", capacity, maximum=MAX_RING_CAPACITY)
        return self._stream(
            lambda blink: ring_table(blink, rows), self._stream_request(), verb="ring"
        )


class SymbolStreamQuery(StreamQuery):
    """A streaming query with a `Symbol` column: the `StreamQuery` methods and `last_by()`.

    Import it for annotations::

        from deephaven_massive.typehints import SymbolStreamQuery

    You get the query from a dataset method, such as `m.stocks.luld(...)`, and you can reuse it. Its
    four methods return the same stream as four Deephaven table types; `help(StreamQuery)` covers
    what every stream method shares.

    A static table of symbols is read once, when you call. A watchlist is followed: the stream
    subscribes and unsubscribes as you add and delete rows. A row that isn't Massive's exact ticker
    is skipped and listed in `m.streams()`.

    Massive allows 1,000 option contracts on one connection, and the library counts a contract's
    trades and quotes as two. Passing more contracts than that as a list or a static table raises
    `ValueError`. A watchlist's extra contracts wait for room, and `Deferred` in `m.streams()`
    counts them.

    Example:
        ::

            bands = m.stocks.luld(dm.ALL).last_by()  # the latest band of every ticker
            recent = m.stocks.imbalances(["AAPL", "MSFT"]).ring(1000)
    """

    __slots__ = ()

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
        elif isinstance(by, Sequence):
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
        SharedStreams.result,
        StreamTransport.open_stream,
        StreamTransport.tradable_pairs,
        _last_by_keys,
    )
    def last_by(self, by: str | Sequence[str] = "Symbol") -> Table:
        """Return the latest row of each key, by default one row per symbol.

        The table starts empty and gains each key's row when its first update arrives. For each
        symbol's current state at once, poll a snapshot instead, such as
        `m.stocks.snapshots(["AAPL", "MSFT"]).poll(5)`.

        With a watchlist, each symbol's row shows only while the symbol is on it.

        Args:
            by: The key column or columns, as in Deephaven's `Table.last_by`; `"Symbol"` by default.
                Name each column once.

        Returns:
            A live table with the dataset's columns and one row per key.

        Raises:
            TypeError: If `by` isn't a `str` or a sequence of `str`.
            ValueError: At the call, before the table exists, if:
                - `by` is empty, repeats a column, or names a column the dataset doesn't have;
                - it runs on a Deephaven update-graph thread;
                - a static table of symbols is empty, or no row of a table of symbols is Massive's
                  exact ticker;
                - option contracts passed as a list or a static table pass the connection's limit;
                - the dataset is fair market value, which streams only in real time, and
                  `MassiveConfig.delayed_subscriptions` lists its subscription.
            AuthenticationError: If Massive rejects your API key while the call checks which feed
                your plan streams. A later rejection fails the table instead.
            EntitlementError: If Massive's data for this dataset is delayed and its subscription
                isn't in `MassiveConfig.delayed_subscriptions`. The message names the setting to
                change.
            MassiveError: If:
                - the session is closed;
                - the stream fails while it opens;
                - Massive's list of crypto tickers can't be read, or lacks a crypto pair passed as a
                  `str` or list;
                - that list lacks every pair of a table of crypto pairs;
                - that list lacks any pair of such a table, under
                  `unlisted_symbols=dm.OnError.RAISE`.
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
            logger.debug(
                "%s.last_by(%s): latest rows of the watched symbols, each shown from the update "
                "cycle in which its symbol last joined the symbol table",
                self._spec.name,
                list(keys),
            )
            return self._stream(
                lambda blink: latest_rows_since_added(blink, watch, keys),
                request,
                verb="last_by",
            )
        # Groups span symbols: filter first, so a group's row is the latest of a watched symbol.
        logger.debug(
            "%s.last_by(%s): latest rows by keys that span symbols, from the rows whose symbol "
            "the symbol table holds when they arrive",
            self._spec.name,
            list(keys),
        )
        return self._stream(
            lambda blink: watched_rows(blink, watch).last_by(list(keys)),
            request,
            verb="last_by",
        )


class HistoryQuery(Query):
    """A query with history: `history(start, end)` reads a past range.

    Import it for annotations::

        from deephaven_massive.typehints import HistoryQuery

    You get the query from a dataset method, such as `m.options.bars(...)` with a timespan that
    doesn't stream. Each call of `history()` reads its range again.

    Example:
        ::

            q = m.options.bars("O:AAPL250117C00150000", dm.Timespan.HOUR)
            week = q.history("2025-01-06", "2025-01-10")  # whole days, both included
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
        RestTransport.check_history,
        FlatFileTransport.history,
        RestTransport.history_by_symbol,
        FlatFileTransport.history_by_symbol,
        open_followed_read,
        _wait,
    )
    def history(
        self,
        start: DateLike,
        end: DateLike | None = None,
        *,
        source: HistorySource = HistorySource.AUTO,
        reread: Reread = Reread.ALWAYS,
        wait: bool = False,
    ) -> Table:
        """Return the dataset's rows over a range of days or of exact times.

        By default the call returns a live table with no rows at once, and the rows arrive in the
        background. Deephaven updates the table, and every table built on it, as they land. For
        symbols passed in code or in a static table, they land all in one update, and
        `m.streams()` shows the read `LOADING`, then `DONE` or `FAILED`. Pass `wait=True` to wait
        for the rows; without a watchlist, you then get a static table.

        Dates, such as `"2025-03-03"`, read whole days in the market's time zone, both ends
        included. Times, such as a `datetime` with a time zone, read from `start` up to `end`, which
        is excluded. A bar is included only when its whole window is in the range and has ended.

        Rows come from REST or from Massive's flat files. By default, the library picks the source
        and reads from REST any day the flat files can't serve. The table's description says which
        source it read. Each flat file holds a whole day of data, and a flat-file read downloads
        all of it, however few symbols or short a range you ask for.

        A static table of symbols is read once, when you call. A watchlist is followed over the
        range fixed at the call: a symbol you add arrives with all its rows, and one you delete
        leaves
        (https://github.com/chipkent/deephaven-massive/blob/main/README.md#drive-with-a-watchlist).
        One symbol's failure leaves the watchlist's other symbols in place, and `m.symbol_reads()`
        shows each symbol's read.

        Args:
            start: The first day, or the first time included.
            end: The last day included, or the first time excluded. Leave it out to read through
                now.
            source: `dm.HistorySource.AUTO`, the default, picks the source; `REST` or `FLAT_FILES`
                forces one.
            reread: For a watchlist, whether a symbol you delete and add back is downloaded again
                (`dm.Reread.ALWAYS`, the default) or gets its earlier rows back without a request
                (`dm.Reread.NEVER`).
            wait: False, the default, returns at once and fills the table in the background. True
                waits for the rows; for a watchlist, until each symbol has been read once.

        Returns:
            A table with the dataset's columns, with no row when none is in the range: live, or
            static for `wait=True` without a watchlist.

        Raises:
            TypeError: If `source` isn't a `dm.HistorySource`, `reread` isn't a `dm.Reread`, `wait`
                isn't a `bool`, or a bound isn't a date or a time.
            ValueError: At the call, before any request, if:
                - it runs on a Deephaven update-graph thread;
                - a bound is a `datetime` without a time zone, unreadable text, or `NaT`;
                - one bound is a date and the other a time;
                - `start` is after `end`, or in the future when `end` is left out;
                - a static table of symbols is empty, or no row of a table of symbols is Massive's
                  exact ticker;
                - no source can serve the request; the message gives each reason;
                - the endpoint doesn't take one of the query's parameters or filters.
            AuthenticationError: With `wait=True`, if Massive or its flat-file server rejects your
                keys.
            EntitlementError: With `wait=True`, if your plan doesn't include the data from any
                source the call can use.
            MassiveError: At the call, if the session is closed. With `wait=True`, also if:
                - a request fails;
                - adjusted bars come back unadjusted, or the reverse;
                - a trading day's flat file is missing and REST can't serve that day;
                - for a watchlist, every symbol's read failed.

        Without `wait=True`, an error from Massive fails the table instead of raising: it is
        logged and shows in `Error` of `m.streams()`. A watchlist's table fails when every symbol
        it held at the call fails for good, such as on a plan without the dataset.

        Example:
            ::

                day = m.crypto.trades("X:BTCUSD").history("2026-01-02", "2026-01-02")
                rows = m.crypto.trades("X:BTCUSD").history("2026-01-02", "2026-01-02", wait=True)
        """
        background = not _wait(wait)
        again = _require_enum(reread, Reread, "reread")
        rereads = "" if again is Reread.ALWAYS else f", reread={again!r}"
        return self._logged_call(
            f"history({start!r}, {end!r}, source={source!r}{rereads}"
            f"{'' if background else ', wait=True'})",
            lambda call: self._history(
                start, end, source=source, reread=again, call=call, wait=wait
            ),
            background=background,
        )

    def _history(
        self,
        start: DateLike,
        end: DateLike | None,
        *,
        source: HistorySource,
        reread: Reread,
        call: int,
        wait: bool,
    ) -> Table:
        """Run `history()` as call `call`; see `history` for everything else.

        Everything that needs no request is checked here, on the calling thread, before the
        transport call runs or a background read starts. A refreshing symbol table is followed
        (`_followed_history`).

        Args:
            start: As `history` takes it.
            end: As `history` takes it.
            source: As `history` takes it.
            reread: As `history` takes it.
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
        if isinstance(symbols, SymbolTable) and symbols.table.is_refreshing:
            return self._followed_history(
                call, symbols, first=first, last=last, source=chosen, reread=reread, wait=wait
            )
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

    def _followed_history(
        self,
        call: int,
        symbols: SymbolTable,
        *,
        first: date | datetime,
        last: date | datetime | None,
        source: HistorySource,
        reread: Reread,
        wait: bool,
    ) -> Table:
        """Run a `history()` call that follows a refreshing symbol table.

        The table's current rows are checked now, as a live verb checks them, and its call-time
        symbols fix the window's day zones; the window itself is fixed for every later read.
        Under `AUTO` the source is REST whenever REST can serve the request, whatever the
        number of symbols, so one added symbol never re-reads the flat files of the whole range;
        otherwise flat files. With `wait=True` the call-time symbols are read now, as one read;
        the call raises only when every one of them failed, and logs one WARNING when some did.

        Args:
            call: The call id.
            symbols: The boxed refreshing table.
            first: The parsed `start`.
            last: The parsed `end`, or None for the call's instant.
            source: As `history` takes it.
            reread: As `history` takes it.
            wait: As `history` takes it.

        Returns:
            The refreshing result (`open_followed_read`).

        Raises:
            ValueError: If no source can serve the request, as `choose_source` and
                `_check_rest_history` say.
        """
        spec, core = self._spec, self._core
        call_time = _read_symbol_table(core, spec, symbols, query=repr(self), live=True)
        request = replace(self._request, start=first, end=last, symbols=call_time)
        window = history_window(
            spec, request, start=first, end=last, classes=map(symbol_class, call_time)
        )
        span = _window_text(first, last, window.now)
        if source is HistorySource.REST:
            self._check_rest_history()
            plan = self._rest_plan(call, request, window=window)
            note = "Read from REST: requested with source=dm.HistorySource.REST."
        elif source is HistorySource.AUTO and rest_history_problem(spec, request) is None:
            plan = self._rest_plan(call, request, window=window)
            note = (
                "Read from REST: AUTO reads a followed symbol table from REST, which can serve it."
            )
        else:
            choose_source(spec, request, requested=source, config=core.config)
            files = core.flat_file_transport()
            dates = partition_dates(spec, window)
            forced = source is HistorySource.FLAT_FILES
            plan = FollowPlan(
                Transport.FLAT_FILES,
                one_scan=True,
                symbols_per_read=1,
                read=lambda names, stop: files.history_by_symbol(
                    spec,
                    request,
                    window,
                    symbols=names,
                    dates=dates,
                    call=call,
                    forced=forced,
                    stop=stop,
                ),
            )
            note = (
                "Read from flat files: requested with source=dm.HistorySource.FLAT_FILES."
                if forced
                else "Read from flat files: AUTO reads a followed symbol table from flat files, "
                f"since {rest_history_problem(spec, request)}."
            )
        logger.info("%s history: follows its symbol table; %s", spec.name, note)
        loaded = self._first_read(plan, call_time, verb="history") if wait and call_time else None
        return open_followed_read(
            core,
            spec=spec,
            call=call,
            query=repr(self),
            symbol_table=symbols.table,
            rule=SymbolRowRule.of(spec, core.config.noncanonical_symbols),
            reread=reread,
            plan=plan,
            note=f"Follows its symbol table over {span}. {note}",
            first=loaded,
        )

    def _rest_plan(self, call: int, request: Request, *, window: HistoryWindow) -> FollowPlan:
        """Return the plan of a followed `history()` that reads REST, its parameters checked.

        Args:
            call: The call id.
            request: The request with the call-time symbols and the parsed bounds.
            window: The call's window.

        Returns:
            One read per symbol (`RestTransport.history_by_symbol`).
        """
        spec = self._spec
        rest = self._core.rest_transport()
        if not window.empty:  # a parameter error raises now, with symbols or without
            rest.check_history(spec, request, window, split_pairs=False)
        return FollowPlan(
            Transport.REST,
            one_scan=False,
            symbols_per_read=1,
            read=lambda names, stop: rest.history_by_symbol(
                spec, request, window, symbols=names, call=call, stop=stop
            ),
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
        the read (`FlatFileTransport.read_history`), and what the files may lack: the days
        taken for holidays, and the futures sessions that may be incomplete. A background
        read's is written when the read starts, so under `AUTO`, when REST can serve the
        request, it says a day the flat files cannot serve is read from REST, and the read's
        WARNING names the days; the log names the days taken for holidays too.

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
            table, served, lacks = files.read_history(
                spec, request, window, dates=dates, call=call, forced=forced
            )
            source = note if served is None else served
            return self._described(table, note=source if lacks is None else f"{source} {lacks}")
        if not forced and rest_history_problem(spec, request) is None:
            # REST may read the days the files lack: its parameter errors raise now, as a read
            # that waits raises them before it reads those days.
            self._core.rest_transport().check_history(spec, request, window, split_pairs=False)
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
        if not wait and not window.empty:  # a parameter error and a pair in code raise now
            rest.check_history(spec, request, window, split_pairs=self._in_code)
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


class StreamHistoryQuery(SymbolStreamQuery, HistoryQuery):
    """A streaming query that also has history: the four stream methods and `history()`.

    Import it for annotations::

        from deephaven_massive.typehints import StreamHistoryQuery

    You get the query from a dataset method, such as `m.stocks.bars(...)` with a one-second or
    one-minute timespan, and you can reuse it. `help(StreamQuery)` covers what every stream method
    shares, and `help(SymbolStreamQuery)` covers the symbols and watchlists it takes.

    Example:
        ::

            q = m.stocks.bars(["AAPL", "MSFT"], dm.Timespan.MINUTE)
            live = q.append_only()
            day = q.history("2025-03-03", "2025-03-03")
    """

    __slots__ = ()


def query_class(spec: DatasetSpec) -> type[Query]:
    """Return the query class of a dataset, derived from the transports its spec declares.

    A dataset's verbs are exactly what its transports can serve, so the class follows from the
    spec: channels give the stream verbs, a history source (a REST history or grouped-daily
    path, or flat-file prefixes) gives `history()`, and a fetch path gives `fetch()`. Each
    namespace factory's return annotation is this class, which the namespace tests check.

    | Channels | History | Fetch | Class                |
    |----------|---------|-------|----------------------|
    | no       | no      | yes   | `FetchQuery`         |
    | yes      | no      | no    | `SymbolStreamQuery`  |
    | no       | yes     | no    | `HistoryQuery`       |
    | yes      | yes     | no    | `StreamHistoryQuery` |

    A stream whose rows name no symbol (`DatasetSpec.names_symbols` False) has channels alone and
    gives `StreamQuery`, which has no `last_by()`.

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
        return FetchQuery if spec.has_fetch else HistoryQuery
    if not spec.names_symbols:
        return StreamQuery
    return StreamHistoryQuery if spec.has_history else SymbolStreamQuery


def bar_query_class(spec: DatasetSpec, timespan: object, multiplier: object) -> type[Query]:
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
