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
`poll`, REST `history`), `StreamTransport` (opening a WebSocket stream), and `FlatFileTransport`
(flat-file `history`). `history()` chooses its source itself (`choose_source` in
`_datasets/history_source.py`) and then calls one transport, so the transports only do I/O; under
`AUTO` a flat-file read whose last days are not published yet reads those days from REST too. Each
static verb (`fetch()`, `history()`) is one *call*: it takes a process-unique id from the session's
stream-id counter and logs `call 7 start: <query>.<verb>`, then `call 7 done: <rows> rows in <s> s`
or `call 7 failed after <s> s: <error>`, at DEBUG; the transports name their worker threads after it
(`massive #1 call-7 rest_0`), so every line of one call can be found. With `wait=False` a static
verb checks its arguments on the calling thread, then returns at once a refreshing table that a
library thread of its own fills in one update when the download completes (`open_read` in
`_tables/background_read.py`); that thread logs the call's outcome. A `history()` or `fetch()` whose
symbols are a refreshing table follows it instead: the result is always refreshing, each symbol
added to the table is read (by `history()` over the call's fixed window, by `fetch()` as Massive
answers then) and lands whole, and each symbol removed leaves it (`open_followed_read` in
`_tables/followed_read.py`, run by the session's follow coordinator). A table passed where a
dataset takes one value (`Request.value_table`: an option chain's underlying, a futures product
code) is read value by value, each as if passed alone (`RestTransport.fetch_by_value`): a static
one once, a refreshing one followed by `fetch()` the same way, keyed by the value, and re-read by
each `poll()` cycle. Live stream results are shaped
and tracked by the session (`SessionCore.stream_result`), so every stream verb shares one stream per
(dataset, channel, symbol set).

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
from deephaven_massive._datasets.symbols import SymbolTable, symbol_class
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
from deephaven_massive._types import ALL, override

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable, Mapping

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.request import Request, ValueTable
    from deephaven_massive._tables.transports import SymbolOutcome
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

_FAILED_SHOWN: Final = 5
"""The most failed call-time symbols the WARNING of a followed `history(wait=True)` names."""

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

        Every verb of the dataset returns these columns in this order, whichever source serves
        it. A column the serving source doesn't supply is null. A value Massive sends that
        doesn't fit its column is null, logged once, and named in the result's `SchemaDrift` in
        `m.streams()`; a row missing its symbol or time is dropped. A symbol spelling the library
        can't read keeps its row, with a null `Symbol` and the text in `RawSymbol`. The call makes
        no request, opens no stream, and works on a closed session.

        Returns:
            A static table with one row per column, in result column order:
            `Name` (String), the column name; `Type` (String), its Deephaven type (`long`,
            `int`, `double`, `boolean`, `String`, `Instant`, `long[]`, or `String[]`; every
            calendar date, such as `Date`, is ISO `yyyy-MM-dd` text, a `String`);
            `Description` (String), what the column holds, empty when Massive gives no
            description; and `SuppliedBy` (String), the sources that fill the column for this
            dataset, spelled `REST`, `flat files`, and `WebSocket`, in that order, joined by
            ", ". `fetch()` and `poll()` read REST, the stream verbs read the WebSocket, and
            `history()` reads REST or flat files. Every `SuppliedBy` names at least one source.

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
        if verb in ("fetch", "poll") and snapshots is not None:
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
        _read_value_table,
        _merged,
        RestTransport.fetch,
        RestTransport.fetch_by_symbol,
        RestTransport.fetch_by_value,
        open_followed_read,
        _wait,
    )
    def fetch(self, *, wait: bool = True) -> Table:
        """Return every row the dataset's endpoint returns, all pages of it.

        A `limit` the query was built with caps the rows. An endpoint that takes one symbol per
        request reads up to `max_concurrency` symbols at once. A static symbol table is read once,
        when you call; a crypto pair in it that Massive doesn't list follows
        `MassiveConfig.unlisted_symbols`. A table passed as `underlying=` or `product_code=` is read
        value by value, each as if passed alone; if it's static, one failed value fails the call.

        A live symbol table, a watchlist, is followed: the result holds the rows of each symbol's
        latest read, Massive's answer as of that read. A symbol you delete leaves in one update,
        and one you add back is read again. One symbol's failure leaves the others in place, and
        `m.symbol_reads()` shows each symbol's state. A live `underlying=` or `product_code=` table
        is followed the same way, keyed by its value.

        A symbol Massive has no data for (HTTP 404 on its first request) follows
        `MassiveConfig.unavailable_symbols`: by default it gets no rows and logs a WARNING. Current
        data must be real-time unless its subscription is in
        `MassiveConfig.delayed_subscriptions`: a response Massive labels `DELAYED` raises. Data of
        a listed subscription is returned as your plan serves it, and the table's description says
        so (https://github.com/chipkent/deephaven-massive/blob/main/README.md#configuration).

        Args:
            wait: True (the default) blocks and returns a static table, or for a watchlist blocks
                until each symbol on it has had one read. False returns at once a live table that
                a background read fills in one update.

        Returns:
            A static table in the dataset's schema. With `wait=False`, a live table filled once
            in the background. For a watchlist, a live table that follows it.

        Raises:
            TypeError: If `wait` isn't a `bool`.
            ValueError: Before any request: on a Deephaven update-graph thread (inside a table
                listener, say); if a path the call reads doesn't take a parameter or filter the
                query sets; if a symbol table, or a table of values, holds no value, only rows
                the dataset doesn't take, or any such row under `noncanonical_symbols=RAISE`;
                or if both `symbols` and a table of values are live tables.
            AuthenticationError: If Massive rejects your API key (HTTP 401). With `wait=False`,
                this and every other request failure fail the table instead of raising, and a
                watchlist's table fails on this one too.
            EntitlementError: If your plan doesn't include the endpoint (HTTP 403), or Massive
                labels the data `DELAYED` for a subscription not in
                `MassiveConfig.delayed_subscriptions`; the message names the line to add.
            MassiveError: If the session is closed, a rate limit outlasts the retries, every
                row of a page is unreadable, a symbol's data stops partway with HTTP 404, a crypto
                pair can't be split into its currencies (for a symbol table's pair, only under
                `unlisted_symbols=RAISE`, or when no pair in it is listed), or a request fails
                in any other way. For a watchlist with `wait=True`, only when every symbol on it
                failed, with the first symbol's error, or for an unlisted pair under
                `unlisted_symbols=RAISE`.

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
        if not wait:  # a parameter error raises now, as it would before the first request
            rest.check_fetch(spec, request)
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
            rest.check_fetch(spec, request)
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
        if not wait:  # a parameter error raises now, as it would before the first request
            rest.check_fetch(spec, request.for_value(names[0]))
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
            rest.check_fetch(spec, request.for_value(call_time[0]))
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
        """Return a live table that re-runs `fetch()` every `interval`.

        Each cycle reads the whole query again, including a symbol table or a table passed as
        `underlying=` or `product_code=`, so the table follows a watchlist. Some symbols are left
        out of a cycle and listed in `m.streams()`: a row that isn't Massive's exact ticker, a
        crypto pair Massive doesn't list, and a symbol Massive has no data for. So is a symbol
        whose path doesn't take a parameter the query sets, such as `adjusted=False` on a crypto
        indicator. To fail the table instead, set `noncanonical_symbols`, `unlisted_symbols`, or
        `unavailable_symbols` to `RAISE`. A feed, a dataset whose default mode is `APPEND` such as
        news and filings, needs a `start` or a `limit`. The table polls for as long as it, or any
        table made from it, is in use
        (https://github.com/chipkent/deephaven-massive/blob/main/README.md#follow-the-news).

        A failed cycle shows in the `Error` column of `m.streams()`, logs a WARNING, and the next
        cycle runs. A few failures fail the table and stop polling: authentication, entitlement,
        a schema failure, a null or NaN key, and a response Massive labels `DELAYED` for a
        subscription not in `MassiveConfig.delayed_subscriptions`
        (https://github.com/chipkent/deephaven-massive/blob/main/README.md#configuration). The
        table's description and its `m.streams()` row say whether its data is real-time or
        delayed. `TimestampInsert` is the time of the latest cycle.

        Args:
            interval: Seconds between cycles, or a `timedelta`; at least 1 second. Use a stream
                for anything faster.
            mode: How cycles combine. `AUTO` (the default) is the dataset's own mode, which its page
                in `docs/datasets` names: `REPLACE` keeps the latest cycle by key, `UPSERT` keeps
                every key ever seen at its latest row, `APPEND` adds only rows whose identity (the
                poll keys, plus the update time on snapshots and movers) is new.

        Returns:
            A live table: keyed for `REPLACE` and `UPSERT`, append-only for `APPEND`. It
            remembers every key or identity it has held, so its memory grows with the keys seen.

        Raises:
            TypeError: If `interval` isn't a number or a `timedelta`, or `mode` isn't a
                `dm.PollMode`.
            ValueError: Before the table exists: on a Deephaven update-graph thread; if
                `interval` isn't finite or is shorter than 1 second; if a feed's query has
                neither `start` nor `limit`; if a symbol table holds only rows that aren't
                Massive's exact tickers, or any under `noncanonical_symbols=RAISE`; or if a path
                doesn't take a parameter or filter the query sets.
            AuthenticationError: If Massive rejects your API key while the call reads Massive's
                crypto ticker list, which a crypto path needs to split a pair into its currencies.
            EntitlementError: If your plan doesn't include that list.
            MassiveError: If the session is closed, or a crypto ticker passed in code can't be
                split into its currencies.
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


class StreamQuery(QueryBase):
    """A WebSocket dataset: four live shapes of one shared stream.

    Built by a dataset method, such as `m.stocks.luld(...)`; immutable and reusable. Every verb
    of an equal query shares one stream, and each call returns its own table. The stream stays
    subscribed while any of its tables, or a table made from one, is in use.

    A verb returns once its table exists, without waiting for Massive to confirm the
    subscription. A topic Massive hasn't confirmed is sent again, and `m.streams()` names it in
    `Error` until Massive confirms it. A failure Massive reports later, such as a refused topic
    or a rejected key, fails the stream's tables and shows in `m.streams()`. A row that can't be
    decoded is dropped and logged, and the stream goes on. A verb whose stream fails while it
    opens raises `MassiveError` naming the failure. Once a stream has failed, the next verb of an
    equal query opens a new one.

    A live symbol table, a watchlist, is followed: the stream subscribes and unsubscribes as you
    edit it. An empty watchlist opens its stream and subscribes as rows arrive. A static table is
    read once, when you call, and opens the same stream as an equal list of symbols. A row that
    isn't Massive's exact ticker follows `MassiveConfig.noncanonical_symbols`. Under `LOG`, the
    default, and `IGNORE`, it's never subscribed, and `m.streams()` lists it in `Error` until you
    fix or delete it. Under `RAISE`, it raises `ValueError` at the call, or fails the stream when
    you add it later. A table whose rows are all bad, or a static table with no symbol, raises
    `ValueError`.

    Options contracts listed in code that would put more than 1,000 topics on the options
    connection raise `ValueError` at once. A watchlist's contracts past that limit wait until
    room frees up instead, and `Deferred` in `m.streams()` counts them.

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
        `_read_symbol_table`; under `LOG` and `IGNORE` such a row is skipped. A crypto pair of
        the table that Massive's crypto ticker list does not hold follows
        `unlisted_symbols` (`StreamTransport.tradable_pairs`).

        Returns:
            The request.
        """
        request = self._request
        symbols = request.symbols
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
        StreamTransport.tradable_pairs,
    )
    def blink(self) -> Table:
        """Return the stream as a blink table: each update cycle holds only that cycle's rows.

        Every verb of an equal query shares one stream, and each call returns its own table.
        The stream stays subscribed while any of its tables, or a table made from one, is in
        use. A static symbol table is read once, when you call. A live one, a watchlist, is
        followed: the stream subscribes and unsubscribes as you edit it. A row that isn't
        Massive's exact ticker is skipped and listed in `m.streams()`; `dm.canonicalize_symbols`
        converts other spellings. A failure Massive reports after the call, such as a refused
        topic or a rejected key, fails the table and shows in `m.streams()`.

        Returns:
            A live blink table in the dataset's schema.

        Raises:
            ValueError: On a Deephaven update-graph thread (inside a table listener, say); if
                a static symbol table holds no symbol; if a symbol table holds only rows that
                aren't Massive's exact tickers, or any such row under
                `noncanonical_symbols=OnError.RAISE`; if options contracts listed in code would put
                more than 1,000 topics on the options connection; or for a fair-market-value
                stream of a subscription in `MassiveConfig.delayed_subscriptions`, since FMV is
                real-time only.
            AuthenticationError: If Massive rejects your API key when the stream first opens.
            EntitlementError: If Massive shows that your plan's data for this dataset is delayed
                and its subscription isn't in `MassiveConfig.delayed_subscriptions`; the message
                names the line to add
                (https://github.com/chipkent/deephaven-massive/blob/main/README.md#configuration).
            MassiveError: If the session is closed; if the stream fails while the call opens it,
                naming the failure; or if Massive's crypto ticker list can't be read, lacks a
                crypto pair listed in code (or a symbol table's pair, under
                `unlisted_symbols=OnError.RAISE`), or holds no pair of a symbol table.
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
        StreamTransport.tradable_pairs,
    )
    def append_only(self) -> Table:
        """Return every row received since the call, in arrival order.

        Memory grows with the stream; use `ring()` to bound it.

        Every verb of an equal query shares one stream, and each call returns its own table.
        The stream stays subscribed while any of its tables, or a table made from one, is in
        use. A static symbol table is read once, when you call. A live one, a watchlist, is
        followed: the stream subscribes and unsubscribes as you edit it. A row that isn't
        Massive's exact ticker is skipped and listed in `m.streams()`; `dm.canonicalize_symbols`
        converts other spellings. A failure Massive reports after the call, such as a refused
        topic or a rejected key, fails the table and shows in `m.streams()`.

        Returns:
            A live append-only table in the dataset's schema.

        Raises:
            ValueError: On a Deephaven update-graph thread (inside a table listener, say); if
                a static symbol table holds no symbol; if a symbol table holds only rows that
                aren't Massive's exact tickers, or any such row under
                `noncanonical_symbols=OnError.RAISE`; if options contracts listed in code would put
                more than 1,000 topics on the options connection; or for a fair-market-value
                stream of a subscription in `MassiveConfig.delayed_subscriptions`, since FMV is
                real-time only.
            AuthenticationError: If Massive rejects your API key when the stream first opens.
            EntitlementError: If Massive shows that your plan's data for this dataset is delayed
                and its subscription isn't in `MassiveConfig.delayed_subscriptions`; the message
                names the line to add
                (https://github.com/chipkent/deephaven-massive/blob/main/README.md#configuration).
            MassiveError: If the session is closed; if the stream fails while the call opens it,
                naming the failure; or if Massive's crypto ticker list can't be read, lacks a
                crypto pair listed in code (or a symbol table's pair, under
                `unlisted_symbols=OnError.RAISE`), or holds no pair of a symbol table.
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
        StreamTransport.tradable_pairs,
        require_positive_int,
    )
    def ring(self, capacity: int) -> Table:
        """Return the most recent `capacity` rows received.

        Every verb of an equal query shares one stream, and each call returns its own table.
        The stream stays subscribed while any of its tables, or a table made from one, is in
        use. A static symbol table is read once, when you call. A live one, a watchlist, is
        followed: the stream subscribes and unsubscribes as you edit it. A row that isn't
        Massive's exact ticker is skipped and listed in `m.streams()`; `dm.canonicalize_symbols`
        converts other spellings. A failure Massive reports after the call, such as a refused
        topic or a rejected key, fails the table and shows in `m.streams()`.

        Args:
            capacity: How many rows to keep, from 1 to 2**31 - 1.

        Returns:
            A live ring table in the dataset's schema.

        Raises:
            TypeError: If `capacity` isn't an `int`.
            ValueError: On a Deephaven update-graph thread (inside a table listener, say); if
                a static symbol table holds no symbol; if a symbol table holds only rows that
                aren't Massive's exact tickers, or any such row under
                `noncanonical_symbols=OnError.RAISE`; if options contracts listed in code would put
                more than 1,000 topics on the options connection; or for a fair-market-value
                stream of a subscription in `MassiveConfig.delayed_subscriptions`, since FMV is
                real-time only;
                or if `capacity` is outside 1 to 2**31 - 1.
            AuthenticationError: If Massive rejects your API key when the stream first opens.
            EntitlementError: If Massive shows that your plan's data for this dataset is delayed
                and its subscription isn't in `MassiveConfig.delayed_subscriptions`; the message
                names the line to add
                (https://github.com/chipkent/deephaven-massive/blob/main/README.md#configuration).
            MassiveError: If the session is closed; if the stream fails while the call opens it,
                naming the failure; or if Massive's crypto ticker list can't be read, lacks a
                crypto pair listed in code (or a symbol table's pair, under
                `unlisted_symbols=OnError.RAISE`), or holds no pair of a symbol table.
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
        SessionCore.stream_result,
        StreamTransport.open_stream,
        StreamTransport.tradable_pairs,
        _last_by_keys,
    )
    def last_by(self, by: str | Sequence[str] = "Symbol") -> Table:
        """Return the latest row per key, by default one row per `Symbol`.

        The table keeps every key it has seen. On a watchlist, with `Symbol` among the keys, a
        symbol's row shows only while the symbol is on the watchlist. The row must also have
        arrived after the symbol last joined it, so a symbol you delete and add back shows no row
        until its next update arrives. With keys that span symbols (`by="Exchange"`), each
        group's row is the latest from a symbol on the watchlist when the row arrived. Deleting
        that symbol keeps the group's row. `last_by()` starts empty and fills as
        rows arrive. For current state at once, poll the snapshots, as in
        `snapshots(symbols).poll(5)`, on a plan that includes snapshots
        (https://github.com/chipkent/deephaven-massive/blob/main/README.md#what-your-plan-allows).

        Every verb of an equal query shares one stream, and each call returns its own table.
        The stream stays subscribed while any of its tables, or a table made from one, is in
        use. A static symbol table is read once, when you call. A live one, a watchlist, is
        followed: the stream subscribes and unsubscribes as you edit it. A row that isn't
        Massive's exact ticker is skipped and listed in `m.streams()`; `dm.canonicalize_symbols`
        converts other spellings. A failure Massive reports after the call, such as a refused
        topic or a rejected key, fails the table and shows in `m.streams()`.

        Args:
            by: The key column or columns, positionally or by keyword as in Deephaven's
                `Table.last_by` (`q.last_by("Exchange")`); `"Symbol"` by default. Each column
                may appear once.

        Returns:
            A live table with one row per key, in the dataset's schema.

        Raises:
            TypeError: If `by` isn't a `str` or a sequence of `str`.
            ValueError: On a Deephaven update-graph thread (inside a table listener, say); if
                a static symbol table holds no symbol; if a symbol table holds only rows that
                aren't Massive's exact tickers, or any such row under
                `noncanonical_symbols=OnError.RAISE`; if options contracts listed in code would put
                more than 1,000 topics on the options connection; or for a fair-market-value
                stream of a subscription in `MassiveConfig.delayed_subscriptions`, since FMV is
                real-time only;
                or if `by` is empty, repeats a column, or names a column the dataset doesn't
                have.
            AuthenticationError: If Massive rejects your API key when the stream first opens.
            EntitlementError: If Massive shows that your plan's data for this dataset is delayed
                and its subscription isn't in `MassiveConfig.delayed_subscriptions`; the message
                names the line to add
                (https://github.com/chipkent/deephaven-massive/blob/main/README.md#configuration).
            MassiveError: If the session is closed; if the stream fails while the call opens it,
                naming the failure; or if Massive's crypto ticker list can't be read, lacks a
                crypto pair listed in code (or a symbol table's pair, under
                `unlisted_symbols=OnError.RAISE`), or holds no pair of a symbol table.
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
        wait: bool = True,
    ) -> Table:
        """Return the dataset's rows over a range of days or of exact instants.

        Pass dates (`"2025-03-03"`, a `date`, `dh_today()`) to read whole days in the asset
        class's time zone, both ends included. Pass aware datetimes or instants to read an exact
        range that includes `start` and excludes `end`. A bar is in the result only when its
        whole window lies in the range and has ended.

        Rows come from REST or from the flat files (see `dm.HistorySource`). Under `AUTO`, days
        whose flat file is missing, and files your plan doesn't include, are read from REST, so
        the call fails only when neither source can serve it. The table's description says which
        source it read and why. `MassiveConfig.delayed_subscriptions` doesn't apply to history
        (https://github.com/chipkent/deephaven-massive/blob/main/README.md#configuration).

        A static symbol table is read once, when you call. A live one, a watchlist, is followed
        (https://github.com/chipkent/deephaven-massive/blob/main/README.md#history-of-a-watchlist).
        The result holds each listed symbol's complete rows over the range, which is fixed when you
        call, so `end=None` means through that moment. A symbol you add arrives with all its rows
        in one update, and one you delete leaves in one update. `AUTO` reads a watchlist from REST
        whenever REST can serve it. One symbol's failure leaves the others in place. A symbol
        Massive has no data for (HTTP 404) gets no rows. A failure that may pass, such as a rate
        limit or a network error, is retried 1, 5, and 15 minutes later, and any other failure
        leaves the symbol out. `m.symbol_reads()` shows each symbol's state.

        Args:
            start: The first day, or the first instant included (`DateLike`).
            end: The last day included, or the first instant excluded. None (the default) reads
                through now.
            source: `dm.HistorySource.AUTO` (the default) picks the source; `REST` or
                `FLAT_FILES` forces one.
            reread: For a watchlist, whether a symbol you delete and add again is downloaded
                again (`dm.Reread.ALWAYS`, the default) or gets its earlier rows back with no
                request (`NEVER`). Ignored for any other `symbols`.
            wait: True (the default) blocks until the rows are read, or for a watchlist until
                each symbol on it has had one read. False returns at once a live table that the
                read fills in the background.

        Returns:
            A static table in the dataset's schema, empty when no row lies in the range. With
            `wait=False`, a live table filled once in the background. For a watchlist, a live
            table that follows it. The dataset's page lists the columns.

        Raises:
            TypeError: If `source` isn't a `dm.HistorySource`, `reread` isn't a `dm.Reread`, a
                bound isn't `DateLike`, or `wait` isn't a `bool`.
            ValueError: Before any request, for a naive, unparseable, or `NaT` bound; a date
                mixed with a datetime; `start` after `end` or after now; an empty or unreadable
                symbol table; futures session dates with a `session_end_date` filter; a source
                that can't serve the request (the message lists each reason); or a call on a
                Deephaven update-graph thread, such as inside a table listener.
            AuthenticationError: If Massive or S3 rejects your keys. With `wait=False`, this
                and every other request failure fail the table instead of raising, and a
                watchlist's table fails on this one too.
            EntitlementError: If your plan doesn't include the data. Under `AUTO`, flat files
                your plan lacks are read from REST instead, when REST can serve them.
            MassiveError: If the session is closed or a request fails; if adjusted bars come
                back on the other basis; if a symbol's data stops partway with HTTP 404; or, under
                `FLAT_FILES` or when REST can't serve the read, if a day's flat file isn't
                published yet or is a week or more old and missing. For a watchlist with
                `wait=True`, only when every symbol on it failed, with the first symbol's error.

        Example:
            ::

                day = m.crypto.trades("X:BTCUSD").history("2026-01-02", "2026-01-02")
        """
        background = not _wait(wait)
        again = _require_enum(reread, Reread, "reread")
        rereads = "" if again is Reread.ALWAYS else f", reread={again!r}"
        return self._logged_call(
            f"history({start!r}, {end!r}, source={source!r}{rereads}"
            f"{', wait=False' if background else ''})",
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
            rest.check_history(spec, request, window)
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
