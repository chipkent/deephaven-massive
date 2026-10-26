"""The six query classes and their verbs, and the transport interfaces the verbs call.

A dataset method such as `m.stocks.trades(["AAPL"])` returns a *query*: an immutable,
hashable description of what to fetch. Nothing touches the network until a *verb* is called,
and each verb returns a Deephaven `Table` of a particular shape:

| Class                      | Verbs                                                     |
|----------------------------|-----------------------------------------------------------|
| `Query`                    | `fetch()`, `poll(interval)`                               |
| `StreamQuery`              | `blink()`, `append_only()`, `ring(capacity)`, `last_by()` |
| `HistoryQuery`             | `history(start, end)`                                     |
| `StreamHistoryQuery`       | the stream verbs and `history()`                          |
| `StreamHistoryLatestQuery` | the above and `fetch()` (the "last" endpoint)             |
| `HistoryLatestQuery`       | `history()` and `fetch()` (the previous session's bar)    |

A verb that cannot work for a dataset does not exist on its query class, so tab completion is the
contract. Verbs validate their parameters synchronously on the calling thread: a wrong type raises
`TypeError` and an invalid value raises `ValueError`. Every verb also raises `ValueError` on a
Deephaven update-graph thread (for example inside a table listener), because verbs create tables and
take the session's registry lock.

The verbs delegate the transport work to three interfaces, one per transport, which the session
provides through its factory hooks: `RestTransport` (`fetch`, `poll`, REST `history`),
`StreamTransport` (opening a WebSocket stream), and `FlatFileTransport` (flat-file `history`).
`history()` chooses its source itself (`_sources.choose_source`) and then calls one transport,
so the transports only do I/O.
Live stream results are shaped and tracked by the session (`SessionCore.stream_result`), so
every stream verb shares one stream per (dataset, channel, symbol set).

The classes are importable from this private module for type annotations only; users never
construct them.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, fields, replace
from datetime import date, datetime, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Final, Protocol, TypeAlias

from deephaven.stream import blink_to_append_only
from deephaven.table import Table
from deephaven.table_factory import ring_table

from deephaven_massive._config import require_positive_int, require_positive_number
from deephaven_massive._dates import bounds, parse_date_like
from deephaven_massive._enums import (
    AssetClass,
    ChannelFamily,
    FuturesVenue,
    HistorySource,
    PollMode,
    Recency,
    Timespan,
    _require_enum,
)
from deephaven_massive._liveness import entered
from deephaven_massive._sources import choose_source, partition_dates, rest_history_problem
from deephaven_massive._symbols import SymbolTable, canonical_view, symbol_canonicalizer
from deephaven_massive._types import ALL, override

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from enum import StrEnum

    from deephaven_massive._catalog import DatasetSpec
    from deephaven_massive._dates import FuturesSessions, TimeRange
    from deephaven_massive._liveness import StreamHandle
    from deephaven_massive._session import SessionCore
    from deephaven_massive._symbols import NormalizedSymbols
    from deephaven_massive._types import DateLike

__all__ = [
    "FlatFileTransport",
    "HistoryLatestQuery",
    "HistoryQuery",
    "HostDecision",
    "ParamValue",
    "Query",
    "QueryBase",
    "Request",
    "RestTransport",
    "StreamHistoryLatestQuery",
    "StreamHistoryQuery",
    "StreamKey",
    "StreamQuery",
    "StreamTransport",
]

logger = logging.getLogger(__name__)

MAX_RING_CAPACITY: Final = 2**31 - 1
"""The largest `ring()` capacity Deephaven accepts (a Java `int`)."""

ParamValue: TypeAlias = "str | int | float | bool | date | datetime | StrEnum | tuple[str, ...]"
"""A normalized parameter or filter value, as a `Request` stores it (sequences become tuples)."""

StreamKey: TypeAlias = "tuple[str, str | None, NormalizedSymbols]"
"""The identity of a stream: `(dataset name, channel, symbols)`. The symbols are the sorted
tuple of canonical symbols, `ALL`, or a `SymbolTable` box, which compares by the identity of the
reactive table's wrapper and holds it strongly while the key exists."""


@dataclass(frozen=True, slots=True)
class Request:
    """The normalized arguments of a dataset call, shared by every verb of its query.

    Built by `_domains/base.py` when the query is created; immutable and hashable, so equal
    calls give equal queries and share streams.

    Attributes:
        symbols: The normalized symbols, or None for datasets without instruments.
        timespan: The bar timespan (bars and indicators only).
        multiplier: The bar multiplier.
        venue: The futures venue, which selects flat-file prefixes only.
        adjusted: Whether bars are adjusted for splits, for datasets with an `adjusted`
            parameter; None for datasets without one. Flat files hold only unadjusted bars.
        start: The lower bound of the dataset's time axis (`…T` datasets), or of `history()`.
        end: The upper bound of the dataset's time axis, or of `history()`.
        params: The dataset's other typed parameters, by SDK parameter name, sorted by name.
        filters: The `filters` mapping, by SDK parameter name, sorted by name.
        limit: The cap on the total number of rows across pages, or None for every row. It is
            not an SDK parameter: the page size is internal (`DatasetSpec.page_size`), and
            the REST transport stops paging once `limit` rows have arrived.
    """

    symbols: NormalizedSymbols | None = None
    timespan: Timespan | None = None
    multiplier: int = 1
    venue: FuturesVenue | None = None
    adjusted: bool | None = None
    start: date | datetime | None = None
    end: date | datetime | None = None
    params: tuple[tuple[str, ParamValue], ...] = ()
    filters: tuple[tuple[str, ParamValue], ...] = ()
    limit: int | None = None

    def symbol_key(self) -> NormalizedSymbols:
        """Return the symbol part of a stream key.

        Returns:
            The sorted symbol tuple, `ALL`, or the reactive table's `SymbolTable` box.

        Raises:
            ValueError: If the request has no symbols (a stream always has some).
        """
        if self.symbols is None:
            raise ValueError("a stream needs symbols")
        return self.symbols

    def describe(self) -> str:
        """Render the fields that differ from their defaults, for a query's `repr`.

        Returns:
            Space-separated `name=value` pairs, such as `symbols=('AAPL', 'MSFT')`; `ALL`
            renders as `ALL`, a reactive table as `<reactive table>`, enums by member name,
            dates in ISO form, and parameters and filters as dictionaries.
        """
        parts = []
        for f in fields(self):
            value = getattr(self, f.name)
            if value != f.default:
                parts.append(f"{f.name}={_render(value)}")
        return " ".join(parts)


def _render(value: object) -> str:
    """Render one request value for a `repr`.

    Args:
        value: A field value of a `Request`.

    Returns:
        Its display form.
    """
    if value is ALL:
        return "ALL"
    if isinstance(value, SymbolTable):
        return "<reactive table>"
    if isinstance(value, Enum):
        return f"{type(value).__name__}.{value.name}"
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, tuple) and value and isinstance(value[0], tuple):
        return "{" + ", ".join(f"{k!r}: {_render(v)}" for k, v in value) + "}"
    return repr(value)


@dataclass(frozen=True, slots=True)
class HostDecision:
    """Which WebSocket host serves a (asset class, channel family), and why.

    Returned by `RestTransport.resolve_host`; immutable, so it can be cached and shared between
    threads.

    Attributes:
        host: The hostname, such as `"delayed.massive.com"`.
        recency: Whether the host serves real-time or delayed data, or `UNKNOWN` when the
            evidence did not say (only then may a server verdict re-route the connection).
        evidence: Why: a human-readable account of the decision, shown in `m.connections()`.
    """

    host: str
    recency: Recency
    evidence: str


class RestTransport(Protocol):
    """The REST transport, provided by `SessionCore.rest_transport()`.

    Every method runs synchronously on the calling thread and raises `EntitlementError` on
    HTTP 403 and `MassiveError` on other failures; `poll()` instead fails its live table.
    """

    def resolve_host(self, asset_class: AssetClass, family: ChannelFamily) -> HostDecision:
        """Decide the WebSocket host of a (class, family), once per session.

        Called by the WebSocket transport on the caller thread at the first stream verb for the
        pair. The first call may make one REST request; the decision is then cached for the
        life of the session, so later calls return it without a request. Each new decision
        writes the pair's `Transport=REST` row of `m.connections()` (host, recency, evidence).

        Args:
            asset_class: The asset class of the stream.
            family: The channel family, `TICK` or `FMV`.
        """
        ...

    def fetch(self, spec: DatasetSpec, request: Request) -> Table:
        """Return every row the dataset's `fetch` endpoint returns for the request, static.

        Args:
            spec: The dataset.
            request: The normalized arguments.
        """
        ...

    def poll(
        self, spec: DatasetSpec, request: Request, *, interval: timedelta, mode: PollMode
    ) -> Table:
        """Return a live table that re-runs `fetch` every `interval`, tracked by the session.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            interval: The time between cycles.
            mode: How cycles combine.
        """
        ...

    def history(
        self, spec: DatasetSpec, request: Request, window: TimeRange | FuturesSessions
    ) -> Table:
        """Return the dataset's REST history over `window`, paged internally, static.

        Args:
            spec: The dataset.
            request: The normalized arguments, with the raw `start` and `end` bounds.
            window: The bounds under the class's day rules: a half-open `TimeRange`, or the
                `FuturesSessions` of date-bounded futures.
        """
        ...


class StreamTransport(Protocol):
    """The WebSocket transport, provided by `SessionCore.stream_transport()`."""

    def open_stream(
        self, spec: DatasetSpec, request: Request, channel: str | None, *, stream_id: int
    ) -> StreamHandle:
        """Create a new stream: its publisher and blink table in a new stream scope.

        Runs on the caller thread; may resolve the host with one REST call. Must not hold the
        session's registry lock while calling Deephaven. The session has already registered the
        stream under `stream_id`, so `SessionCore.fail_stream(stream_id, ...)` and
        `SessionCore.stream_shut_down(stream_id)` may be called from the moment the publisher
        exists, even before this method returns. The publisher is named
        `massive.{dataset}#{stream_id}`, and the blink table's `on_shutdown_callback` must first
        call `SessionCore.stream_shut_down(stream_id)` and only then release the stream's topics.
        The returned handle carries `stream_id`.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            channel: The WebSocket channel.
            stream_id: The id the session minted for the stream.
        """
        ...


class FlatFileTransport(Protocol):
    """The flat-file transport, provided by `SessionCore.flat_file_transport()`.

    It only reads files: `HistoryQuery.history()` has already chosen flat files as the source
    (`_sources.choose_source`).
    """

    def history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: TimeRange | FuturesSessions,
        *,
        dates: Sequence[date],
    ) -> Table:
        """Return the rows of `window` from the dataset's flat files, static.

        Args:
            spec: The dataset, with a flat-file prefix for the request.
            request: The normalized arguments, with the raw `start` and `end` bounds and static
                symbols (never a reactive table).
            window: The bounds under the class's day rules: a half-open `TimeRange`, or the
                `FuturesSessions` of date-bounded futures.
            dates: The partition dates to read (`_sources.partition_dates`).
        """
        ...


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


def _require_explicit_symbols(spec: DatasetSpec, request: Request) -> None:
    """Reject `dm.ALL` for a per-ticker `fetch()` endpoint.

    Args:
        spec: The dataset.
        request: The normalized arguments.

    Raises:
        ValueError: If the symbols are `dm.ALL`; the message names `snapshots(dm.ALL)`.
    """
    if request.symbols is ALL:
        namespace = spec.name.split(".", 1)[0]
        raise ValueError(
            f"{spec.name}.fetch() needs explicit symbols; "
            f"use m.{namespace}.snapshots(dm.ALL).fetch() for every symbol"
        )


def _read_symbol_table(
    core: SessionCore, spec: DatasetSpec, symbols: SymbolTable
) -> tuple[str, ...]:
    """Read a reactive symbol table once, for a static `history()` result.

    The table is read with `Table.iter_tuple`, which holds the update graph's shared lock while
    it reads, so the symbols are one consistent snapshot. Runs on the caller thread, inside the
    session's execution context, with no library lock held. Each row is canonicalized as the
    stream verbs do (`_symbols.symbol_canonicalizer`): a row that is not a valid symbol of the
    class is ignored and logged once at WARNING.

    Args:
        core: The session core, for its execution context.
        spec: The dataset, whose asset class canonicalizes the symbols.
        symbols: The boxed reactive table.

    Returns:
        The table's valid symbols, canonical, sorted, and de-duplicated.

    Raises:
        ValueError: If the table holds no valid symbol, as an empty symbol list does.
    """
    canonical_symbol = symbol_canonicalizer(spec.asset_class)
    with entered(core.exec_ctx):
        raw = {canonical_symbol(row[0]) for row in symbols.table.iter_tuple(cols="Symbol")}
    found = tuple(sorted(symbol for symbol in raw if symbol is not None))
    if not found:
        raise ValueError(f"{spec.name} history: the symbol table holds no valid symbols")
    return found


@dataclass(frozen=True, repr=False, slots=True)
class QueryBase:
    """The state every query class shares: the session, the dataset, and the arguments.

    Frozen and hashable; equality includes the concrete class, so a `StreamQuery` never equals
    a `HistoryQuery`. Queries have no public attributes: the verbs are the whole API, and the
    `repr` names the class, the dataset, and the arguments that differ from their defaults.
    """

    _core: SessionCore
    _spec: DatasetSpec
    _request: Request

    @override
    def __repr__(self) -> str:
        """Show the query class, the dataset, and the non-default arguments.

        Returns:
            For example `<StreamHistoryLatestQuery stocks.trades symbols=('AAPL', 'MSFT')>`.
        """
        described = self._request.describe()
        suffix = f" {described}" if described else ""
        return f"<{type(self).__name__} {self._spec.name}{suffix}>"


class Query(QueryBase):
    """A REST list or snapshot dataset: a static `fetch()` or a live `poll()`.

    No namespace declares a dataset of this kind yet, so no query can be built and there is no
    example here yet.
    """

    __slots__ = ()

    def fetch(self) -> Table:
        """Return every row the dataset's endpoint returns, fully paginated, as a static table.

        The request runs synchronously on the calling thread. `limit`, when the dataset was
        built with one, caps the total number of rows; the page size is internal. Columns are
        the dataset's schema. HTTP 403 raises `EntitlementError` and any other failure
        `MassiveError`.

        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        Returns:
            A static table.
        """
        self._core.require_verb_thread()
        return self._core.rest_transport().fetch(self._spec, self._request)

    def poll(self, interval: float | timedelta, *, mode: PollMode | None = None) -> Table:
        """Return a live table that re-runs `fetch()` every `interval`.

        A reactive `Table` passed as `symbols` is re-read once per cycle on the poller thread.
        Fan-out endpoints issue one request per symbol per cycle, at most `max_concurrency` at a
        time. Releasing the table (`m.release`, garbage collection) stops its poller. A
        non-numeric `interval` or a `mode` that is not a `PollMode` raises `TypeError`, and a
        non-positive `interval` raises `ValueError`.

        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        Args:
            interval: Seconds between cycles, or a `timedelta`; must be positive.
            mode: How cycles combine (`PollMode`); None uses the dataset's default, `APPEND`
                for news and filings and `REPLACE` otherwise.

        Returns:
            A live table: keyed for `REPLACE` and `UPSERT`, append-only for `APPEND`.
        """
        self._core.require_verb_thread()
        period = _interval(interval)
        chosen = self._spec.poll_mode if mode is None else _require_enum(mode, PollMode, "mode")
        return self._core.rest_transport().poll(
            self._spec, self._request, interval=period, mode=chosen
        )


class StreamQuery(QueryBase):
    """A WebSocket dataset: four live shapes over one shared stream.

    Every verb of an equal query reuses one stream (one subscription, one publisher); each call
    returns a new, distinct table held in its own liveness scope. The stream stays subscribed
    while any result, or any table derived from one, is alive.

    No namespace declares a dataset of this kind yet, so no query can be built and there is no
    example here yet.
    """

    __slots__ = ()

    @property
    def _channel(self) -> str | None:
        """The WebSocket channel: per timespan for bars, otherwise the dataset's channel."""
        return self._spec.channels.get(self._request.timespan)

    def _stream(self, build: Callable[[Table], Table], *, wrap: bool = False) -> Table:
        """Shape a live result from this query's stream, creating the stream if needed.

        Args:
            build: Turns the stream's blink table into the result.
            wrap: True when `build` returns a new wrapper of the blink table itself.

        Returns:
            The tracked live result.
        """
        return self._core.stream_result(
            self._spec, self._request, channel=self._channel, build=build, wrap=wrap
        )

    def blink(self) -> Table:
        """Return the stream as a blink table: each update cycle holds only that cycle's rows.

        Each call returns a new wrapper with its own liveness scope, so two `blink()` results
        are released independently.

        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        Returns:
            A live blink table in the dataset's schema.
        """
        self._core.require_verb_thread()
        return self._stream(lambda blink: Table(j_table=blink.j_table), wrap=True)

    def append_only(self) -> Table:
        """Return every row received since the call, in arrival order.

        Memory grows with the stream; use `ring()` to bound it.

        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        Returns:
            A live append-only table in the dataset's schema.
        """
        self._core.require_verb_thread()
        return self._stream(blink_to_append_only)

    def ring(self, capacity: int) -> Table:
        """Return the most recent `capacity` rows received.

        A `capacity` that is not an `int` raises `TypeError`, and one outside 1 to 2**31 - 1
        raises `ValueError` (`require_positive_int`).

        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        Args:
            capacity: How many rows to keep, from 1 to 2**31 - 1.

        Returns:
            A live ring table in the dataset's schema.
        """
        self._core.require_verb_thread()
        rows = require_positive_int("capacity", capacity, maximum=MAX_RING_CAPACITY)
        return self._stream(lambda blink: ring_table(blink, rows))

    def last_by(self, by: str | Sequence[str] | None = None) -> Table:
        """Return the latest row per key, by default one row per `Symbol`.

        The underlying state keeps every key ever seen. With a reactive symbol table and
        `Symbol` among the keys, rows of symbols removed from it are filtered out; a symbol
        removed and re-added shows its last row until it ticks again. With keys that span
        symbols (`by="Exchange"`), rows are filtered before the aggregation, so each group's
        row is the latest from a symbol that was watched when it arrived; a group keeps that
        row after the symbol is removed, until a watched symbol ticks in the group. The table
        may hold any accepted spelling (`EUR/USD` for `C:EURUSD`): it is matched through
        `_symbols.canonical_view`. `last_by()` never seeds
        from REST: use `snapshots(symbols).poll(n)` for current state on every plan. A `by`
        that is not a `str` or a sequence of `str` raises `TypeError`; an empty `by` or an
        unknown column raises `ValueError`.

        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        Args:
            by: The key column or columns, positionally or by keyword as in Deephaven's
                `Table.last_by` (`q.last_by("Exchange")`); None means `"Symbol"`. Each column
                may appear once.

        Returns:
            A live table with one row per key, in the dataset's schema.
        """
        self._core.require_verb_thread()
        keys = self._last_by_keys(by)
        symbols = self._request.symbols
        if not isinstance(symbols, SymbolTable):
            return self._stream(lambda blink: blink.last_by(list(keys)))
        watch = symbols.table
        asset_class = self._spec.asset_class
        if "Symbol" in keys:
            # One row per symbol: filtering after the aggregation evicts a removed symbol's rows.
            return self._stream(
                lambda blink: blink.last_by(list(keys)).where_in(
                    canonical_view(asset_class, watch), "Symbol"
                )
            )
        # Groups span symbols: filter first, so a group's row is the latest of a watched symbol.
        return self._stream(
            lambda blink: blink.where_in(canonical_view(asset_class, watch), "Symbol").last_by(
                list(keys)
            )
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
        columns = self._spec.schema.column_names
        unknown = [key for key in keys if key not in columns]
        if unknown:
            raise ValueError(
                f"{self._spec.name} has no columns {unknown}; columns: {list(columns)}"
            )
        return tuple(str(key) for key in keys)


class HistoryQuery(QueryBase):
    """A dataset with a historical retrieval path: `history(start, end)`.

    No namespace declares a dataset of this kind yet, so no query can be built and there is no
    example here yet.
    """

    __slots__ = ()

    def history(
        self, start: DateLike, end: DateLike, *, source: HistorySource = HistorySource.AUTO
    ) -> Table:
        """Return the rows in `[start, end)` as a static table.

        A date `start` is that day's midnight and a date `end` the next day's midnight in the
        asset class's day zone (New York for stocks, options, and indices; UTC for forex and
        crypto; sessions for futures), so whole days are inclusive. There is no `limit`: the
        library pages internally.

        Flat files (S3) can serve a request when S3 credentials are configured, the dataset
        has files for it (bars: only 1-minute and 1-day bars, and 1-session bars for futures,
        unadjusted where the dataset has `adjusted`), and every day's file is published (from
        `flat_file_publish_cutoff`, New York time, after the day ends). REST can serve it when
        the dataset has a REST history endpoint and the symbols are explicit, or for `dm.ALL`
        daily bars with a grouped-daily endpoint. `AUTO` reads flat files when only they can,
        or when both can and the request has `flat_file_symbol_threshold` symbols or more (or
        `dm.ALL`); otherwise it uses REST. A reactive symbol table is read once, when the verb
        is called, and every source receives the same sorted canonical symbols; rows that are
        not valid symbols of the class are ignored with a WARNING. Flat-file reads skip a
        missing weekend file silently and a missing weekday file (a holiday) with a WARNING.

        A `source` that is not a `HistorySource`, or a bound of the wrong type, raises
        `TypeError`. A naive or unparseable bound, an empty range, a reactive symbol table
        without a valid symbol, a source that cannot serve
        the request (`REST` without REST history or with `dm.ALL` and no grouped-daily
        endpoint; `FLAT_FILES` when a flat-file condition above fails; `AUTO` when neither
        can, with every violated condition listed) raises `ValueError`, synchronously. A failed
        request raises `MassiveError` (`EntitlementError` on HTTP 403, which for futures flat
        files names the venue and the `venue=` parameter), as does a missing flat file for the
        last requested day ("not yet published").

        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        Args:
            start: The first day or instant, as an ISO string, `date`, or aware `datetime`.
            end: The last day, or the first instant excluded.
            source: Where to read: `AUTO` picks flat files or REST, `REST` or `FLAT_FILES`
                forces one.

        Returns:
            A static table in the dataset's schema.
        """
        self._core.require_verb_thread()
        chosen = _require_enum(source, HistorySource, "source")
        first, last = parse_date_like(start, "start"), parse_date_like(end, "end")
        window = bounds(self._spec, first, last)
        symbols = self._request.symbols
        if isinstance(symbols, SymbolTable):
            symbols = _read_symbol_table(self._core, self._spec, symbols)
        request = replace(self._request, start=first, end=last, symbols=symbols)
        if chosen is HistorySource.REST:
            self._check_rest_history()
            return self._core.rest_transport().history(self._spec, request, window)
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
        if choice.source is HistorySource.REST:
            return self._core.rest_transport().history(self._spec, request, window)
        return self._core.flat_file_transport().history(self._spec, request, window, dates=dates)

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

    No namespace declares a dataset of this kind yet, so no query can be built and there is no
    example here yet.
    """

    __slots__ = ()


class StreamHistoryLatestQuery(StreamHistoryQuery):
    """A stream-and-history dataset with a REST "last" endpoint, served by `fetch()`.

    No namespace declares a dataset of this kind yet, so no query can be built and there is no
    example here yet.
    """

    __slots__ = ()

    def fetch(self) -> Table:
        """Return exactly one row per symbol, the latest, from the dataset's "last" endpoint.

        One request per symbol, synchronously on the calling thread. The "last" endpoints are
        absent from some plans; `snapshots(symbols).fetch()` is the alternative that works on
        every plan. A failed request raises `MassiveError` (`EntitlementError` on HTTP 403).
        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        Symbols of `dm.ALL` raise `ValueError` naming `snapshots(dm.ALL)`.

        Returns:
            A static table in the stream's schema.
        """
        self._core.require_verb_thread()
        _require_explicit_symbols(self._spec, self._request)
        return self._core.rest_transport().fetch(self._spec, self._request)


class HistoryLatestQuery(HistoryQuery):
    """Daily bars: `history()` plus `fetch()` of the previous completed session's bar.

    No namespace declares a dataset of this kind yet, so no query can be built and there is no
    example here yet.
    """

    __slots__ = ()

    def fetch(self) -> Table:
        """Return one row per symbol: the bar of the previous completed session.

        Massive documents the endpoint as "the prior session's open, high, low, close, and
        volume". A failed request raises `MassiveError` (`EntitlementError` on HTTP 403).
        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        The endpoint takes one ticker per request, so symbols of `dm.ALL` raise `ValueError`
        naming `snapshots(dm.ALL)`; `history()` serves every symbol's daily bars.

        Returns:
            A static table in the bars schema.
        """
        self._core.require_verb_thread()
        _require_explicit_symbols(self._spec, self._request)
        return self._core.rest_transport().fetch(self._spec, self._request)
