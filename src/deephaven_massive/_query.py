"""The six query classes and their verbs.

A dataset method such as `m.stocks.trades(["AAPL"])` returns a *query*: an immutable,
hashable description of what to fetch. Nothing touches the network until a *verb* is called,
and each verb returns a Deephaven `Table` of a particular shape:

| Class                      | Verbs                                                     |
|----------------------------|-----------------------------------------------------------|
| `Query`                    | `fetch()`, `poll(interval)`                               |
| `StreamQuery`              | `blink()`, `append_only()`, `ring(capacity)`, `last_by()` |
| `HistoryQuery`             | `history(start, end=None)`                                |
| `StreamHistoryQuery`       | the stream verbs and `history()`                          |
| `StreamHistoryLatestQuery` | the above and `fetch()` (the "last" endpoint)             |
| `HistoryLatestQuery`       | `history()` and `fetch()` (the previous session's bar)    |

A verb that cannot work for a dataset does not exist on its query class, so tab completion is the
contract. Every query also has `columns()`, which is not a verb: it lists the dataset's columns,
their types, and what each one means, as a static table built without any network call.
Verbs validate their parameters synchronously on the calling thread: a wrong type raises
`TypeError` and an invalid value raises `ValueError`. Every verb also raises `ValueError` on a
Deephaven update-graph thread (for example inside a table listener), because verbs create tables and
take the session's registry lock.

The verbs delegate the transport work to three interfaces, one per transport, defined in
`_tables/core.py` and provided by the session core's factory hooks: `RestTransport` (`fetch`,
`poll`, REST `history`), `StreamTransport` (opening a WebSocket stream), and
`FlatFileTransport` (flat-file `history`).
`history()` chooses its source itself (`choose_source` in `_datasets/history_source.py`) and
then calls one transport, so the transports only do I/O.
Live stream results are shaped and tracked by the session (`SessionCore.stream_result`), so
every stream verb shares one stream per (dataset, channel, symbol set).

The classes are importable from this private module for type annotations only; users never
construct them.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import timedelta
from typing import TYPE_CHECKING, Final

from deephaven import arrow as dh_arrow
from deephaven.stream import blink_to_append_only
from deephaven.table_factory import ring_table

from deephaven_massive._config import require_positive_int, require_positive_number
from deephaven_massive._datasets.dates import parse_date_like
from deephaven_massive._datasets.history_source import (
    choose_source,
    history_window,
    partition_dates,
    rest_history_problem,
)
from deephaven_massive._datasets.symbols import (
    SymbolTable,
    symbol_canonicalizer,
    symbol_class,
)
from deephaven_massive._enums import (
    HistorySource,
    PollMode,
    _require_enum,
)
from deephaven_massive._tables.columns import column_table, describe
from deephaven_massive._tables.core import ON_UPDATE_GRAPH, on_update_graph
from deephaven_massive._tables.liveness import entered
from deephaven_massive._tables.watchlist import canonical_view
from deephaven_massive._types import ALL, override

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._tables.core import SessionCore
    from deephaven_massive._types import DateLike

__all__ = [
    "HistoryLatestQuery",
    "HistoryQuery",
    "Query",
    "QueryBase",
    "StreamHistoryLatestQuery",
    "StreamHistoryQuery",
    "StreamQuery",
]

logger = logging.getLogger(__name__)

MAX_RING_CAPACITY: Final = 2**31 - 1
"""The largest `ring()` capacity Deephaven accepts (a Java `int`)."""


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
    """Read a reactive symbol table once, for a static result (`fetch()` or `history()`).

    The table is read with `Table.iter_tuple`, which holds the update graph's shared lock while
    it reads, so the symbols are one consistent snapshot. Runs on the caller thread, inside the
    session's execution context, with no library lock held. Each row is canonicalized as the
    stream verbs do (`symbol_canonicalizer` in `_datasets/symbols.py`): a row that is not a
    valid symbol of the class is ignored and logged once at WARNING.

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
        raise ValueError(f"{spec.name}: the symbol table holds no valid symbols")
    return found


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

        Raises:
            ValueError: If called on an update-graph thread, for example from a table listener.

        Example:
            ::

                cols = m.stocks.trades(["AAPL"]).columns()
                stream_only = cols.where("SuppliedBy = `WebSocket`")
        """
        if on_update_graph(self._core.exec_ctx):
            raise ValueError(ON_UPDATE_GRAPH)
        schema = self._core.catalog.dataset_schema(self._spec)
        with entered(self._core.exec_ctx):
            return column_table(schema)

    @override
    def __repr__(self) -> str:
        """Show the query class, the dataset, and the non-default arguments.

        Returns:
            For example `<StreamHistoryLatestQuery stocks.trades symbols=('AAPL', 'MSFT')>`.
        """
        described = self._request.describe()
        suffix = f" {described}" if described else ""
        return f"<{type(self).__name__} {self._spec.name}{suffix}>"

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
        schema = self._core.catalog.dataset_schema(spec)
        with entered(self._core.exec_ctx):
            return describe(table, spec, schema)

    def _static_request(self) -> Request:
        """Return the request with a reactive symbol table read once, for a static verb.

        Returns:
            The request; for a reactive table, with its valid symbols (`_read_symbol_table`).
        """
        request = self._request
        if isinstance(request.symbols, SymbolTable):
            symbols = _read_symbol_table(self._core, self._spec, request.symbols)
            return replace(request, symbols=symbols)
        return request


class Query(QueryBase):
    """A REST list or snapshot dataset: a static `fetch()` or a live `poll()`.

    No namespace declares a dataset of this kind yet, so no query can be built and there is no
    example here yet.
    """

    __slots__ = ()

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
        reactive symbol table holds no valid symbol. A failed request raises
        `AuthenticationError` on HTTP 401, `EntitlementError` on HTTP 403, and `MassiveError`
        for a rate limit that outlived the retries, a row that breaks the dataset's schema, or
        any other failure.

        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        Returns:
            A static table.
        """
        self._core.require_verb_thread()
        return self._described(
            self._core.rest_transport().fetch(self._spec, self._static_request())
        )

    def poll(self, interval: float | timedelta, *, mode: PollMode | None = None) -> Table:
        """Return a live table that re-runs `fetch()` every `interval`.

        A reactive `Table` passed as `symbols` is re-read once per cycle on the poller thread.
        Fan-out endpoints issue one request per symbol per cycle, at most `max_concurrency` at a
        time. Releasing the table (`m.release`, garbage collection) stops its poller. A
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
                `APPEND` for news and filings (not the filing taxonomies), `UPSERT` for IPOs,
                and `REPLACE` otherwise.

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

    No namespace declares a dataset of this kind yet, so no query can be built and there is no
    example here yet.
    """

    __slots__ = ()

    @property
    def _channel(self) -> str:
        """The WebSocket channel: per timespan for bars, otherwise the dataset's channel.

        A stream query exists only for a timespan with a channel (`bar_query_class`), so the
        lookup always finds one.
        """
        return self._spec.channels[self._request.timespan]

    def _stream(self, shape: Callable[[Table], Table]) -> Table:
        """Shape a live result from this query's stream, creating the stream if needed.

        The result is the shape, described last (`describe`): an append-only or ring table
        copies no attributes, so describing the blink table first would lose them. The schema
        is read before the stream is acquired, so the result is built under no library lock.

        Args:
            shape: Turns the stream's blink table into the result's shape; the identity for
                `blink()`, whose described copy is then a new table of its own.

        Returns:
            The tracked live result.
        """
        spec = self._spec
        schema = self._core.catalog.dataset_schema(spec)
        return self._core.stream_result(
            spec,
            self._request,
            channel=self._channel,
            build=lambda blink: describe(shape(blink), spec, schema),
        )

    def blink(self) -> Table:
        """Return the stream as a blink table: each update cycle holds only that cycle's rows.

        Each call returns a new table, a described copy of the stream's blink table, with its
        own liveness scope, so two `blink()` results are released independently.

        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        Returns:
            A live blink table in the dataset's schema.
        """
        self._core.require_verb_thread()
        return self._stream(lambda blink: blink)

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
        `canonical_view` in `_tables/watchlist.py`. `last_by()` never seeds
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
        columns = self._core.catalog.dataset_schema(self._spec).column_names
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

    def history(
        self,
        start: DateLike,
        end: DateLike | None = None,
        *,
        source: HistorySource = HistorySource.AUTO,
    ) -> Table:
        """Return the dataset's rows over a range of whole days or of exact instants.

        The two bounds are of one kind, never mixed:

        - **Dates** (`"2025-03-03"` or a `date`) are whole days, inclusive at both ends:
          `history("2025-03-03", "2025-03-03")` is all of March 3. A day is a calendar day in
          the asset class's day zone: New York for stocks, options, and indices, UTC for forex
          and crypto. For futures a date names the trading session whose `Date`
          (`session_end_date`) it is, which opens at 17:00 Chicago time the evening before
          (earlier after a holiday).
        - **Aware datetimes** are exact instants, half-open: rows with `start <= Timestamp <
          end` (for bars, `TimestampStart`). Equal instants give an empty table.

        `end=None`, the default, reads through now. A bar is returned only when its whole
        window lies inside the range and has ended: an intraday or 1-day bar its own interval;
        for stocks, options, indices, and futures, a week Monday to Friday and a month,
        quarter, or year its calendar period less its leading and trailing weekend days; for
        forex and crypto, which trade at weekends, whole calendar weeks (Monday to Sunday),
        months, quarters, and years; a bar of several units all of them; and a futures bar
        named by session dates its sessions. So the current day's, week's, or month's bar is
        never returned, and `history("2025-03-05", "2025-03-20")` of monthly bars is empty.
        Datetime bounds keep only whole calendar days of 1-day bars: Monday 09:30 to Friday
        16:00 New York time returns Tuesday to Thursday, so pass dates for daily bars. A
        futures bar of several sessions, weeks, or months named by dates is kept only when it
        fits the range whether Massive labels it with its first or its last session, which is
        not documented. There is no holiday calendar: a week whose Friday is a holiday is still
        that Monday-to-Friday week. There is no `limit`: the library pages internally.

        Every row says when it happened, its trading day, and when it arrived: a tick its
        `Timestamp`; a bar its window, `TimestampStart` and the exclusive `TimestampEnd`
        (join on the end, `aj(..., on=["Symbol", "Timestamp>=TimestampEnd"])`, so no bar is
        attached before it closed); its `Date`, ISO `yyyy-MM-dd` text, null for a bar that
        spans more than one date; and `TimestampInsert`, when its page or file block arrived
        (the download time). A bar of a day or longer starts at 00:00 of its window's first
        day in the class day zone (New York for stocks, options, and indices; UTC for forex
        and crypto), whatever the source, and ends at 00:00 after its last day (an hour later
        for indices, whose day is not verified); a futures bar keeps Massive's window start and
        ends at 17:00 Chicago time on its session date.

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
        called, and every source receives the same sorted canonical symbols; rows that are not
        valid symbols of the class are ignored with a WARNING. Flat-file reads skip a missing
        weekend file silently and a missing weekday file (a holiday) with a WARNING. Futures
        trades and quotes named by session dates are read over REST one request series per
        weekday session and symbol; a long one-request-per-day download logs its request count
        first, at INFO.

        Side effects: network requests on the calling thread, and for flat files a pool of up
        to `max_concurrency` reader threads that end before the call returns. It blocks until
        the whole range is read.

        Args:
            start: The first day, or the first instant included: an ISO-8601 string, a `date`,
                or a timezone-aware `datetime`.
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

        Errors. `TypeError` if `source` is not a `HistorySource` or a bound is not a `str`,
        `date`, or `datetime`. `ValueError`, synchronously, before any request: a naive or
        unparseable bound; a date mixed with a datetime; `start` after `end` (or after now when
        `end` is None); a reactive symbol table without a valid symbol; futures trades or quotes
        named by session dates on a query that also filters `session_end_date` in any form; a
        source that cannot serve the request (`REST` without REST history or with `dm.ALL` and
        no grouped-daily endpoint; `FLAT_FILES` when a flat-file condition above fails; `AUTO`
        when neither can, with every violated condition listed); or a call on a Deephaven
        update-graph thread. `MassiveError` when a request fails (`EntitlementError` on HTTP
        403, which for futures flat files names the venue and the `venue=` parameter;
        `AuthenticationError` when S3 rejects the flat-file credentials); when a page of
        adjusted bars echoes another basis than the request; when a symbol's later page answers
        HTTP 404, its data cut short (a symbol with no data at all simply returns no rows); when
        the flat file of the last requested day is missing within a day after its publication
        time ("... is not yet published (flat files land ~11:00 ET next day), or was a market
        holiday"; after that the day is skipped with a WARNING, as a holiday); when a futures
        range named by instants ends on a holiday and the next session's file, which holds the
        holiday's hours, is not published yet or does not exist within 7 days; when a bar of a
        day or longer is stamped at a time of day, or on a day, Massive is not known to use for
        its class (a crypto 1-day bar at midnight New York time, or a crypto week bar on a
        Sunday), naming the dataset, the source, and what to do instead; or once the session is
        closed.

        Example:
            ::

                q = m.crypto.trades("BTC-USD")
                day = q.history("2026-01-02", "2026-01-02")  # all of that UTC day
                hour = q.history(
                    datetime(2026, 1, 2, 14, tzinfo=UTC), datetime(2026, 1, 2, 15, tzinfo=UTC)
                )
                since = q.history("2026-01-02")  # through now
        """
        self._core.require_verb_thread()
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
            return self._described(self._core.rest_transport().history(self._spec, request, window))
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
            return self._described(self._core.rest_transport().history(self._spec, request, window))
        return self._described(
            self._core.flat_file_transport().history(self._spec, request, window, dates=dates)
        )

    def _empty(self) -> Table:
        """Return the empty result of a range no row can lie in, without any request.

        Returns:
            A static table with every column of the dataset and no row, described as a
            result is (`_described`).
        """
        schema = self._core.catalog.dataset_schema(self._spec)
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

        The requests run on the calling thread, or on up to `max_concurrency` threads: one per
        symbol for a per-symbol endpoint (`/v2/last/trade`), or one per batch of symbols for a
        snapshot endpoint (the indices snapshot). A reactive symbol table is read once, when
        the verb is called. The "last" endpoints are absent from some plans;
        `snapshots(symbols).fetch()` is the alternative that works on every plan. A failed
        request raises `MassiveError` (`EntitlementError` on HTTP 403).
        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        Symbols of `dm.ALL` raise `ValueError` naming `snapshots(dm.ALL)`.

        Returns:
            A static table in the stream's schema.
        """
        self._core.require_verb_thread()
        _require_explicit_symbols(self._spec, self._request)
        return self._described(
            self._core.rest_transport().fetch(self._spec, self._static_request())
        )


class HistoryLatestQuery(HistoryQuery):
    """Daily bars: `history()` plus `fetch()` of the previous completed session's bar.

    The `bars()` of stocks, options, indices, forex, and crypto return this class for 1-day
    bars with `multiplier=1`. A query is built by the namespace method, holds no state of its
    own beyond its arguments, and may be used from any thread but a Deephaven update-graph
    thread.

    Example:
        ::

            daily = m.stocks.bars("AAPL", dm.Timespan.DAY)
            last = daily.fetch()  # the previous session's bar
            month = daily.history("2025-03-03", "2025-03-28")
    """

    __slots__ = ()

    def fetch(self) -> Table:
        """Return one row per symbol: the bar of the previous completed session.

        Massive documents the endpoint as "the prior session's open, high, low, close, and
        volume". The bar's `TimestampStart` is 00:00 of its day in the class day zone (New
        York for stocks, options, and indices; UTC for forex and crypto), whatever time Massive
        stamps it at, its `TimestampEnd` 00:00 of the next day (01:00 New York time for
        indices), and its `Date` that day. A stamp at a time of day Massive is not known to use
        for the class raises `MassiveError`, naming the dataset and `/prev`. Stock and option
        bars are split-adjusted unless the query says `adjusted=False`, and a page whose
        `adjusted` echo differs from the request raises `MassiveError`. A failed request raises
        `MassiveError` (`EntitlementError` on HTTP 403).
        Like every verb, it raises `ValueError` on a Deephaven update-graph thread and
        `MassiveError` once the session is closed.

        The endpoint takes one ticker per request, so symbols of `dm.ALL` raise `ValueError`
        naming `snapshots(dm.ALL)`; `history()` serves every symbol's daily bars.

        Returns:
            A static table in the bars schema.
        """
        self._core.require_verb_thread()
        _require_explicit_symbols(self._spec, self._request)
        return self._described(
            self._core.rest_transport().fetch(self._spec, self._static_request())
        )
