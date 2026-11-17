"""`StreamQuery` and `NoSymbolStreamQuery`: the queries of Massive's WebSocket streams.

A stream dataset sends rows as they happen. Its query turns that one stream into the table
shape the caller needs: `blink()` for the rows of the latest update cycle, `append_only()` for
every row since the call, `ring(capacity)` for the latest rows, and, where the rows name their
symbol, `last_by()` for each key's latest row. Identical queries share one subscription
(`SharedStreams` in `_tables/shared_streams.py`), and a live table of symbols is followed as a
watchlist: the stream subscribes and unsubscribes as its rows are added and deleted.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final

from deephaven.stream import blink_to_append_only
from deephaven.table_factory import ring_table

from deephaven_massive._config import require_positive_int
from deephaven_massive._datasets.catalog import timespan_key
from deephaven_massive._datasets.symbols import ALL, SymbolTable
from deephaven_massive._errors import raises_through
from deephaven_massive._tables.columns import (
    describe,
    recency_note,
)
from deephaven_massive._tables.execution_context import refuse_update_graph_thread
from deephaven_massive._tables.session_core import SessionCore
from deephaven_massive._tables.shared_streams import SharedStreams
from deephaven_massive._tables.transports import StreamTransport
from deephaven_massive._tables.watchlist import (
    SymbolRowRule,
    latest_rows_since_added,
    read_symbols,
    watched_rows,
)
from deephaven_massive.queries._query import (
    Query,
    _read_symbol_rows,
    _read_symbol_table,
    _verb,
    table_problems,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.table import Table

    from deephaven_massive._datasets.request import Request
    from deephaven_massive._tables.transports import StreamHandle

__all__ = ["MAX_RING_CAPACITY", "NoSymbolStreamQuery", "StreamQuery"]

logger = logging.getLogger(__name__)

MAX_RING_CAPACITY: Final = 2**31 - 1
"""The largest `ring()` capacity Deephaven accepts (a Java `int`)."""


@dataclass(frozen=True, slots=True)
class _StreamRequest:
    """The request a stream opens for, and what the read of a static symbol table skipped.

    Attributes:
        request: The request; a static table's symbols are already read into a tuple.
        static: The static symbol table read at the call, or None.
        skipped: The values that read skipped.
    """

    request: Request
    static: SymbolTable | None = None
    skipped: tuple[str, ...] = ()


class _StreamShapes(Query):
    """The three table shapes of a live WebSocket stream: `blink()`, `append_only()`, `ring()`.

    Both public stream queries derive from it: `StreamQuery`, whose rows name their symbol and
    which adds `last_by()`, and `NoSymbolStreamQuery`, whose rows don't. Its methods are public
    members of both. The class itself stays private, since no dataset method returns it; code
    that takes either query annotates `StreamQuery | NoSymbolStreamQuery`.
    """

    __slots__ = ()

    @property
    def _channel(self) -> str:
        """The WebSocket channel: per timespan for bars, otherwise the dataset's channel.

        A bars query has this class only for bars with a channel (`bar_query_class`), so the
        lookup always finds one.
        """
        return self._spec.channels[timespan_key(self._request.timespan)]

    def _stream_request(self) -> _StreamRequest:
        """Return the request the stream is opened for, with a static symbol table read once.

        The request of a stream whose rows name no symbol (`DatasetSpec.names_symbols` False),
        which has none, gets `ALL`: the stream subscribes its channel's every topic. A static
        table cannot be listened to, so it is read now, like `fetch()` reads one
        (`_read_symbol_table`), and becomes the sorted tuple of its symbols; a refreshing table
        stays reactive. A static table with no symbol, with only rows that are not canonical,
        or with any such row under `noncanonical_symbols=RAISE`, raises the `ValueError` of
        `_read_symbol_table`; under `LOG` and `IGNORE` such a row is skipped, and listed in
        `m.problems()` as a problem of the stream once it has one (`_stream`).

        Returns:
            The request, and what the read of a static table skipped.
        """
        request = self._request
        symbols = request.symbols
        if not self._spec.names_symbols:
            # A stream whose rows name no symbol subscribes its channel's every topic, so its
            # stream is keyed as dm.ALL's would be.
            return _StreamRequest(replace(request, symbols=ALL))
        if isinstance(symbols, SymbolTable) and not symbols.table.is_refreshing:
            rows = _read_symbol_rows(
                self._core,
                self._spec,
                symbols,
                query=repr(self),
                noncanonical=self._request.on_error.noncanonical_symbols,
                listed=True,
            )
            return _StreamRequest(
                replace(request, symbols=rows.symbols), static=symbols, skipped=rows.skipped
            )
        return _StreamRequest(request)

    def _stream(
        self, shape: Callable[[Table], Table], opened: _StreamRequest, *, verb: str
    ) -> Table:
        """Shape a live result from this query's stream, creating the stream if needed.

        The result is the shape, described last (`describe`): an append-only or ring table
        copies no attributes, so describing the blink table first would lose them. The schema
        is read before the stream is acquired, so the result is built under no library lock.
        A reactive symbol table is read first, so a row that is not Massive's exact ticker
        fails the call before any stream starts. A crypto pair takes its split from Massive's
        crypto ticker list, read once per session, after the stream opens, wherever the pair
        came from (`StreamTransport.open_stream`).

        Args:
            shape: Turns the stream's blink table into the result's shape; the identity for
                `blink()`, whose described copy is then a new table of its own.
            opened: The request from `_stream_request`, with what a static table's read
                skipped: each skipped or refused value is listed in `m.problems()` as a problem
                of the stream, once, since the table never changes (`table_problems`).
            verb: The verb's name, such as `"last_by"`, for the log line of a stream's open.

        Returns:
            The tracked live result.

        A symbol table whose rows are all bad, or a bad row under `RAISE`, raises the
        `ValueError` of `_read_symbol_table`.
        """
        spec = self._spec
        request = opened.request
        symbols = request.symbols  # a static table is already read: never read it twice
        if isinstance(symbols, SymbolTable):
            _read_symbol_table(
                self._core,
                spec,
                symbols,
                query=repr(self),
                noncanonical=self._request.on_error.noncanonical_symbols,
                live=True,
            )
        schema = self._core.dataset_schema(spec)
        streams: list[int] = []  # the stream the result reads, which the build learns

        def build(handle: StreamHandle) -> Table:
            streams.append(handle.stream_id)
            return describe(
                shape(handle.blink),
                spec,
                schema,
                note=recency_note(handle.recency, host=handle.host),
            )

        result = self._core.streams.result(
            spec, request, channel=self._channel, query=repr(self), verb=verb, build=build
        )
        if opened.static is not None:
            rule = SymbolRowRule.of(spec, self._request.on_error.noncanonical_symbols)
            [stream_id] = streams
            problems = table_problems(
                spec,
                opened.static,
                rule,
                skipped=opened.skipped,
                stream_id=stream_id,
                query=repr(self),
            )
            if problems:
                self._core.problems(problems)
        return result

    @_verb
    @raises_through(
        SessionCore.run_verb,
        refuse_update_graph_thread,
        _read_symbol_table,
        read_symbols,
        SharedStreams.result,
        StreamTransport.open_stream,
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
                - option contracts passed as a list or a static table pass the connection's limit.
            AuthenticationError: If Massive rejects your API key while the call checks which feed
                your plan streams. A later rejection fails the table instead.
            EntitlementError: If Massive's data for this dataset is delayed and its subscription
                isn't in `MassiveConfig.delayed_subscriptions`. The message names the setting to
                change.
            SessionClosedError: If the session is closed.
            MassiveError: If the stream fails while it opens.
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
                - option contracts passed as a list or a static table pass the connection's limit.
            AuthenticationError: If Massive rejects your API key while the call checks which feed
                your plan streams. A later rejection fails the table instead.
            EntitlementError: If Massive's data for this dataset is delayed and its subscription
                isn't in `MassiveConfig.delayed_subscriptions`. The message names the setting to
                change.
            SessionClosedError: If the session is closed.
            MassiveError: If the stream fails while it opens.
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
                - option contracts passed as a list or a static table pass the connection's limit.
            AuthenticationError: If Massive rejects your API key while the call checks which feed
                your plan streams. A later rejection fails the table instead.
            EntitlementError: If Massive's data for this dataset is delayed and its subscription
                isn't in `MassiveConfig.delayed_subscriptions`. The message names the setting to
                change.
            SessionClosedError: If the session is closed.
            MassiveError: If the stream fails while it opens.
        """
        rows = require_positive_int("capacity", capacity, maximum=MAX_RING_CAPACITY)
        return self._stream(
            lambda blink: ring_table(blink, rows), self._stream_request(), verb="ring"
        )


class NoSymbolStreamQuery(_StreamShapes):
    """A query of a live stream whose rows name no symbol: `blink()`, `append_only()`, `ring()`.

    Massive sends a few streams per channel rather than per symbol, such as
    `m.options.admin_messages()`, whose rows are notices from the options exchanges. Their rows
    have no `Symbol` column, so their query has no `last_by()`. Each of its three methods returns
    the stream as a different kind of live Deephaven table: `blink()` holds only the latest
    update cycle's rows, `append_only()` keeps every row since the call, and `ring(capacity)`
    keeps the latest `capacity` rows.

    Identical queries share one stream, and each call returns its own table. The stream stays
    subscribed while any of its tables, or a table made from one, is in use. A method returns
    once its table exists, without waiting for Massive to confirm the subscription. A later
    failure, such as a refused subscription or a rejected key, fails the table and shows in
    `m.streams()`, and the next call on an identical query opens a new stream.

    Import it as `from deephaven_massive.queries import NoSymbolStreamQuery` to annotate your own
    code. You get one from a dataset method and never construct one.

    Example:
        ::

            notices = m.options.admin_messages().append_only()
    """

    __slots__ = ()


class StreamQuery(_StreamShapes):
    """A query of a live WebSocket stream: `blink()`, `append_only()`, `ring()`, and `last_by()`.

    Massive streams trades, quotes, bars, and other events over its WebSocket as they happen.
    A stream's dataset method returns a `StreamQuery`, and each of its methods returns the
    same stream as a different kind of live Deephaven table: `blink()` holds only the latest
    update cycle's rows, `append_only()` keeps every row since the call, `ring(capacity)` keeps
    the latest `capacity` rows, and `last_by()` keeps each symbol's latest row, a live table of
    the current state of every symbol you follow. A stream whose rows name no symbol has no
    `last_by()`, so its query is a `NoSymbolStreamQuery` instead.

    Identical queries share one stream, and each call returns its own table. The stream stays
    subscribed while any of its tables, or a table made from one, is in use. A method returns
    once its table exists, without waiting for Massive to confirm the subscription. A later
    failure, such as a refused subscription or a rejected key, fails the table and shows in
    `m.streams()`, and the next call on an identical query opens a new stream.

    The symbols can be a list, a static table, which is read once at the call, or a live table,
    which works as a watchlist: the stream subscribes and unsubscribes as you add and delete
    rows. A watchlist row that isn't Massive's exact ticker is skipped and listed in
    `m.streams()`.
    Massive allows 1,000 option contracts on one connection, and the library counts a
    contract's trades and quotes as two. More contracts than that, passed as a list or a static
    table, raise `ValueError`; a watchlist's extra contracts wait for room, and `Deferred` in
    `m.streams()` counts them.

    Import it as `from deephaven_massive.queries import StreamQuery` to annotate your own code.
    You get one from a dataset method and never construct one.

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
                - option contracts passed as a list or a static table pass the connection's limit.
            AuthenticationError: If Massive rejects your API key while the call checks which feed
                your plan streams. A later rejection fails the table instead.
            EntitlementError: If Massive's data for this dataset is delayed and its subscription
                isn't in `MassiveConfig.delayed_subscriptions`. The message names the setting to
                change.
            SessionClosedError: If the session is closed.
            MassiveError: If the stream fails while it opens.
        """
        keys = self._last_by_keys(by)
        request = self._stream_request()
        symbols = request.request.symbols
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
