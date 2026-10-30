"""Live streams on the Deephaven side: publishers, the flush thread, and the shared-batch fan-out.

A WebSocket *stream* is one `(dataset, channel, symbols)` with its own publisher and blink table.
The WebSocket transport (`_ingest/ws.py`) owns the connections and decides which topics are
subscribed; this module owns everything that touches Deephaven for a stream:

- `ChannelPlan`, how one dataset reads one channel: the channel's generated columns, renamed
  for the dataset (`schema.rename`), their wire types, the dataset's merged schema, and the
  required keys and symbol column the wire client checks (`set_required`). Pure; built once
  per (dataset, channel).
- `open_stream_tables`, which creates a stream's liveness scope and, inside it, the publisher
  and its blink table (`blink_publisher`, whose failure is delivered exactly once) and, for a
  reactive symbol table, the `SymbolWatch` that follows it. It runs on the caller thread,
  inside the session's execution context.
- `StreamSink`, a stream as the flush thread sees it: its session, the publisher, the symbols it
  receives (`None` for `dm.ALL`), and its count of published rows.
- `Flusher`, one per connection: a daemon thread that every `flush_interval` drains the
  connection's row buffers (one `queue.SimpleQueue` per channel, fed by the connection's loop
  thread with each frame's rows and the instant it arrived) and its control queue (failures and
  drift), and publishes. A connection may serve the streams of several sessions that share it,
  so the flusher holds no session: each sink reports to its own.

**Fan-out.** Per channel and flush, the rows (keyed by long-form column, symbols as the wire spells
them) are converted once: one Arrow table built from the rows, raw symbols mapped to canonical ones
per distinct symbol, every epoch run through the timestamp routine as a vectorized mask, columns
widened to the dataset schema, and the library's time columns filled (`row_times.TimeRule`): `Date`,
a bar's end, and `TimestampInsert`, the instant each row's frame arrived. Futures messages carry no
session date, so a futures row's `Date` is the 17:00 Chicago rule's, corrected by the connection's
`FuturesSessionDates` (the WebSocket transport's schedule calendar), which never waits; a row its
schedules do not cover keeps the rule's date, and the streams that receive it show the miss in their
`m.streams()` row. A row whose timestamp fails the bound fails only the streams that would have
received it, and is dropped from the batch. The batch becomes one static Deephaven table
(`deephaven.arrow.to_table`); then each `dm.ALL` stream gets `publisher.add(batch)` and each symbol
stream `publisher.add(batch.where_in(filter, "Symbol"))`, where `filter` is a static one-column
table cached per stream and rebuilt only when the stream's symbols change. So the Python work is
O(rows) per channel and flush, independent of the number of streams, and the per-stream work is one
Java filter and one `add`. Each flush ends with one `SessionCore.update_streams` call per session,
carrying the published-row total of each of its streams.

**Failures.** The connection's loop thread never calls Java, so it hands every stream failure
(a refused topic, a rejected row, a rejected key) to the flush thread's control queue; the
flush thread fails the stream's table with `publish_failure` and records it with
`SessionCore.fail_stream`, once per stream. Drift goes through the same queue: the flush thread
logs it once per (dataset, transport, field) through each session's `DriftRegistry` and adds it
to each receiving stream's `SchemaDrift` (`SessionCore.record_stream_drift`). A reactive
stream's watchlist that fails queues the stream's failure on its session's reaper
(`StreamSink.fail_later`), since the listener reports it on an update-graph thread.

**Threads and locks.** The flush thread creates only static tables, enters the execution
context of the session that created its connection through its own wrapper (sessions share a
connection only when they share an update graph), and never holds `registry_lock` while it calls
Deephaven. Its one lock, the flush lock, serializes flushes (the thread's own and any a test
calls) and is never taken under another library lock; the routing table has its own leaf lock.
The flusher holds no session, and a routed sink holds its own only weakly. A flush thread that
dies on something it cannot carry on after fails every stream it routes before it ends.
"""

from __future__ import annotations

import functools
import logging
import queue
import threading
import time
import weakref
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final, Protocol, TypeVar, cast

import pyarrow as pa
import pyarrow.compute as pc
from deephaven import new_table
from deephaven.column import string_col

from deephaven_massive._datasets.row_times import TimeRule, time_column
from deephaven_massive._datasets.schema import (
    OPTION_ROOT,
    RAW_SYMBOL,
    key_column,
    rename,
    required_fields,
    widen,
)
from deephaven_massive._datasets.symbols import (
    SymbolTable,
    WireSymbols,
    option_roots,
    unknown_symbol_message,
)
from deephaven_massive._datasets.timestamps import masked_instants, mismatch_message
from deephaven_massive._enums import NotBars, UnknownSymbols
from deephaven_massive._errors import MassiveError
from deephaven_massive._massive import Transport, WireType
from deephaven_massive._tables.columns import col_defs, dataset_table
from deephaven_massive._tables.diagnostics import StreamUpdate
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import new_scope, release_scope
from deephaven_massive._tables.publisher import BlinkPublisher, blink_publisher
from deephaven_massive._tables.transports import StreamHandle
from deephaven_massive._tables.watchlist import SymbolWatch
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.schema import ColumnType, Schema
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._datasets.timestamps import TimeUnit
    from deephaven_massive._enums import AssetClass
    from deephaven_massive._massive import DriftReport
    from deephaven_massive._tables.session_core import SessionCore

__all__ = [
    "BACKLOG_WARNING_ROWS",
    "ChannelPlan",
    "Flusher",
    "FuturesSessionDates",
    "OpenedStream",
    "RowFailure",
    "StreamSink",
    "open_stream_tables",
]

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

BACKLOG_WARNING_ROWS: Final = 1_000_000
"""Rows a connection may hold waiting for its flush thread before a WARNING is logged.

Rows wait between two flushes (`MassiveConfig.flush_interval`), so a connection normally holds
only a few flush intervals' worth. A backlog this large means the flushes are falling behind the
wire: the update graph is stalled or too slow for the stream (a `dm.ALL` burst at the open, for
example). Nothing is dropped, so memory grows with the backlog; the WARNING says so, once each
time the backlog crosses this level, and again only after it has fallen below half of it.
"""

SYMBOL_CACHE_LIMIT: Final = 100_000
"""Wire spellings a flush thread remembers for one dataset on one channel before it starts over.

A flush thread reads each distinct wire symbol once and remembers its canonical symbol, so a
stream's steady traffic costs no symbol reading. A `dm.ALL` stream of options, crypto, or forex
meets new spellings every day (new contracts are listed, old ones expire), so the memory would
grow for as long as the connection lives. Past this many spellings the memory is emptied after
the flush and refilled from the next flushes' rows: some tens of megabytes at most, and
one extra reading of each spelling still trading.
"""

_SYMBOL: Final = "Symbol"

_WIRE_ARROW: Final[Mapping[WireType, pa.DataType]] = {
    WireType.LONG: pa.int64(),
    WireType.EPOCH: pa.int64(),
    WireType.DOUBLE: pa.float64(),
    WireType.BOOL: pa.bool_(),
    WireType.STRING: pa.string(),
    WireType.JSON_TEXT: pa.string(),
    WireType.DATE: pa.date32(),
    WireType.ISO_INSTANT: pa.int64(),
    WireType.LONG_ARRAY: pa.list_(pa.int64()),
    WireType.STRING_ARRAY: pa.list_(pa.string()),
}
"""The Arrow type of each wire type as the WebSocket client delivers its values: an epoch as its
raw integer count (converted by its unit afterwards), an ISO-8601 instant as integer
nanoseconds, a date as a `datetime.date`, and a list as a list of integers or of text."""


class FuturesSessionDates(Protocol):
    """The schedule calendar that corrects streamed futures rows' session dates.

    The WebSocket transport gives a futures connection's flusher one
    (`_ingest/futures_calendar.py`); `ChannelPlan.convert` hands it the rule's dates of every
    futures row. It never waits on the network: what its schedules do not cover yet keeps the
    rule's date.
    """

    def correct(
        self, symbols: pa.Array, instants: pa.Array, dates: pa.Array
    ) -> tuple[pa.Array, list[tuple[str, str]]]:
        """Return the rows' session dates, corrected by the schedules, and the new misses.

        Args:
            symbols: The rows' canonical symbols, `string`.
            instants: The rows' instants (a tick's time, a bar's start), epoch nanoseconds,
                `int64`.
            dates: The 17:00 Chicago rule's session dates, ISO text.

        Returns:
            The dates to keep, one per row; and each miss reported for the first time, as
            `(symbol, message)`: a row its schedules cannot place, which keeps the rule's date.
        """
        ...


@dataclass(frozen=True, slots=True)
class RowFailure:
    """A received row that cannot be published, and the symbol whose streams it fails.

    Attributes:
        symbol: The row's canonical symbol, or None when the row has none that is valid; then
            only the channel's `dm.ALL` streams would have received it.
        message: Why, naming the dataset, the column, the unit, and the value.
        every_stream: True for a row whose symbol is a spelling the library cannot read:
            since it cannot tell which symbol stream the row was meant for, it fails every
            stream of the dataset on the channel, so none starves silently.
    """

    symbol: str | None
    message: str
    every_stream: bool = False


@dataclass(frozen=True, slots=True)
class Converted:
    """One channel's rows of one flush, converted for one dataset by `ChannelPlan.convert`.

    Attributes:
        batch: The rows in the dataset's schema, without the rows that failed.
        failures: The rows that failed, in row order.
        symbols: The batch's distinct canonical symbols, so a stream that receives none of
            them, or all of them, needs no filter.
        null_symbols: True when some row of the batch has no valid symbol (a null `Symbol`).
        misses: The futures rows whose session the schedules could not place, reported for the
            first time, as `(symbol, message)`; they keep the 17:00 Chicago rule's date.
    """

    batch: pa.Table
    failures: list[RowFailure]
    symbols: frozenset[str] = frozenset()
    null_symbols: bool = False
    misses: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True, eq=False, slots=True)
class ChannelPlan:
    """How one dataset reads the rows of one WebSocket channel.

    Built once per (dataset, channel) by `build` from the committed generated files, immutable,
    and shared by every stream of the pair on every thread. `convert` runs on the flush thread.

    Attributes:
        dataset: The dataset name, such as `"stocks.trades"`.
        asset_class: The dataset's class.
        symbols: How the wire's symbols become canonical (`WireSymbols`), and what a spelling
            the library cannot read does.
        channel: The channel, such as `"T"`.
        wire_schema: The Arrow schema of the decoded rows: one field per column of the
            channel's source that the dataset keeps, named by its long-form column.
        columns: Per dataset column, in order: its type, its name, the long-form column of the
            rows that fills it (None for a column the channel does not carry, which is null), and
            the unit of an epoch column (None for any other).
        arrow_schema: The dataset's Arrow schema.
        date_columns: The dataset's calendar-date columns, stored one shared string per date
            (`columns.dataset_table`).
        symbol_field: The long-form column of the rows that holds the raw symbol.
        required: The long-form columns a row must carry: the dataset's keys for the channel's
            source, excluding keys filled from a request (`set_required`).
        times: How the rows' time columns are filled: the dataset's row times, its class,
            and for bars the channel's timespan (`row_times.TimeRule`).
        time_name: The dataset's time column (`Timestamp`, or a bar's `TimestampStart`), whose
            epoch `0` is kept, and so fails the timestamp bound, rather than read as absent.
    """

    dataset: str
    asset_class: AssetClass
    symbols: WireSymbols
    channel: str
    wire_schema: pa.Schema
    columns: tuple[tuple[ColumnType, str, str | None, TimeUnit | None], ...]
    arrow_schema: pa.Schema
    date_columns: tuple[str, ...]
    symbol_field: str
    required: frozenset[str]
    times: TimeRule
    time_name: str

    @classmethod
    def build(
        cls,
        catalog: Catalog,
        spec: DatasetSpec,
        channel: str,
        *,
        unknown_symbols: UnknownSymbols,
    ) -> ChannelPlan:
        """Prepare the reading of a channel's rows for a dataset.

        Args:
            catalog: The catalog, for the generated source and the dataset's merged schema.
            spec: A WebSocket dataset (with channels, so with an asset class).
            channel: One of its channels.
            unknown_symbols: The session's rule for a symbol spelling the library cannot
                read; `WARN` also adds the `RawSymbol` column to the schema.

        Returns:
            The plan.

        Raises:
            ValueError: If the dataset declares no symbol key for the channel's source: a
                stream routes rows by symbol, so it cannot read such a channel.

        An unknown source raises the `KeyError` of `Catalog.source`, and a bad key declaration
        the `ValueError` of `schema.rename`.
        """
        asset_class = cast("AssetClass", spec.asset_class)
        key = f"{asset_class.value}/{channel}"
        source = catalog.source(Transport.WS, key)
        renamed = rename(spec, source)
        symbol = spec.keys_for(Transport.WS, key).symbol
        if symbol is None or symbol.from_request:
            raise ValueError(f"{spec.name}: the WebSocket source {key} declares no symbol key")
        # A WebSocket source has no key filled from a request: every column has its field.
        fields = {c.column: (c, f) for c in renamed if (f := c.field) is not None}
        schema = catalog.dataset_schema(spec, raw_symbol=unknown_symbols is UnknownSymbols.WARN)
        _, symbol_field = fields[key_column(spec, "symbol", symbol)]
        # A bars channel serves one timespan, always of one unit; any other channel has none.
        span = next(span for span, name in spec.channels.items() if name == channel)
        timespan = None if span is NotBars.ANY else span
        return cls(
            dataset=spec.name,
            asset_class=asset_class,
            symbols=WireSymbols(spec.name, asset_class, unknown_symbols, spec.symbols_as_written),
            channel=channel,
            wire_schema=pa.schema(
                [pa.field(f.column, _WIRE_ARROW[c.wire_type]) for c, f in fields.values()]
            ),
            columns=tuple(
                (c.type, c.name, None, None)
                if c.name not in fields
                else (c.type, c.name, fields[c.name][1].column, fields[c.name][0].unit)
                for c in schema.columns
            ),
            arrow_schema=schema.arrow_schema(),
            date_columns=schema.date_columns,
            symbol_field=symbol_field.column,
            required=frozenset(f.column for f in required_fields(spec, source)),
            times=TimeRule(spec.name, spec.times, asset_class, timespan),
            time_name=time_column(spec.times),
        )

    def convert(
        self,
        rows: Sequence[Mapping[str, object]],
        symbols: dict[str, str | None],
        *,
        arrived: Sequence[int],
        sessions: FuturesSessionDates | None = None,
    ) -> Converted:
        """Turn one flush's rows of the channel into a batch of the dataset's schema.

        Pure Arrow apart from the symbol map: the rows become one Arrow table at once, each
        distinct raw symbol is read once (`WireSymbols.read`, remembered in `symbols`), every
        epoch column runs through the masked timestamp routine, every column is widened to its
        dataset type, the derived columns (`OptionRoot`, `RawSymbol`) are computed, and the
        library's time columns are filled (`row_times.TimeRule.fill`). A row
        with a timestamp that overflows or lies outside 1990 to 2100 is dropped and reported.
        A row whose symbol is a spelling the library cannot read is dropped and reported too,
        failing every stream of the dataset on the channel (it cannot be routed to one), when
        the session's `unknown_symbols` is `FAIL`; under `WARN` it is kept with a null
        `Symbol` and the text in `RawSymbol`.

        Args:
            rows: The decoded rows, keyed by long-form column.
            symbols: The caller's map from raw to canonical symbol (None for an unreadable
                spelling), read and extended here; one per flush thread and plan, so it needs
                no lock.
            arrived: When each row's frame arrived, epoch nanoseconds, one per row: the rows'
                `TimestampInsert`.
            sessions: For a futures channel, the schedule calendar that corrects the rows'
                session dates; None keeps the 17:00 Chicago rule's.

        Returns:
            The batch without the failing rows, one `RowFailure` per failing row in row order,
            the batch's distinct symbols, and the futures session misses reported for the
            first time.
        """
        wire = pa.Table.from_pylist(list(rows), schema=self.wire_schema)
        count = wire.num_rows
        failed: pa.Array | None = None
        failures: list[tuple[int, str, bool]] = []
        arrays: dict[str, pa.Array] = {}
        raw = pa.nulls(count, pa.string())
        for column_type, name, field, unit in self.columns:
            bad = None
            if name == OPTION_ROOT:
                values = option_roots(arrays[_SYMBOL])
            elif name == RAW_SYMBOL:
                values = raw
            elif field is None:
                values = pa.nulls(count, column_type.arrow_type)
            elif name == _SYMBOL:
                values, raw, bad = self._read_symbols(
                    wire.column(field), symbols, failures=failures
                )
            elif unit is not None:
                values, bad = self._instants(name, unit, wire.column(field), failures=failures)
            else:
                values = wire.column(field).combine_chunks()
            if bad is not None:
                failed = bad if failed is None else pc.call_function("or", [failed, bad])
            arrays[name] = widen(values, column_type.arrow_type)
        batch = pa.Table.from_arrays(list(arrays.values()), schema=self.arrow_schema)
        misses: list[tuple[str, str]] = []

        def corrected(symbols: pa.Array, instants: pa.Array, dates: pa.Array) -> pa.Array:
            fixed, found = cast("FuturesSessionDates", sessions).correct(symbols, instants, dates)
            misses.extend(found)
            return fixed

        batch = self.times.fill(
            batch,
            received=pa.array(arrived, pa.int64()),
            session_dates=None if sessions is None else corrected,
        )
        rejected: list[RowFailure] = []
        if failed is not None:
            canonical_symbols = batch.column(_SYMBOL)
            rejected = [
                RowFailure(canonical_symbols[index].as_py(), message, every)
                for index, message, every in sorted(failures)
            ]
            batch = batch.filter(pc.call_function("invert", [failed]))
        distinct = pc.call_function("unique", [batch.column(_SYMBOL)]).to_pylist()
        return Converted(
            batch,
            rejected,
            symbols=frozenset(s for s in distinct if s is not None),
            null_symbols=None in distinct,
            misses=tuple(misses),
        )

    def _read_symbols(
        self,
        column: pa.ChunkedArray,
        symbols: dict[str, str | None],
        *,
        failures: list[tuple[int, str, bool]],
    ) -> tuple[pa.Array, pa.Array, pa.Array | None]:
        """Read the rows' wire symbols; under `FAIL`, report the rows of unreadable ones.

        Args:
            column: The rows' symbols as sent.
            symbols: The flush thread's raw-to-canonical map (`convert`).
            failures: Extended with `(row, message, True)` for each row whose spelling the
                library cannot read, when the session's `unknown_symbols` is `FAIL`: such a
                row fails every stream of the channel (`RowFailure.every_stream`).

        Returns:
            The canonical symbols, the `RawSymbol` values, and the mask of the rows to drop
            (None when no row fails).
        """
        read = self.symbols.read(column.combine_chunks(), symbols)
        if not read.unknown or self.symbols.unknown is UnknownSymbols.WARN:
            return read.symbols, read.raw, None
        bad = pc.call_function("is_valid", [read.raw])
        message = unknown_symbol_message(self.dataset, read.unknown)
        indices = pc.call_function("indices_nonzero", [bad]).to_pylist()
        failures.extend((index, message, True) for index in indices)
        return read.symbols, read.raw, bad

    def _instants(
        self,
        name: str,
        unit: TimeUnit,
        column: pa.ChunkedArray,
        *,
        failures: list[tuple[int, str, bool]],
    ) -> tuple[pa.Array, pa.Array | None]:
        """Convert an epoch column, reporting the rows whose value fails the bound.

        Args:
            name: The column.
            unit: The epoch's unit.
            column: The rows' raw values.
            failures: Extended with `(row, message, False)` for each failing row.

        Returns:
            The values as nanoseconds, and the mask of the rows to drop (None when none fails).
        """
        values = column.combine_chunks()
        nanos, bad = masked_instants(unit, values, time_column=name == self.time_name)
        if not pc.call_function("any", [bad]).as_py():
            return nanos, None
        label = f"{self.dataset}.{name}"
        indices = pc.call_function("indices_nonzero", [bad]).to_pylist()
        failures.extend(
            (i, mismatch_message(label, unit, values[i].as_py()), False) for i in indices
        )
        return nanos, bad


class StreamSink:
    """One stream as its connection's flush thread publishes to it.

    Created by `open_stream_tables` on the caller thread and added to the connection's routing
    table by the transport; removed when the stream's blink table has shut down, or when its
    session closes. It refers to its session's core weakly (`core()`): a flush thread shared by
    several sessions needs to know which one each stream reports to, but a sink can outlive its
    stream's record in a container that is cleaned up later (the routes of a connection a
    verdict moved the stream off, until its flush thread ends), and a strong reference there
    would keep a finished session alive. While the stream is registered its record keeps the
    session in the live set, so the core is there whenever the stream still matters; once it is
    gone, the sink skips what it would have reported to it. Two locks keep it consistent
    whichever thread reaches it:

    - its leaf lock guards the symbol set, which the connection's loop replaces (`set_symbols`)
      when a reactive symbol table changes;
    - its publish lock serializes `publish` and `fail`, which call Deephaven. Usually only the
      flush thread takes it, but a stream can briefly be reached from two flush threads (when
      the server's verdict moves its connection), and is failed from the caller thread when
      its connection failed while it was opening, from the closing thread at `m.close()`, and
      from the reaper when its watchlist failed. The order is the flush lock (on a flush
      thread), then the publish lock, then `registry_lock` (`SessionCore.fail_stream`, which
      `fail` calls); the publish lock is never taken under `registry_lock`. A caller that holds
      the update-graph lock (a console command) takes it to fail the stream, which is safe
      because nothing done under it takes the update-graph lock or waits for an update cycle:
      `where_in` of static tables, `publisher.add` of a static table, `publish_failure`, and
      `SessionCore.fail_stream`.

    A failed stream publishes nothing more, and is failed once, so a failure queued behind the
    session's close never replaces the close.

    Attributes:
        stream_id: The stream's id.
        plan: How the stream's dataset reads its channel.
        publisher: The stream's publisher.
    """

    __slots__ = (
        "__weakref__",
        "_core",
        "_filter",
        "_filter_version",
        "_lock",
        "_publish_lock",
        "_symbols",
        "_version",
        "failed",
        "plan",
        "publisher",
        "rows_published",
        "stream_id",
    )

    def __init__(
        self,
        stream_id: int,
        *,
        core: SessionCore,
        plan: ChannelPlan,
        publisher: BlinkPublisher,
        symbols: frozenset[str] | None,
    ) -> None:
        """Create the sink.

        Args:
            stream_id: The stream's id.
            core: The stream's session, which records its rows and its failure.
            plan: How the stream's dataset reads its channel.
            publisher: The stream's publisher.
            symbols: The canonical symbols the stream receives; None for `dm.ALL`, which
                receives every row of the channel.
        """
        self.stream_id: Final = stream_id
        self._core: Final = weakref.ref(core)
        self.plan: Final = plan
        self.publisher: Final = publisher
        self._lock = threading.Lock()
        self._symbols = symbols
        self._version = 0
        # Publish-lock state.
        self._publish_lock = threading.Lock()
        self._filter: Table | None = None
        self._filter_version = -1
        self.rows_published = 0
        self.failed = False

    def core(self) -> SessionCore | None:
        """Return the stream's session core, or None once it has been freed.

        Returns:
            The core.
        """
        return self._core()

    def set_symbols(self, symbols: frozenset[str]) -> None:
        """Replace the symbols a reactive stream receives. Any thread; never blocks.

        Args:
            symbols: The canonical symbols the stream's watchlist now holds.
        """
        with self._lock:
            self._symbols = symbols
            self._version += 1

    def receives(self, symbol: str | None) -> bool:
        """Tell whether the stream would receive a row of `symbol`.

        Args:
            symbol: A canonical symbol, or None for a row without a valid one.

        Returns:
            True for a `dm.ALL` stream, and for a symbol stream that holds `symbol`.
        """
        with self._lock:
            symbols = self._symbols
        return symbols is None or (symbol is not None and symbol in symbols)

    def publish(self, batch: Table, converted: Converted) -> int:
        """Add the part of a channel's batch this stream receives.

        No Python work per row: a `dm.ALL` stream gets the whole batch; a symbol stream gets
        nothing when it holds none of the batch's symbols, the whole batch when it holds all of
        them (the common single-stream case), and otherwise a `where_in` against its cached
        filter table, rebuilt only when its symbols changed.

        Args:
            batch: The channel's static batch in the dataset's schema.
            converted: The batch's conversion, for its distinct symbols.

        Returns:
            The rows added; 0 once the stream has failed.
        """
        with self._publish_lock:
            if self.failed:
                return 0
            selected = self._select(batch, converted)
            added = 0 if selected is None else selected.size
            if selected is not None and added:
                self.publisher.add(selected)
                self.rows_published += added
            return added

    def _select(self, batch: Table, converted: Converted) -> Table | None:
        """Choose the rows of a batch the stream receives; the publish lock is held.

        Args:
            batch: The channel's batch.
            converted: Its conversion.

        Returns:
            The rows, or None when the stream receives none.
        """
        with self._lock:
            symbols, version = self._symbols, self._version
        if symbols is None:
            return batch
        if symbols.isdisjoint(converted.symbols):
            return None
        if converted.symbols <= symbols and not converted.null_symbols:
            return batch
        if version != self._filter_version:
            self._filter = new_table([string_col(_SYMBOL, sorted(symbols))])
            self._filter_version = version
        return batch.where_in(cast("Table", self._filter), _SYMBOL)

    def fail(self, error: MassiveError) -> bool:
        """Fail the stream's table and record the failure, once.

        The failure is recorded first (`SessionCore.fail_stream`) and the table failed after
        (`publish_failure`), so a verb that finds the table failed always finds the failure
        recorded too, and names it. When the session's close came first, the table fails with
        the close's error instead, so the table and its `CLOSED` row name the same cause, and
        nothing is logged (the close logs its count). Any other failure is logged at ERROR.
        The stream counts as failed only once `publish_failure` has returned, so a failure
        that raises can be retried. Never call it on an update-graph thread or under
        `registry_lock`; the thread needs an execution context.

        Args:
            error: The error the table fails with.

        Returns:
            False when the stream had already failed, so nothing was done.
        """
        with self._publish_lock:
            if self.failed:
                return False
            core = self._core()
            closed = None if core is None else core.fail_stream(self.stream_id, str(error))
            if closed is None:
                logger.error("%s stream %d failed: %s", self.plan.dataset, self.stream_id, error)
            self.publisher.publish_failure(
                error if closed in (None, str(error)) else MassiveError(closed)
            )
            self.failed = True
            return True

    def fail_later(self, message: str) -> None:
        """Queue the stream's failure on its session's reaper: its watchlist cannot be followed.

        The watchlist's listener reports a row that is not canonical, or the failure of the
        watchlist itself, on an update-graph thread, where failing a table (a Deephaven call
        that takes the publish lock) is not allowed, so the failure is only queued here; it
        never blocks. The reaper, unlike a connection's flush thread, runs as long as the
        session, also after the server's real-time verdict moved the stream.

        Args:
            message: Why the stream cannot follow its watchlist, naming the fix.
        """
        failure = MassiveError(message)

        def fail() -> None:
            self.fail(failure)

        core = self._core()
        if core is not None:  # a freed session has no stream left to fail
            core.defer(fail)


_Frame = tuple[int, list[dict[str, object]]]
"""One frame's rows of one channel, with the instant it arrived (epoch nanoseconds)."""


class _Routes:
    """The streams of one connection by channel, and the row buffer of each channel.

    Shared by the caller thread (a stream is added), the connection's loop thread (a stream is
    removed; rows are buffered), and the flush thread (it reads). Guarded by one leaf lock, held
    only to copy or replace a reference: the routes are an immutable mapping replaced on every
    change, so a reader works on a snapshot without the lock.
    """

    __slots__ = ("_buffers", "_lock", "_routes")

    def __init__(self) -> None:
        """Start with no stream and no buffer."""
        self._lock = threading.Lock()
        self._routes: Mapping[str, tuple[StreamSink, ...]] = {}
        self._buffers: Mapping[str, queue.SimpleQueue[_Frame]] = {}

    def add(self, channel: str, sink: StreamSink) -> None:
        """Route a channel's rows to a stream, creating the channel's buffer if needed.

        Args:
            channel: The stream's channel.
            sink: The stream.
        """
        with self._lock:
            routes = dict(self._routes)
            routes[channel] = (*routes.get(channel, ()), sink)
            self._routes = routes
            if channel not in self._buffers:
                self._buffers = {**self._buffers, channel: queue.SimpleQueue()}

    def remove(self, stream_id: int) -> StreamSink | None:
        """Stop routing rows to a stream.

        Args:
            stream_id: The stream.

        Returns:
            The removed stream, or None if it was not routed.
        """
        with self._lock:
            found = None
            routes: dict[str, tuple[StreamSink, ...]] = {}
            for channel, sinks in self._routes.items():
                kept = tuple(s for s in sinks if s.stream_id != stream_id)
                found = found or next((s for s in sinks if s.stream_id == stream_id), None)
                if kept:
                    routes[channel] = kept
            self._routes = routes
        return found

    def clear(self) -> None:
        """Stop routing rows to any stream: the flush thread has ended and reads no more."""
        with self._lock:
            self._routes = {}

    def sinks(self) -> tuple[StreamSink, ...]:
        """Return every routed stream.

        Returns:
            The streams, by channel and then in the order they were added.
        """
        with self._lock:
            routes = self._routes
        return tuple(sink for sinks in routes.values() for sink in sinks)

    def current(self) -> Mapping[str, tuple[StreamSink, ...]]:
        """Return the current routes, to read without the lock.

        Returns:
            The streams by channel.
        """
        with self._lock:
            return self._routes

    def buffers(self) -> tuple[tuple[str, queue.SimpleQueue[_Frame]], ...]:
        """Return the row buffer of each channel that has ever been routed.

        Returns:
            `(channel, buffer)` pairs.
        """
        with self._lock:
            return tuple(self._buffers.items())

    def buffer(self, channel: str) -> queue.SimpleQueue[_Frame] | None:
        """Return the row buffer of a channel.

        Args:
            channel: The channel.

        Returns:
            The buffer, or None when no stream was ever routed to the channel.
        """
        with self._lock:
            return self._buffers.get(channel)


@dataclass(frozen=True, slots=True)
class _Fail:
    """A request to fail streams, from the connection's loop thread.

    Attributes:
        stream_ids: The streams to fail.
        error: The error their tables fail with.
    """

    stream_ids: tuple[int, ...]
    error: MassiveError


@dataclass(frozen=True, slots=True)
class _Drift:
    """A channel's drift in one frame, from the connection's loop thread.

    Attributes:
        channel: The channel.
        drift: The frame's drift for the channel.
        where: The connection's URL and the channel, for the drift WARNING.
    """

    channel: str
    drift: DriftReport
    where: str


class Flusher:
    """The flush thread of one connection: drains its row buffers and publishes to its streams.

    Created by the connection; `start()` starts the daemon thread, which flushes every
    `interval` seconds until `stop()`. `flush()` may also be called directly (the tests do):
    flushes are serialized by the flush lock. The producers, the connection's loop thread and
    the caller thread, only put plain data on queues (`rows`, `fail`, `drift`) or change the
    routes, and never wait.

    The flusher holds no session core: the sinks it routes carry their own, and it reports each
    stream to that one. Each flush enters the execution context it was created with through a
    fresh wrapper. An exception in a flush is logged at ERROR and the thread keeps flushing;
    anything the thread cannot carry on after (a `KeyboardInterrupt` or `SystemExit`) first
    fails every routed stream, so no table is left frozen without an error.

    It counts the rows buffered and not taken by a flush yet (`backlog`), under a leaf lock of its
    own, and logs a WARNING when the count passes `BACKLOG_WARNING_ROWS`. The buffers are
    unbounded on purpose: a slow flush delays rows but never drops them.

    It remembers the canonical symbol of each wire spelling it has read, per dataset and channel,
    so steady traffic costs no symbol reading. That memory is bounded: a flush forgets the
    datasets no longer routed, and empties a dataset's memory once it passes
    `SYMBOL_CACHE_LIMIT` spellings. A spelling the library cannot read is still warned about
    once, whatever is forgotten.

    Attributes:
        routes: The connection's streams by channel, and its row buffers.
    """

    __slots__ = (
        "_backlog",
        "_backlog_lock",
        "_backlog_warned",
        "_control",
        "_exec_ctx",
        "_final",
        "_flush_lock",
        "_interval",
        "_sessions",
        "_shown",
        "_stop",
        "_symbols",
        "_thread",
        "routes",
    )

    def __init__(
        self,
        exec_ctx: ExecutionContext,
        name: str,
        *,
        interval: float,
        sessions: FuturesSessionDates | None = None,
    ) -> None:
        """Create the flusher; nothing runs until `start()`.

        Args:
            exec_ctx: The execution context every flush enters: that of the session which
                created the connection.
            name: The thread's name, for thread dumps.
            interval: Seconds between flushes (`MassiveConfig.flush_interval`).
            sessions: For a futures connection, the schedule calendar that corrects its rows'
                session dates; None otherwise.
        """
        self._exec_ctx = exec_ctx
        self._interval = interval
        self._sessions = sessions
        self.routes: Final = _Routes()
        self._control: queue.SimpleQueue[_Fail | _Drift] = queue.SimpleQueue()
        self._flush_lock = threading.Lock()
        self._stop = threading.Event()
        self._final = threading.Event()
        # Flush-lock state: the raw-to-canonical symbol map of each routed plan, at most
        # `SYMBOL_CACHE_LIMIT` spellings each after a flush.
        self._symbols: dict[ChannelPlan, dict[str, str | None]] = {}
        # Flush-lock state: the futures session miss each stream's `Error` shows, by stream.
        self._shown: dict[int, str] = {}
        # Guarded by the backlog lock, a leaf lock.
        self._backlog_lock = threading.Lock()
        self._backlog = 0
        self._backlog_warned = False
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)

    @property
    def backlog(self) -> int:
        """Rows buffered and not taken by a flush yet: received, and waiting for the next one."""
        with self._backlog_lock:
            return self._backlog

    def start(self) -> None:
        """Start the flush thread. Call it once."""
        self._thread.start()

    def stop(self, *, final: bool = False) -> None:
        """Ask the flush thread to end after its current flush. Never waits; idempotent.

        Args:
            final: True to have the thread flush once more before it ends, so the failures
                queued last (a connection's fatal failure fails every stream) are applied.
                False when the session closes: what is still buffered is not published, and
                no stream is failed here, since the close has already failed its tables.
        """
        if final:
            self._final.set()
        self._stop.set()

    def join(self, timeout: float) -> bool:
        """Wait for the flush thread to end, if it was started.

        Args:
            timeout: The longest wait in seconds.

        Returns:
            Whether the thread has ended (true for a thread never started).
        """
        if self._thread.ident is not None:
            self._thread.join(timeout)
        return not self._thread.is_alive()

    def rows(self, channel: str, rows: list[dict[str, object]]) -> None:
        """Buffer one frame's rows of a channel, with the instant they arrived. Never blocks.

        The instant, `time.time_ns()` now, on the connection's loop thread as the frame is
        handed over, becomes every row's `TimestampInsert`. Rows of a channel no stream was
        ever routed to are dropped. The rows count in the backlog until a flush takes them; a
        backlog that passes `BACKLOG_WARNING_ROWS` is logged at WARNING (see that constant for
        when it is logged again).

        Args:
            channel: The channel.
            rows: The decoded rows, keyed by long-form column; the flusher keeps the list.
        """
        buffer = self.routes.buffer(channel)
        if buffer is None:
            return
        with self._backlog_lock:  # counted before the put, so a flush never counts below zero
            self._backlog += len(rows)
            backlog = self._backlog
            warn = backlog > BACKLOG_WARNING_ROWS and not self._backlog_warned
            self._backlog_warned |= warn
        buffer.put((time.time_ns(), rows))
        if warn:
            logger.warning(
                "%s: %d rows are waiting to be published, more than %d; the flushes are "
                "falling behind (a stalled or slow update graph), so memory grows until they "
                "catch up",
                self._thread.name,
                backlog,
                BACKLOG_WARNING_ROWS,
            )

    def fail(self, stream_ids: Iterable[int], error: MassiveError) -> None:
        """Ask the flush thread to fail streams. Never blocks.

        Args:
            stream_ids: The streams; one already failed is skipped.
            error: The error their tables fail with, also logged at ERROR and shown in
                `m.streams()`.
        """
        self._control.put(_Fail(tuple(stream_ids), error))

    def drift(self, channel: str, drift: DriftReport, *, where: str) -> None:
        """Ask the flush thread to report a channel's drift for the streams on it. Never blocks.

        Args:
            channel: The channel.
            drift: The frame's drift for the channel.
            where: Where the frame came from, such as `"wss://delayed.massive.com/stocks
                channel T"`, named in the drift WARNING.
        """
        self._control.put(_Drift(channel, drift, where))

    def _run(self) -> None:
        """Flush every interval until `stop()`; a failing flush is logged and the next one runs.

        Anything else that escapes a flush ends the thread: it is logged at CRITICAL, every
        routed stream is failed with a `MassiveError` naming the thread, and it is re-raised.
        However the thread ends, it clears the routes last: nothing reads them any more, and a
        stream left there (one a verdict moved to another connection, which this thread kept
        routed for its final flush) would be held by a connection that is gone.
        """
        try:
            while not self._stop.wait(self._interval):
                self._flush_logged()
            if self._final.is_set():
                self._flush_logged()
        except BaseException as death:
            logger.critical("%s died; its streams fail", self._thread.name, exc_info=True)
            error = MassiveError(
                f"the flush thread {self._thread.name} died ({death!r}); this table no longer "
                "updates"
            )
            with entered(self._exec_ctx):
                for sink in self.routes.sinks():
                    try:
                        sink.fail(error)
                    except Exception:
                        logger.exception("%s: could not fail a stream", self._thread.name)
            raise
        finally:
            self.routes.clear()

    def _flush_logged(self) -> None:
        """Flush once, logging a failure at ERROR instead of raising it."""
        try:
            self.flush()
        except Exception:
            logger.exception("%s: a flush failed; continuing", self._thread.name)

    def flush(self) -> None:
        """Apply the queued failures, publish every buffered row, then report the queued drift.

        Failures come first, so a stream that failed gets no row published after its failure
        was reported. Runs under the flush lock, inside the flusher's execution context, with
        no other library lock held: it calls Deephaven (`to_table`, `where_in`, `add`,
        `publish_failure`) and then each session with a stream that published once
        (`update_streams`).
        """
        with self._flush_lock, entered(self._exec_ctx):
            controls = _drain(self._control)
            drained = {channel: _drain(buffer) for channel, buffer in self.routes.buffers()}
            # Each frame is `(arrived, rows)`: the backlog counts the rows, not the pairs.
            self._drained(sum(len(rows) for frames in drained.values() for _, rows in frames))
            # Routes after the rows: a stream routed meanwhile gets the rows that reached it.
            routes = self.routes.current()
            for item in controls:
                if isinstance(item, _Fail):
                    for sink in self.routes.sinks():
                        if sink.stream_id in item.stream_ids:
                            sink.fail(item.error)
            updates: dict[StreamSink, StreamUpdate] = {}
            for channel, frames in drained.items():
                sinks = routes.get(channel, ())
                if frames and sinks:
                    rows = [row for _, frame in frames for row in frame]
                    arrived = [at for at, frame in frames for _ in frame]
                    self._publish(rows, sinks, arrived=arrived, updates=updates)
            for item in controls:
                if isinstance(item, _Drift):
                    self._report_drift(routes.get(item.channel, ()), item.drift, where=item.where)
            _report_updates(updates)
            routed = [sink for sinks in routes.values() for sink in sinks]
            if self._shown:  # forget the misses of streams no longer routed
                live = {sink.stream_id for sink in routed}
                self._shown = {i: m for i, m in self._shown.items() if i in live}
            if self._symbols:  # forget the spellings of datasets no longer routed
                plans = {sink.plan for sink in routed}
                self._symbols = {p: m for p, m in self._symbols.items() if p in plans}

    def _drained(self, count: int) -> None:
        """Take rows a flush drained off the backlog; re-arm the WARNING below half the level.

        Args:
            count: How many rows the flush took from the buffers.
        """
        with self._backlog_lock:
            self._backlog -= count
            if self._backlog < BACKLOG_WARNING_ROWS // 2:
                self._backlog_warned = False

    def _publish(
        self,
        rows: list[dict[str, object]],
        sinks: tuple[StreamSink, ...],
        *,
        arrived: list[int],
        updates: dict[StreamSink, StreamUpdate],
    ) -> None:
        """Convert a channel's rows once per dataset and add them to each of its streams.

        Args:
            rows: The channel's rows of this flush, in arrival order.
            sinks: The channel's streams.
            arrived: When each row's frame arrived, epoch nanoseconds, one per row.
            updates: Receives each stream's new published-row total and session-date misses.
        """
        by_plan: dict[ChannelPlan, list[StreamSink]] = {}
        for sink in sinks:
            if not sink.failed:
                by_plan.setdefault(sink.plan, []).append(sink)
        for plan, readers in by_plan.items():
            try:
                self._publish_plan(plan, rows, readers=readers, arrived=arrived, updates=updates)
            except Exception as error:  # one dataset's failure never costs the others their rows
                logger.exception("%s: could not publish a %s batch", plan.dataset, plan.channel)
                failure = MassiveError(
                    f"{plan.dataset}: could not publish a {plan.channel} batch: {error}"
                )
                for sink in readers:
                    sink.fail(failure)

    def _publish_plan(
        self,
        plan: ChannelPlan,
        rows: list[dict[str, object]],
        *,
        readers: list[StreamSink],
        arrived: list[int],
        updates: dict[StreamSink, StreamUpdate],
    ) -> None:
        """Convert a channel's rows for one dataset once and add them to its streams.

        A futures session-date miss (a row the schedules cannot place, which keeps the 17:00
        Chicago rule's date) is shown in the `Error` of every stream that receives its symbol,
        written again only when the message changes; it does not fail the stream. A stream
        whose next published rows all place clears it.

        Args:
            plan: The dataset's plan for the channel.
            rows: The channel's rows of this flush.
            readers: The dataset's streams on the channel.
            arrived: When each row's frame arrived, epoch nanoseconds, one per row.
            updates: Receives each stream's new published-row total and session-date misses.
        """
        converted = self._convert(plan, rows, arrived=arrived)
        missed: set[int] = set()
        for symbol, message in converted.misses:
            for sink in readers:
                if sink.receives(symbol):
                    missed.add(sink.stream_id)
                    if self._shown.get(sink.stream_id) != message:
                        self._shown[sink.stream_id] = message
                        found = updates.get(sink, StreamUpdate())
                        updates[sink] = replace(found, error=message)
        for failure in converted.failures:
            error = MassiveError(failure.message)
            for sink in readers:
                if failure.every_stream or sink.receives(failure.symbol):
                    sink.fail(error)
        if not converted.batch.num_rows:
            return
        table = dataset_table(converted.batch, plan.date_columns)
        for sink in readers:
            if sink.publish(table, converted):
                found = updates.get(sink, StreamUpdate())
                cleared = sink.stream_id not in missed and sink.stream_id in self._shown
                if cleared:
                    del self._shown[sink.stream_id]
                updates[sink] = replace(
                    found, rows_published=sink.rows_published, clear_error=cleared
                )

    def _convert(
        self, plan: ChannelPlan, rows: list[dict[str, object]], *, arrived: list[int]
    ) -> Converted:
        """Convert a channel's rows for one dataset, with the plan's remembered symbols.

        The plan's memory of wire spellings is emptied after the conversion once it holds more
        than `SYMBOL_CACHE_LIMIT` of them, logged at DEBUG; the next flushes refill it.

        Args:
            plan: The dataset's plan for the channel.
            rows: The channel's rows of this flush.
            arrived: When each row's frame arrived, epoch nanoseconds, one per row.

        Returns:
            The plan's conversion of the rows (`ChannelPlan.convert`).
        """
        known = self._symbols.setdefault(plan, {})
        converted = plan.convert(rows, known, arrived=arrived, sessions=self._sessions)
        if len(known) > SYMBOL_CACHE_LIMIT:
            logger.debug(
                "%s: the %s symbol cache passed %d spellings and starts over",
                plan.dataset,
                plan.channel,
                SYMBOL_CACHE_LIMIT,
            )
            known.clear()
        return converted

    @staticmethod
    def _report_drift(sinks: tuple[StreamSink, ...], drift: DriftReport, *, where: str) -> None:
        """Log a channel's drift once per session, dataset, and field; add it to each stream's row.

        Args:
            sinks: The channel's streams.
            drift: The drift.
            where: The connection's URL and the channel.
        """
        live = [(core, sink) for sink in sinks if (core := sink.core()) is not None]
        for core, dataset in dict.fromkeys((core, sink.plan.dataset) for core, sink in live):
            core.drift.report(dataset, Transport.WS, drift, where=where)
        for core, sink in live:
            core.record_stream_drift(sink.stream_id, Transport.WS, drift)


def _report_updates(updates: Mapping[StreamSink, StreamUpdate]) -> None:
    """Hand each session the row changes of its streams, in one `update_streams` call.

    A stream whose session has been freed shows no row, so its change is dropped.

    Args:
        updates: The change of each stream.
    """
    by_core: dict[SessionCore, dict[int, StreamUpdate]] = {}
    for sink, update in updates.items():
        core = sink.core()
        if core is not None:
            by_core.setdefault(core, {})[sink.stream_id] = update
    for core, changes in by_core.items():
        core.update_streams(changes)


def _fail_later(sink_ref: weakref.ref[StreamSink], message: str) -> None:
    """A watch's `on_invalid`: queue its stream's failure, if the stream still exists.

    The listener's callbacks are held from Java until the JVM collects the listener, so they
    hold the sink, and through it the session, only weakly: a strong reference there would
    keep a finished session alive until the next JVM collection.

    Args:
        sink_ref: The stream's sink, weakly.
        message: Why the stream cannot follow its watchlist.
    """
    sink = sink_ref()
    if sink is not None:
        sink.fail_later(message)


def _drain(source: queue.SimpleQueue[_T]) -> list[_T]:
    """Take everything queued so far, without waiting.

    Args:
        source: The queue.

    Returns:
        The items, in queue order.
    """
    items = []
    while True:
        try:
            items.append(source.get_nowait())
        except queue.Empty:
            return items


@dataclass(frozen=True, slots=True)
class OpenedStream:
    """What `open_stream_tables` created for one stream.

    Attributes:
        handle: The stream as the session holds it: its id, blink table, scope, host, path,
            and its fail and watch-stop hooks.
        sink: The stream as its connection's flush thread publishes to it.
        watch: The watch of a reactive symbol table, not yet started; None otherwise. The
            handle's `stop_watch` holds it, so the session's record keeps it reachable.
    """

    handle: StreamHandle
    sink: StreamSink
    watch: SymbolWatch | None


def open_stream_tables(
    plan: ChannelPlan,
    schema: Schema,
    symbols: NormalizedSymbols,
    *,
    core: SessionCore,
    stream_id: int,
    host: str,
    path: str,
    on_shutdown: Callable[[], None],
    on_symbols: Callable[[frozenset[str], frozenset[str]], None],
) -> OpenedStream:
    """Create a stream's scope and, inside it, its publisher, blink table, and symbol watch.

    Runs on the caller thread, inside the session's execution context, with no library lock
    held. The publisher is named `massive.{dataset}#{stream_id}`. For a reactive symbol table,
    the `SymbolWatch` is created in the stream scope and tied to the blink table's liveness
    (`SymbolWatch.keep_alive_with`), so it follows the watchlist for as long as anything keeps
    the stream alive, and the handle holds it until the session's record of the stream goes;
    the caller starts it once the stream's topics can receive its changes. A watchlist row that
    is not canonical, or a failure of the watchlist itself, queues the stream's failure
    (`StreamSink.fail_later`). If anything raises, even
    `KeyboardInterrupt`, the scope is released (which shuts the blink table down and runs
    `on_shutdown`) and the exception propagates.

    Args:
        plan: How the dataset reads the channel.
        schema: The dataset's merged schema, the blink table's columns.
        symbols: The stream's symbols: a sorted tuple of canonical symbols, `dm.ALL`, or a
            reactive table's `SymbolTable` box.
        core: The stream's session, which its sink reports to.
        stream_id: The id the session minted for the stream.
        host: The WebSocket host serving the stream.
        path: The cluster path, such as `/stocks`.
        on_shutdown: The blink table's `on_shutdown_callback`.
        on_symbols: Receives the watch's changes of a reactive symbol table.

    Returns:
        The stream's handle, sink, and watch.
    """
    scope = new_scope()
    try:
        with scope.open():
            blink, publisher = blink_publisher(
                f"massive.{plan.dataset}#{stream_id}",
                col_defs(schema),
                on_shutdown=on_shutdown,
                release_later=core.release_later,
            )
            watch = None
            received: frozenset[str] | None
            if isinstance(symbols, _All):
                received = None
            else:
                reactive = isinstance(symbols, SymbolTable)
                received = frozenset() if reactive else frozenset(cast("tuple[str, ...]", symbols))
            sink = StreamSink(
                stream_id, core=core, plan=plan, publisher=publisher, symbols=received
            )
            if isinstance(symbols, SymbolTable):
                watch = SymbolWatch(
                    plan.asset_class,
                    symbols.table,
                    on_symbols,
                    on_invalid=functools.partial(_fail_later, weakref.ref(sink)),
                )
                watch.keep_alive_with(blink)
    except BaseException:  # also KeyboardInterrupt: the scope must not leak
        release_scope(core.exec_ctx, scope)
        raise
    handle = StreamHandle(
        stream_id,
        blink,
        scope,
        host,
        path,
        sink.fail,
        stop_watch=None if watch is None else watch.stop,
    )
    return OpenedStream(handle, sink, watch)
