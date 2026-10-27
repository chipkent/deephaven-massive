"""Live streams on the Deephaven side: publishers, the flush thread, and the shared-batch fan-out.

A WebSocket *stream* is one `(dataset, channel, symbols)` with its own `TablePublisher` and blink
table. The WebSocket transport (`_ingest/ws.py`) owns the connections and decides which topics
are subscribed; this module owns everything that touches Deephaven for a stream:

- `ChannelPlan`, how one dataset reads one channel: the channel's generated columns, renamed
  for the dataset (`schema.rename`), their wire types, the dataset's merged schema, and the
  required keys and symbol column the wire client checks (`set_required`). Pure; built once
  per (dataset, channel).
- `open_stream_tables`, which creates a stream's liveness scope and, inside it, the publisher,
  its blink table, and, for a reactive symbol table, the `SymbolWatch` that follows it. It runs
  on the caller thread, inside the session's execution context.
- `StreamSink`, a stream as the flush thread sees it: the publisher, the symbols it receives
  (`None` for `dm.ALL`), and its count of published rows.
- `Flusher`, one per connection: a daemon thread that every `flush_interval` drains the
  connection's row buffers (one `queue.SimpleQueue` per channel, fed by the connection's loop
  thread) and its control queue (failures and drift), and publishes.

**Fan-out.** Per channel and flush, the rows (keyed by long-form column, symbols as the wire
spells them) are converted once: one Arrow table built from the rows, raw symbols mapped to
canonical ones per distinct symbol, every epoch run through the timestamp routine as a
vectorized mask, columns widened to the dataset schema. A row whose timestamp fails the bound
fails only the streams that would have received it, and is dropped from the batch. The batch
becomes one static Deephaven table (`deephaven.arrow.to_table`); then each `dm.ALL` stream gets
`publisher.add(batch)` and each symbol stream `publisher.add(batch.where_in(filter, "Symbol"))`,
where `filter` is a static one-column table cached per stream and rebuilt only when the stream's
symbols change. So the Python work is O(rows) per channel and flush, independent of the number
of streams, and the per-stream work is one Java filter and one `add`. Each flush ends with one
`SessionCore.update_streams` call carrying every stream's published-row total.

**Failures.** The connection's loop thread never calls Java, so it hands every stream failure
(a refused topic, a rejected row, a rejected key) to the flush thread's control queue; the
flush thread fails the stream's table with `publish_failure` and records it with
`SessionCore.fail_stream`, once per stream. Drift goes through the same queue: the flush thread
logs it once per (dataset, transport, field) through the session's `DriftRegistry` and adds it
to each receiving stream's `SchemaDrift` (`SessionCore.record_stream_drift`).

**Threads and locks.** The flush thread creates only static tables, enters the session's
execution context through its own wrapper, and never holds `registry_lock` while it calls
Deephaven. Its one lock, the flush lock, serializes flushes (the thread's own and any a test
calls) and is never taken under another library lock; the routing table has its own leaf lock.
The flusher holds the session core weakly, so a running flush thread never keeps a session
alive.
"""

from __future__ import annotations

import logging
import queue
import threading
import weakref
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, TypeVar, cast

import pyarrow as pa
import pyarrow.compute as pc
from deephaven import new_table
from deephaven.arrow import to_table
from deephaven.column import string_col
from deephaven.stream.table_publisher import table_publisher

from deephaven_massive._datasets.schema import conform, key_column, rename, required_fields
from deephaven_massive._datasets.symbols import SymbolTable, canonical
from deephaven_massive._datasets.timestamps import masked_instants, mismatch_message
from deephaven_massive._errors import MassiveError, log_once
from deephaven_massive._massive import Transport, WireType
from deephaven_massive._tables.columns import col_defs
from deephaven_massive._tables.diagnostics import StreamUpdate
from deephaven_massive._tables.liveness import StreamHandle, entered, new_scope
from deephaven_massive._tables.watchlist import SymbolWatch
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from deephaven.stream.table_publisher import TablePublisher
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.schema import ColumnType, Schema, SourceColumn
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._enums import AssetClass
    from deephaven_massive._massive import DriftReport
    from deephaven_massive._tables.core import SessionCore

__all__ = [
    "ChannelPlan",
    "Flusher",
    "OpenedStream",
    "RowFailure",
    "StreamSink",
    "open_stream_tables",
]

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

_SYMBOL: Final = "Symbol"
_TIMESTAMP: Final = "Timestamp"

_WIRE_ARROW: Final[Mapping[WireType, pa.DataType]] = {
    WireType.LONG: pa.int64(),
    WireType.EPOCH: pa.int64(),
    WireType.DOUBLE: pa.float64(),
    WireType.BOOL: pa.bool_(),
    WireType.STRING: pa.string(),
    WireType.JSON_TEXT: pa.string(),
    WireType.DATE: pa.date32(),
    WireType.ISO_INSTANT: pa.int64(),
    WireType.INT_ARRAY: pa.list_(pa.int32()),
}
"""The Arrow type of each wire type as the WebSocket client delivers its values: an epoch as its
raw integer count (converted by its unit afterwards), an ISO-8601 instant as integer
nanoseconds, a date as a `datetime.date`, and a list as a list of integers."""


@dataclass(frozen=True, slots=True)
class RowFailure:
    """A received row that cannot be published, and the symbol whose streams it fails.

    Attributes:
        symbol: The row's canonical symbol, or None when the row has none that is valid; then
            only the channel's `dm.ALL` streams would have received it.
        message: Why, naming the dataset, the column, the unit, and the value.
    """

    symbol: str | None
    message: str


@dataclass(frozen=True, eq=False, slots=True)
class ChannelPlan:
    """How one dataset reads the rows of one WebSocket channel.

    Built once per (dataset, channel) by `build` from the committed generated files, immutable,
    and shared by every stream of the pair on every thread. `convert` runs on the flush thread.

    Attributes:
        dataset: The dataset name, such as `"stocks.trades"`.
        asset_class: The dataset's class, whose symbol rules map the wire's symbols.
        channel: The channel, such as `"T"`.
        wire_schema: The Arrow schema of the decoded rows: one field per column of the
            channel's source that the dataset keeps, named by its long-form column.
        columns: Per dataset column, in order: its type, its name, and the source column that
            fills it, or None for a column the channel does not carry (null).
        arrow_schema: The dataset's Arrow schema.
        symbol_field: The long-form column of the rows that holds the raw symbol.
        required: The long-form columns a row must carry: the dataset's keys for the channel's
            source, excluding keys filled from a request (`set_required`).
    """

    dataset: str
    asset_class: AssetClass
    channel: str
    wire_schema: pa.Schema
    columns: tuple[tuple[ColumnType, str, SourceColumn | None], ...]
    arrow_schema: pa.Schema
    symbol_field: str
    required: frozenset[str]

    @classmethod
    def build(cls, catalog: Catalog, spec: DatasetSpec, channel: str) -> ChannelPlan:
        """Prepare the reading of a channel's rows for a dataset.

        Args:
            catalog: The catalog, for the generated source and the dataset's merged schema.
            spec: A WebSocket dataset (with channels, so with an asset class).
            channel: One of its channels.

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
        schema = catalog.dataset_schema(spec)
        _, symbol_field = fields[key_column("symbol", symbol)]
        return cls(
            dataset=spec.name,
            asset_class=asset_class,
            channel=channel,
            wire_schema=pa.schema(
                [pa.field(f.column, _WIRE_ARROW[c.wire_type]) for c, f in fields.values()]
            ),
            columns=tuple(
                (c.type, c.name, fields[c.name][0] if c.name in fields else None)
                for c in schema.columns
            ),
            arrow_schema=schema.arrow_schema(),
            symbol_field=symbol_field.column,
            required=frozenset(f.column for f in required_fields(spec, source)),
        )

    def convert(
        self, rows: Sequence[Mapping[str, object]], symbols: dict[str, str | None]
    ) -> tuple[pa.Table, list[RowFailure]]:
        """Turn one flush's rows of the channel into a batch of the dataset's schema.

        Pure Arrow apart from the symbol map: the rows become one Arrow table at once, each
        distinct raw symbol is mapped to its canonical form once (and remembered in
        `symbols`), every epoch column runs through the masked timestamp routine, and every
        column is widened to its dataset type. A row with a timestamp that overflows or lies
        outside 1990 to 2100 is dropped and reported; a raw symbol that is not valid for the
        class becomes a null `Symbol` and is logged once.

        Args:
            rows: The decoded rows, keyed by long-form column.
            symbols: The caller's map from raw to canonical symbol, read and extended here;
                one per flush thread and plan, so it needs no lock.

        Returns:
            `(batch, failures)`: the batch without the failing rows, and one `RowFailure` per
            failing row, in row order.
        """
        wire = pa.Table.from_pylist(list(rows), schema=self.wire_schema)
        count = wire.num_rows
        failed: pa.Array | None = None
        failures: list[tuple[int, str]] = []
        arrays = []
        for column_type, name, source in self.columns:
            if source is None or source.field is None:
                arrays.append(pa.nulls(count, column_type.arrow_type))
                continue
            values = wire.column(source.field.column).combine_chunks()
            if name == _SYMBOL:
                values = self._canonical(values, symbols)
            elif source.unit is not None:
                nanos, bad = masked_instants(source.unit, values, time_column=name == _TIMESTAMP)
                if pc.any(bad).as_py():
                    label = f"{self.dataset}.{name}"
                    for index in pc.indices_nonzero(bad).to_pylist():
                        sent = values[index].as_py()
                        failures.append((index, mismatch_message(label, source.unit, sent)))
                    failed = bad if failed is None else pc.or_(failed, bad)
                values = nanos
            arrays.append(conform(values, column_type))
        batch = pa.Table.from_arrays(arrays, schema=self.arrow_schema)
        if failed is None:
            return batch, []
        canonical_symbols = batch.column(_SYMBOL)
        return batch.filter(pc.invert(failed)), [
            RowFailure(canonical_symbols[index].as_py(), message)
            for index, message in sorted(failures)
        ]

    def _canonical(self, raw: pa.Array, symbols: dict[str, str | None]) -> pa.Array:
        """Map a column of wire symbols to canonical symbols, one Python call per new symbol.

        Args:
            raw: The rows' symbols as the wire spells them.
            symbols: The raw-to-canonical map, extended with the symbols not seen before.

        Returns:
            The canonical symbols; null where a symbol is not valid for the class.
        """
        distinct = pc.unique(raw)
        for value in distinct.to_pylist():
            if value is not None and value not in symbols:
                symbols[value] = _canonical_or_none(self.asset_class, value, self.channel)
        mapped = pa.array(
            [symbols.get(v) if v is not None else None for v in distinct.to_pylist()], pa.string()
        )
        return pc.take(mapped, pc.index_in(raw, value_set=distinct))


def _canonical_or_none(asset_class: AssetClass, raw: str, channel: str) -> str | None:
    """Return the canonical form of a wire symbol, or None (logged once) if it has none.

    Args:
        asset_class: The class whose symbol rules apply.
        raw: The symbol as the wire spells it.
        channel: The channel it arrived on, for the log.

    Returns:
        The canonical symbol, or None.
    """
    try:
        return canonical(asset_class, raw)
    except ValueError:
        log_once(
            logger,
            f"invalid-wire-symbol:{asset_class.value}/{channel}",
            "%s channel %s sent %r, which is not a valid symbol; such rows have a null Symbol",
            asset_class.value,
            channel,
            raw,
            level=logging.WARNING,
        )
        return None


class StreamSink:
    """One stream as its connection's flush thread publishes to it.

    Created by `open_stream_tables` on the caller thread and added to the connection's routing
    table by the transport; removed when the stream's blink table has shut down. The publisher,
    the plan, and the published-row count are the flush thread's: it is the only thread that
    adds, fails, or counts. The symbol set may be replaced from another thread (the connection's
    loop, when a reactive symbol table changes) through `set_symbols`, under the sink's own leaf
    lock; the flush thread rebuilds its cached filter table when the version changes.

    Attributes:
        stream_id: The stream's id.
        plan: How the stream's dataset reads its channel.
        publisher: The stream's publisher.
    """

    __slots__ = (
        "_filter",
        "_filter_version",
        "_lock",
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
        plan: ChannelPlan,
        publisher: TablePublisher,
        symbols: frozenset[str] | None,
    ) -> None:
        """Create the sink.

        Args:
            stream_id: The stream's id.
            plan: How the stream's dataset reads its channel.
            publisher: The stream's publisher.
            symbols: The canonical symbols the stream receives; None for `dm.ALL`, which
                receives every row of the channel.
        """
        self.stream_id: Final = stream_id
        self.plan: Final = plan
        self.publisher: Final = publisher
        self._lock = threading.Lock()
        self._symbols = symbols
        self._version = 0
        # Flush-thread state.
        self._filter: Table | None = None
        self._filter_version = -1
        self.rows_published = 0
        self.failed = False

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

    def select(self, batch: Table) -> Table | None:
        """Return the part of a channel's batch this stream receives. Flush thread only.

        Args:
            batch: The channel's static batch in the dataset's schema.

        Returns:
            `batch` itself for a `dm.ALL` stream; its rows of the stream's symbols for a symbol
            stream (a `where_in` against the cached filter table); None when the stream holds
            no symbol.
        """
        with self._lock:
            symbols, version = self._symbols, self._version
        if symbols is None:
            return batch
        if not symbols:
            return None
        if version != self._filter_version:
            self._filter = new_table([string_col(_SYMBOL, sorted(symbols))])
            self._filter_version = version
        return batch.where_in(cast("Table", self._filter), _SYMBOL)


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
        self._buffers: Mapping[str, queue.SimpleQueue[list[dict[str, object]]]] = {}

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

    def sinks(self) -> tuple[StreamSink, ...]:
        """Return every routed stream.

        Returns:
            The streams, by channel and then in the order they were added.
        """
        with self._lock:
            routes = self._routes
        return tuple(sink for sinks in routes.values() for sink in sinks)

    def snapshot(
        self,
    ) -> tuple[
        Mapping[str, tuple[StreamSink, ...]],
        Mapping[str, queue.SimpleQueue[list[dict[str, object]]]],
    ]:
        """Return the current routes and buffers, to read without the lock.

        Returns:
            `(routes, buffers)`: the streams by channel, and the row buffer of each channel
            that has ever been routed.
        """
        with self._lock:
            return self._routes, self._buffers

    def buffer(self, channel: str) -> queue.SimpleQueue[list[dict[str, object]]] | None:
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
    """

    channel: str
    drift: DriftReport


class Flusher:
    """The flush thread of one connection: drains its row buffers and publishes to its streams.

    Created by the connection with the session core; `start()` starts the daemon thread, which
    flushes every `interval` seconds until `stop()`. `flush()` may also be called directly (the
    tests do): flushes are serialized by the flush lock. The producers, the connection's loop
    thread and the caller thread, only put plain data on queues (`rows`, `fail`, `drift`) or
    change the routes, and never wait.

    The flusher holds the core weakly: when the session has been collected, a flush does
    nothing. Each flush enters the session's execution context through a fresh wrapper. An
    exception in a flush is logged at ERROR and the thread keeps flushing.

    Attributes:
        routes: The connection's streams by channel, and its row buffers.
    """

    __slots__ = (
        "_control",
        "_core",
        "_flush_lock",
        "_interval",
        "_stop",
        "_symbols",
        "_thread",
        "routes",
    )

    def __init__(self, core: SessionCore, name: str, *, interval: float) -> None:
        """Create the flusher; nothing runs until `start()`.

        Args:
            core: The session's core, held weakly.
            name: The thread's name, for thread dumps.
            interval: Seconds between flushes (`MassiveConfig.flush_interval`).
        """
        self._core = weakref.ref(core)
        self._interval = interval
        self.routes: Final = _Routes()
        self._control: queue.SimpleQueue[_Fail | _Drift] = queue.SimpleQueue()
        self._flush_lock = threading.Lock()
        self._stop = threading.Event()
        # Flush-lock state: the raw-to-canonical symbol map of each plan.
        self._symbols: dict[ChannelPlan, dict[str, str | None]] = {}
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)

    @property
    def thread(self) -> threading.Thread:
        """The flush thread."""
        return self._thread

    def start(self) -> None:
        """Start the flush thread. Call it once."""
        self._thread.start()

    def stop(self) -> None:
        """Ask the flush thread to end after its current flush. Never waits; idempotent.

        Rows still buffered are not published: a closed session's tables go static.
        """
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
        """Buffer one frame's rows of a channel. Never blocks.

        Rows of a channel no stream was ever routed to are dropped.

        Args:
            channel: The channel.
            rows: The decoded rows, keyed by long-form column; the flusher keeps the list.
        """
        buffer = self.routes.buffer(channel)
        if buffer is not None:
            buffer.put(rows)

    def fail(self, stream_ids: Iterable[int], error: MassiveError) -> None:
        """Ask the flush thread to fail streams. Never blocks.

        Args:
            stream_ids: The streams; one already failed is skipped.
            error: The error their tables fail with, also logged at ERROR and shown in
                `m.streams()`.
        """
        self._control.put(_Fail(tuple(stream_ids), error))

    def drift(self, channel: str, drift: DriftReport) -> None:
        """Ask the flush thread to report a channel's drift for the streams on it. Never blocks.

        Args:
            channel: The channel.
            drift: The frame's drift for the channel.
        """
        self._control.put(_Drift(channel, drift))

    def _run(self) -> None:
        """Flush every interval until `stop()`; a failing flush is logged and the next one runs."""
        while not self._stop.wait(self._interval):
            try:
                self.flush()
            except Exception:
                logger.exception("%s: a flush failed; continuing", self._thread.name)

    def flush(self) -> None:
        """Publish every buffered row, then apply the queued failures and drift.

        Runs under the flush lock, inside the session's execution context, with no other
        library lock held: it calls Deephaven (`to_table`, `where_in`, `add`,
        `publish_failure`) and then the core once (`update_streams`). Does nothing once the
        session's core is gone.
        """
        core = self._core()
        if core is None:
            return
        with self._flush_lock, entered(core.exec_ctx):
            controls = _drain(self._control)
            routes, buffers = self.routes.snapshot()
            updates: dict[int, StreamUpdate] = {}
            for channel, buffer in buffers.items():
                frames = _drain(buffer)
                sinks = routes.get(channel, ())
                if frames and sinks:
                    rows = [row for frame in frames for row in frame]
                    self._publish(core, rows, sinks, updates=updates)
            for item in controls:
                if isinstance(item, _Fail):
                    self._fail(
                        core,
                        [s for s in self.routes.sinks() if s.stream_id in item.stream_ids],
                        item.error,
                    )
                else:
                    self._report_drift(core, routes.get(item.channel, ()), item.drift)
            if updates:
                core.update_streams(updates)

    def _publish(
        self,
        core: SessionCore,
        rows: list[dict[str, object]],
        sinks: tuple[StreamSink, ...],
        *,
        updates: dict[int, StreamUpdate],
    ) -> None:
        """Convert a channel's rows once per dataset and add them to each of its streams.

        Args:
            core: The session's core.
            rows: The channel's rows of this flush, in arrival order.
            sinks: The channel's streams.
            updates: Receives each stream's new published-row total.
        """
        by_plan: dict[ChannelPlan, list[StreamSink]] = {}
        for sink in sinks:
            if not sink.failed:
                by_plan.setdefault(sink.plan, []).append(sink)
        for plan, readers in by_plan.items():
            batch, failures = plan.convert(rows, self._symbols.setdefault(plan, {}))
            for failure in failures:
                self._fail(
                    core,
                    [s for s in readers if s.receives(failure.symbol)],
                    MassiveError(failure.message),
                )
            if not batch.num_rows:
                continue
            table = to_table(batch)
            for sink in readers:
                if sink.failed:
                    continue
                selected = sink.select(table)
                if selected is None or not (added := selected.size):
                    continue
                sink.publisher.add(selected)
                sink.rows_published += added
                updates[sink.stream_id] = StreamUpdate(rows_published=sink.rows_published)

    def _fail(self, core: SessionCore, sinks: Iterable[StreamSink], error: MassiveError) -> None:
        """Fail streams' tables once each, and record the failures.

        Args:
            core: The session's core.
            sinks: The streams; one already failed is skipped.
            error: The error.
        """
        for sink in sinks:
            if sink.failed:
                continue
            sink.failed = True
            logger.error("%s stream %d failed: %s", sink.plan.dataset, sink.stream_id, error)
            sink.publisher.publish_failure(error)
            core.fail_stream(sink.stream_id, str(error))

    @staticmethod
    def _report_drift(core: SessionCore, sinks: tuple[StreamSink, ...], drift: DriftReport) -> None:
        """Log a channel's drift once per dataset and field, and add it to each stream's row.

        Args:
            core: The session's core.
            sinks: The channel's streams.
            drift: The drift.
        """
        for dataset in dict.fromkeys(sink.plan.dataset for sink in sinks):
            core.drift.report(dataset, Transport.WS, drift)
        for sink in sinks:
            core.record_stream_drift(sink.stream_id, Transport.WS.value, drift)


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
        handle: The stream as the session holds it: its id, blink table, scope, host, and path.
        sink: The stream as its connection's flush thread publishes to it.
        watch: The watch of a reactive symbol table, not yet started; None otherwise.
    """

    handle: StreamHandle
    sink: StreamSink
    watch: SymbolWatch | None


def open_stream_tables(
    plan: ChannelPlan,
    schema: Schema,
    symbols: NormalizedSymbols,
    *,
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
    the stream alive; the caller starts it once the stream's topics can receive its changes.
    If anything raises, even `KeyboardInterrupt`, the scope is released (which shuts the blink
    table down and runs `on_shutdown`) and the exception propagates.

    Args:
        plan: How the dataset reads the channel.
        schema: The dataset's merged schema, the blink table's columns.
        symbols: The stream's symbols: a sorted tuple of canonical symbols, `dm.ALL`, or a
            reactive table's `SymbolTable` box.
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
            blink, publisher = table_publisher(
                f"massive.{plan.dataset}#{stream_id}",
                col_defs(schema),
                on_shutdown_callback=on_shutdown,
            )
            watch = None
            if isinstance(symbols, SymbolTable):
                watch = SymbolWatch(plan.asset_class, symbols.table, on_symbols)
                watch.keep_alive_with(blink)
                received: frozenset[str] | None = frozenset()
            elif isinstance(symbols, _All):
                received = None
            else:
                received = frozenset(symbols)
    except BaseException:  # also KeyboardInterrupt: the scope must not leak
        scope.release()
        raise
    handle = StreamHandle(stream_id, blink, scope, host, path)
    return OpenedStream(
        handle, StreamSink(stream_id, plan=plan, publisher=publisher, symbols=received), watch
    )
