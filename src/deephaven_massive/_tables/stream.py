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
  thread with each frame's rows and the instant it arrived) and its control queue (failures and
  drift), and publishes.

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
Java filter and one `add`. Each flush ends with one `SessionCore.update_streams` call carrying every
stream's published-row total.

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
import time
import weakref
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final, Protocol, TypeVar, cast

import pyarrow as pa
import pyarrow.compute as pc
from deephaven import new_table
from deephaven.arrow import to_table
from deephaven.column import string_col
from deephaven.stream.table_publisher import table_publisher

from deephaven_massive._datasets.row_times import TimeRule, time_column
from deephaven_massive._datasets.schema import key_column, rename, required_fields, widen
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
    from deephaven_massive._datasets.schema import ColumnType, Schema
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._datasets.timestamps import TimeUnit
    from deephaven_massive._enums import AssetClass
    from deephaven_massive._massive import DriftReport
    from deephaven_massive._tables.core import SessionCore

__all__ = [
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
    WireType.INT_ARRAY: pa.list_(pa.int32()),
}
"""The Arrow type of each wire type as the WebSocket client delivers its values: an epoch as its
raw integer count (converted by its unit afterwards), an ISO-8601 instant as integer
nanoseconds, a date as a `datetime.date`, and a list as a list of integers."""


class FuturesSessionDates(Protocol):
    """The schedule calendar that corrects streamed futures rows' session dates.

    The WebSocket transport gives a futures connection's flusher one
    (`_ingest/futures_sessions.py`); `ChannelPlan.convert` hands it the rule's dates of every
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
    """

    symbol: str | None
    message: str


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
        asset_class: The dataset's class, whose symbol rules map the wire's symbols.
        channel: The channel, such as `"T"`.
        wire_schema: The Arrow schema of the decoded rows: one field per column of the
            channel's source that the dataset keeps, named by its long-form column.
        columns: Per dataset column, in order: its type, its name, the long-form column of the
            rows that fills it (None for a column the channel does not carry, which is null), and
            the unit of an epoch column (None for any other).
        arrow_schema: The dataset's Arrow schema.
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
    channel: str
    wire_schema: pa.Schema
    columns: tuple[tuple[ColumnType, str, str | None, TimeUnit | None], ...]
    arrow_schema: pa.Schema
    symbol_field: str
    required: frozenset[str]
    times: TimeRule
    time_name: str

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
        _, symbol_field = fields[key_column(spec, "symbol", symbol)]
        # A bars channel serves one timespan, always of one unit.
        timespan = next(span for span, name in spec.channels.items() if name == channel)
        return cls(
            dataset=spec.name,
            asset_class=asset_class,
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
        distinct raw symbol is mapped to its canonical form once (and remembered in
        `symbols`), every epoch column runs through the masked timestamp routine, every
        column is widened to its dataset type, and the library's time columns are filled
        (`row_times.TimeRule.fill`). A row with a timestamp that overflows or lies outside
        1990 to 2100 is dropped and reported; a raw symbol that is not valid for the class
        becomes a null `Symbol` and is logged once.

        Args:
            rows: The decoded rows, keyed by long-form column.
            symbols: The caller's map from raw to canonical symbol, read and extended here;
                one per flush thread and plan, so it needs no lock.
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
        failures: list[tuple[int, str]] = []
        arrays = []
        for column_type, name, field, unit in self.columns:
            if field is None:
                arrays.append(pa.nulls(count, column_type.arrow_type))
                continue
            values = wire.column(field).combine_chunks()
            if name == _SYMBOL:
                values = self._canonical(values, symbols)
            elif unit is not None:
                nanos, bad = masked_instants(unit, values, time_column=name == self.time_name)
                if pc.call_function("any", [bad]).as_py():
                    label = f"{self.dataset}.{name}"
                    for index in pc.call_function("indices_nonzero", [bad]).to_pylist():
                        sent = values[index].as_py()
                        failures.append((index, mismatch_message(label, unit, sent)))
                    failed = bad if failed is None else pc.call_function("or", [failed, bad])
                values = nanos
            arrays.append(widen(values, column_type.arrow_type))
        batch = pa.Table.from_arrays(arrays, schema=self.arrow_schema)
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
                RowFailure(canonical_symbols[index].as_py(), message)
                for index, message in sorted(failures)
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

    def _canonical(self, raw: pa.Array, symbols: dict[str, str | None]) -> pa.Array:
        """Map a column of wire symbols to canonical symbols, one Python call per new symbol.

        Args:
            raw: The rows' symbols as the wire spells them.
            symbols: The raw-to-canonical map, extended with the symbols not seen before.

        Returns:
            The canonical symbols; null where a symbol is not valid for the class.
        """
        distinct = pc.call_function("unique", [raw])
        for value in distinct.to_pylist():
            if value is not None and value not in symbols:
                symbols[value] = _canonical_or_none(self.asset_class, value, self.channel)
        mapped = pa.array(
            [symbols.get(v) if v is not None else None for v in distinct.to_pylist()], pa.string()
        )
        return pc.take(
            mapped, pc.call_function("index_in", [raw], pc.SetLookupOptions(value_set=distinct))
        )


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
    table by the transport; removed when the stream's blink table has shut down. Two locks keep
    it consistent whichever thread reaches it:

    - its leaf lock guards the symbol set, which the connection's loop replaces (`set_symbols`)
      when a reactive symbol table changes;
    - its publish lock serializes `publish` and `fail`, which call Deephaven. Usually only the
      flush thread takes it, but a stream can briefly be reached from two flush threads (when
      the server's verdict moves its connection), and is failed from the caller thread when
      its connection failed while it was opening. The order is the flush lock (on a flush
      thread), then the publish lock, then `registry_lock` (`SessionCore.fail_stream`, which
      `fail` calls); the publish lock is never taken under `registry_lock` or the update-graph
      lock.

    A failed stream publishes nothing more, and is failed once.

    Attributes:
        stream_id: The stream's id.
        plan: How the stream's dataset reads its channel.
        publisher: The stream's publisher.
    """

    __slots__ = (
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
        # Publish-lock state.
        self._publish_lock = threading.Lock()
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

    def fail(self, core: SessionCore, error: MassiveError) -> bool:
        """Fail the stream's table and record the failure, once.

        The failure is recorded first (`SessionCore.fail_stream`) and the table failed after
        (`publish_failure`), so a verb that finds the table failed always finds the failure
        recorded too, and names it. Logged at ERROR. Never call it on an update-graph thread.

        Args:
            core: The session's core.
            error: The error the table fails with.

        Returns:
            False when the stream had already failed, so nothing was done.
        """
        with self._publish_lock:
            if self.failed:
                return False
            self.failed = True
            logger.error("%s stream %d failed: %s", self.plan.dataset, self.stream_id, error)
            core.fail_stream(self.stream_id, str(error))
            self.publisher.publish_failure(error)
            return True


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
        "_final",
        "_flush_lock",
        "_interval",
        "_sessions",
        "_stop",
        "_symbols",
        "_thread",
        "routes",
    )

    def __init__(
        self,
        core: SessionCore,
        name: str,
        *,
        interval: float,
        sessions: FuturesSessionDates | None = None,
    ) -> None:
        """Create the flusher; nothing runs until `start()`.

        Args:
            core: The session's core, held weakly.
            name: The thread's name, for thread dumps.
            interval: Seconds between flushes (`MassiveConfig.flush_interval`).
            sessions: For a futures connection, the schedule calendar that corrects its rows'
                session dates; None otherwise.
        """
        self._core = weakref.ref(core)
        self._interval = interval
        self._sessions = sessions
        self.routes: Final = _Routes()
        self._control: queue.SimpleQueue[_Fail | _Drift] = queue.SimpleQueue()
        self._flush_lock = threading.Lock()
        self._stop = threading.Event()
        self._final = threading.Event()
        # Flush-lock state: the raw-to-canonical symbol map of each plan.
        self._symbols: dict[ChannelPlan, dict[str, str | None]] = {}
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)

    def start(self) -> None:
        """Start the flush thread. Call it once."""
        self._thread.start()

    def stop(self, *, final: bool = False) -> None:
        """Ask the flush thread to end after its current flush. Never waits; idempotent.

        Args:
            final: True to have the thread flush once more before it ends, so the failures
                queued last (a connection's fatal failure fails every stream) are applied.
                False when the session closes: what is still buffered is not published, and
                no stream is failed, since a closed session's tables go static.
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
        ever routed to are dropped.

        Args:
            channel: The channel.
            rows: The decoded rows, keyed by long-form column; the flusher keeps the list.
        """
        buffer = self.routes.buffer(channel)
        if buffer is not None:
            buffer.put((time.time_ns(), rows))

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
            self._flush_logged()
        if self._final.is_set():
            self._flush_logged()

    def _flush_logged(self) -> None:
        """Flush once, logging a failure at ERROR instead of raising it."""
        try:
            self.flush()
        except Exception:
            logger.exception("%s: a flush failed; continuing", self._thread.name)

    def flush(self) -> None:
        """Apply the queued failures, publish every buffered row, then report the queued drift.

        Failures come first, so a stream that failed gets no row published after its failure
        was reported. Runs under the flush lock, inside the session's execution context, with
        no other library lock held: it calls Deephaven (`to_table`, `where_in`, `add`,
        `publish_failure`) and then the core once (`update_streams`). Does nothing once the
        session's core is gone.
        """
        core = self._core()
        if core is None:
            return
        with self._flush_lock, entered(core.exec_ctx):
            controls = _drain(self._control)
            drained = {channel: _drain(buffer) for channel, buffer in self.routes.buffers()}
            # Routes after the rows: a stream routed meanwhile gets the rows that reached it.
            routes = self.routes.current()
            for item in controls:
                if isinstance(item, _Fail):
                    for sink in self.routes.sinks():
                        if sink.stream_id in item.stream_ids:
                            sink.fail(core, item.error)
            updates: dict[int, StreamUpdate] = {}
            for channel, frames in drained.items():
                sinks = routes.get(channel, ())
                if frames and sinks:
                    rows = [row for _, frame in frames for row in frame]
                    arrived = [at for at, frame in frames for _ in frame]
                    self._publish(core, rows, sinks, arrived=arrived, updates=updates)
            for item in controls:
                if isinstance(item, _Drift):
                    self._report_drift(core, routes.get(item.channel, ()), item.drift)
            if updates:
                core.update_streams(updates)

    def _publish(
        self,
        core: SessionCore,
        rows: list[dict[str, object]],
        sinks: tuple[StreamSink, ...],
        *,
        arrived: list[int],
        updates: dict[int, StreamUpdate],
    ) -> None:
        """Convert a channel's rows once per dataset and add them to each of its streams.

        Args:
            core: The session's core.
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
                self._publish_plan(
                    core, plan, rows, readers=readers, arrived=arrived, updates=updates
                )
            except Exception as error:  # one dataset's failure never costs the others their rows
                logger.exception("%s: could not publish a %s batch", plan.dataset, plan.channel)
                failure = MassiveError(
                    f"{plan.dataset}: could not publish a {plan.channel} batch: {error}"
                )
                for sink in readers:
                    sink.fail(core, failure)

    def _publish_plan(
        self,
        core: SessionCore,
        plan: ChannelPlan,
        rows: list[dict[str, object]],
        *,
        readers: list[StreamSink],
        arrived: list[int],
        updates: dict[int, StreamUpdate],
    ) -> None:
        """Convert a channel's rows for one dataset once and add them to its streams.

        A futures session-date miss (a row the schedules cannot place, which keeps the 17:00
        Chicago rule's date) is shown in the `Error` of every stream that receives its symbol;
        it does not fail the stream.

        Args:
            core: The session's core.
            plan: The dataset's plan for the channel.
            rows: The channel's rows of this flush.
            readers: The dataset's streams on the channel.
            arrived: When each row's frame arrived, epoch nanoseconds, one per row.
            updates: Receives each stream's new published-row total and session-date misses.
        """
        converted = plan.convert(
            rows, self._symbols.setdefault(plan, {}), arrived=arrived, sessions=self._sessions
        )
        for symbol, message in converted.misses:
            for sink in readers:
                if sink.receives(symbol):
                    found = updates.get(sink.stream_id, StreamUpdate())
                    updates[sink.stream_id] = replace(found, error=message)
        for failure in converted.failures:
            error = MassiveError(failure.message)
            for sink in readers:
                if sink.receives(failure.symbol):
                    sink.fail(core, error)
        if not converted.batch.num_rows:
            return
        table = to_table(converted.batch)
        for sink in readers:
            if sink.publish(table, converted):
                found = updates.get(sink.stream_id, StreamUpdate())
                updates[sink.stream_id] = replace(found, rows_published=sink.rows_published)

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
