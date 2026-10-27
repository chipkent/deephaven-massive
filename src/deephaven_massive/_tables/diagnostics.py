"""The live diagnostics tables behind `m.connections()` and `m.streams()`.

Both are keyed Deephaven `input_table`s that the library writes and users read:

- **connections**: one row per WebSocket connection, plus one `Transport=REST` row per resolved
  (asset class, channel family) recording the host decision and its evidence. Keyed by
  `Transport`, `AssetClass`, and `Family`.
- **streams**: one row per stream (a live subscription with its own publisher) and per `poll()`
  table. Keyed by `StreamId`.

The tables live in one session-owned `LivenessScope`, created with the session on the caller
thread and released by `m.close()`. One daemon writer thread per session applies every row: the
rest of the library hands it immutable row snapshots through a queue, so no caller, reaper,
or flush thread ever waits on the update graph for bookkeeping, and nothing is written from a
Deephaven listener callback. The writer applies whatever is queued in one batch per table,
keeping the latest row per key. Each row replaces the whole row for its key; the queue is first
in, first out, and the session queues each snapshot while holding its
`registry_lock`, so rows land in the order the registry changed. Once the writer is stopped,
rows are no longer queued.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass, fields, replace
from enum import StrEnum
from typing import TYPE_CHECKING, Final, TypeAlias

import pyarrow as pa
from deephaven import arrow as dh_arrow
from deephaven import input_table

from deephaven_massive._datasets.schema import Column, ColumnType, Schema, pascal_case
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._errors import MassiveError
from deephaven_massive._tables.columns import col_defs
from deephaven_massive._tables.liveness import QueueWorker, new_scope
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from deephaven.execution_context import ExecutionContext
    from deephaven.table_factory import InputTable

    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._enums import AssetClass, ChannelFamily, Recency, Transport

__all__ = [
    "CONNECTIONS",
    "CONNECTION_KEYS",
    "STREAMS",
    "STREAM_KEYS",
    "ConnectionRow",
    "ConnectionState",
    "Diagnostics",
    "StreamRow",
    "StreamState",
    "StreamUpdate",
    "as_row",
    "describe_symbols",
]

logger = logging.getLogger(__name__)

_SYMBOLS_SHOWN: Final = 20
"""How many symbols the `Symbols` column lists before it truncates."""


class ConnectionState(StrEnum):
    """The `State` of a row of `m.connections()`."""

    RESOLVING = "RESOLVING"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    WAITING_SLOT = "WAITING_SLOT"
    RECONNECTING = "RECONNECTING"
    FAILED = "FAILED"
    CLOSED = "CLOSED"


class StreamState(StrEnum):
    """The `State` of a row of `m.streams()`.

    `RELEASED` means the library holds no result of the stream any more; the stream keeps
    publishing until its blink table dies (a derived table or an open panel may hold it).
    `FAILED` can follow `RUNNING` or `RELEASED`; a failed stream's row keeps `FAILED` when its
    last result is released.
    """

    RUNNING = "RUNNING"
    FAILED = "FAILED"
    RELEASED = "RELEASED"
    CLOSED = "CLOSED"


_S, _I, _L, _T = ColumnType.STRING, ColumnType.INT, ColumnType.LONG, ColumnType.INSTANT

CONNECTIONS: Final = Schema(
    "connections",
    tuple(
        Column(name, column_type)
        for name, column_type in (
            ("Transport", _S),
            ("AssetClass", _S),
            ("Family", _S),
            ("Host", _S),
            ("Path", _S),
            ("Recency", _S),
            ("Evidence", _S),
            ("State", _S),
            ("Topics", _I),
            ("AckedTopics", _I),
            ("PendingTopics", _I),
            ("NoAckTopics", _I),
            ("Reconnects", _I),
            ("LastMessageAt", _T),
            ("NextReconnectAt", _T),
            ("UpdatedAt", _T),
            ("LastError", _S),
        )
    ),
)
"""The columns of `m.connections()`."""

CONNECTION_KEYS: Final = ("Transport", "AssetClass", "Family")
"""The key columns of `m.connections()`."""

STREAMS: Final = Schema(
    "streams",
    tuple(
        Column(name, column_type)
        for name, column_type in (
            ("StreamId", _L),
            ("Dataset", _S),
            ("SymbolCount", _I),
            ("Symbols", _S),
            ("HeldResults", _I),
            ("Deferred", _I),
            ("Transport", _S),
            ("Host", _S),
            ("Path", _S),
            ("RowsPublished", _L),
            ("RequestsPerCycle", _I),
            ("State", _S),
            ("Error", _S),
            ("CreatedAt", _T),
        )
    ),
)
"""The columns of `m.streams()`."""

STREAM_KEYS: Final = ("StreamId",)
"""The key column of `m.streams()`."""


@dataclass(frozen=True, kw_only=True, slots=True)
class ConnectionRow:
    """One row of `m.connections()`. Timestamps are epoch nanoseconds; None is null.

    Attributes:
        transport: `WS` for a connection, `REST` for a host-decision row.
        asset_class: The asset class.
        family: The channel family, `TICK` or `FMV`.
        host: The resolved host.
        path: The cluster path, such as `"/stocks"`.
        recency: `REALTIME`, `DELAYED`, or `UNKNOWN`.
        evidence: The host decision's evidence string.
        state: The connection state.
        topics: Topics desired on the connection.
        acked_topics: Topics the server acknowledged.
        pending_topics: Topics sent and awaiting acknowledgement.
        no_ack_topics: Topics never acknowledged after one re-send.
        reconnects: Reconnects so far.
        last_message_at: When the last frame arrived.
        next_reconnect_at: When the next reconnect attempt is due.
        updated_at: When the row was written.
        last_error: The last error text.
    """

    transport: Transport
    asset_class: AssetClass
    family: ChannelFamily
    host: str | None = None
    path: str | None = None
    recency: Recency | None = None
    evidence: str | None = None
    state: ConnectionState = ConnectionState.RESOLVING
    topics: int = 0
    acked_topics: int = 0
    pending_topics: int = 0
    no_ack_topics: int = 0
    reconnects: int = 0
    last_message_at: int | None = None
    next_reconnect_at: int | None = None
    updated_at: int | None = None
    last_error: str | None = None


@dataclass(frozen=True, kw_only=True, slots=True)
class StreamRow:
    """One row of `m.streams()`. Timestamps are epoch nanoseconds; None is null.

    Attributes:
        stream_id: The stream's identifier (the key).
        dataset: The dataset name, such as `"stocks.trades"`.
        symbol_count: How many symbols the stream covers; null for `dm.ALL` and reactive
            tables until the transport fills it in.
        symbols: A truncated, readable list of the symbols.
        held_results: Results of the stream the library still holds.
        deferred: Options contracts deferred by the contract cap.
        transport: `WS` or `REST`.
        host: The serving host.
        path: The cluster path.
        rows_published: Rows published so far.
        requests_per_cycle: REST requests per `poll()` cycle.
        state: The stream state.
        error: The failure or warning text.
        created_at: When the stream was created.
    """

    stream_id: int
    dataset: str
    symbol_count: int | None
    symbols: str
    held_results: int
    deferred: int = 0
    transport: Transport
    host: str | None = None
    path: str | None = None
    rows_published: int = 0
    requests_per_cycle: int | None = None
    state: StreamState = StreamState.RUNNING
    error: str | None = None
    created_at: int


@dataclass(frozen=True, kw_only=True, slots=True)
class StreamUpdate:
    """A change to one stream's diagnostics row, for `SessionCore.update_streams`.

    Built by the WebSocket transport's flush thread after each flush; every field left as None
    keeps the row's current value. It never changes `State`: only `SessionCore.fail_stream`
    fails a stream, so an `error` here is a warning shown in the `Error` column, and
    `clear_error=True` removes it again (for example once the options contract cap no longer
    defers anything).

    Attributes:
        rows_published: The stream's total rows published so far.
        deferred: Options contracts deferred by the contract cap.
        symbol_count: How many symbols the stream now covers.
        host: The serving host.
        path: The cluster path.
        error: Warning text for the `Error` column.
        clear_error: True to set `Error` back to null; takes precedence over `error`.
    """

    rows_published: int | None = None
    deferred: int | None = None
    symbol_count: int | None = None
    host: str | None = None
    path: str | None = None
    error: str | None = None
    clear_error: bool = False

    def apply(self, row: StreamRow) -> StreamRow:
        """Return `row` with the fields this update sets; the others keep their values.

        Args:
            row: The stream's current row.

        Returns:
            The updated row (a new object; rows are immutable).
        """
        return replace(
            row,
            rows_published=row.rows_published
            if self.rows_published is None
            else self.rows_published,
            deferred=row.deferred if self.deferred is None else self.deferred,
            symbol_count=row.symbol_count if self.symbol_count is None else self.symbol_count,
            host=row.host if self.host is None else self.host,
            path=row.path if self.path is None else self.path,
            error=None if self.clear_error else row.error if self.error is None else self.error,
        )


def as_row(row: ConnectionRow | StreamRow) -> dict[str, object]:
    """Return a row keyed by column name: each field's name in PascalCase.

    Args:
        row: A connection or stream row.

    Returns:
        A `{column: value}` mapping for `CONNECTIONS` or `STREAMS`.
    """
    return {pascal_case(f.name): getattr(row, f.name) for f in fields(row)}


def describe_symbols(symbols: NormalizedSymbols) -> tuple[int | None, str]:
    """Summarize a stream's symbols for the `SymbolCount` and `Symbols` columns.

    Args:
        symbols: The stream's normalized symbols.

    Returns:
        `(count, text)`: for a list, its length and the first twenty symbols joined by
        commas with a `(+N more)` suffix when truncated; for `dm.ALL`, `(None, "ALL")`; for
        a reactive table, `(None, "reactive table")`.
    """
    if isinstance(symbols, _All):
        return None, "ALL"
    if isinstance(symbols, SymbolTable):
        return None, "reactive table"
    shown = ", ".join(symbols[:_SYMBOLS_SHOWN])
    hidden = len(symbols) - _SYMBOLS_SHOWN
    return len(symbols), f"{shown} (+{hidden} more)" if hidden > 0 else shown


_Item: TypeAlias = "ConnectionRow | StreamRow | threading.Event"


class Diagnostics:
    """The session's two diagnostics tables and the single thread that writes them.

    Created by `SessionCore` on the caller thread, inside the session's execution context, so
    the tables live in a session-owned liveness scope. A daemon writer (`QueueWorker`) takes
    row snapshots off its queue in batches: everything queued since it last woke is coalesced
    to the latest row per key and applied with one `InputTable.add` per table, so the queue
    cannot grow without bound when rows arrive faster than update-graph cycles. No other thread
    waits on the update graph for diagnostics. `write` only enqueues and is safe from any
    thread, including one that holds the session's `registry_lock`: its internal lock is a leaf
    held only around a non-blocking `put`. A row that does not fit its schema is logged at ERROR
    and skipped; the rest of its batch is applied.

    `release()` lets the writer apply the rows already queued, release the scope, and exit, and
    waits for it; `stop()` does the same without waiting and without taking any lock, for use
    from a finalizer.
    """

    __slots__ = ("_connections", "_release_lock", "_released", "_scope", "_streams", "_writer")

    def __init__(self, exec_ctx: ExecutionContext, name: str) -> None:
        """Create both input tables inside a new session-owned scope and start the writer.

        If creating a table or starting the writer fails, the scope (with any table already
        created in it) is released and the exception propagates.

        Args:
            exec_ctx: The session's execution context, entered by the writer thread.
            name: The writer thread's name, for thread dumps.
        """
        self._release_lock = threading.Lock()
        self._released = False
        self._scope = new_scope()
        try:
            with self._scope.open():
                self._connections: InputTable = input_table(
                    col_defs(CONNECTIONS), key_cols=list(CONNECTION_KEYS)
                )
                self._streams: InputTable = input_table(
                    col_defs(STREAMS), key_cols=list(STREAM_KEYS)
                )
            self._writer: QueueWorker[_Item] = QueueWorker(
                exec_ctx, name, handle=self._apply, on_exit=self._scope.release
            )
        except BaseException:  # also KeyboardInterrupt: the scope must not leak
            self._scope.release()
            raise

    @property
    def connections(self) -> InputTable:
        """The table behind `m.connections()`."""
        return self._connections

    @property
    def streams(self) -> InputTable:
        """The table behind `m.streams()`."""
        return self._streams

    @property
    def thread(self) -> threading.Thread:
        """The writer thread."""
        return self._writer.thread

    def write(self, row: ConnectionRow | StreamRow) -> None:
        """Queue one row's full current state; it replaces the row with the same key.

        Never blocks. After `release()` the row is not queued at all.

        Args:
            row: An immutable snapshot of the row.
        """
        with self._release_lock:
            if not self._released:
                self._writer.put(row)

    def barrier(self) -> threading.Event:
        """Return an event that is set once every row queued before this call is applied.

        Only for a live session's diagnostics, whose writer is running (tests use it to wait
        for rows). After `release()` the event comes back already set. After `stop()` (the
        finalizer of a collected session) the writer may already have exited, so the event
        might never be set: do not call it then.

        Returns:
            The event; already set when the writer has been released.
        """
        event = threading.Event()
        with self._release_lock:
            if self._released:
                event.set()
            else:
                self._writer.put(event)
        return event

    def stop(self) -> None:
        """Ask the writer to apply the queued rows, release the tables, and exit; no wait.

        Takes no lock, so the session's finalizer can call it from garbage collection.
        """
        self._writer.stop()

    def release(self) -> None:
        """Apply the queued rows, release the tables' scope, and end the writer. Idempotent.

        Blocks until the writer has finished, so it must not run on an update-graph thread.
        """
        with self._release_lock:
            self._released = True
        self._writer.stop()
        self._writer.thread.join()

    def _apply(self, items: list[_Item]) -> None:
        """Apply one batch: the latest row per key, one `add` per table, then the barriers.

        Args:
            items: Rows and barrier events, in queue order.
        """
        streams: dict[int, StreamRow] = {}
        connections: dict[tuple[str, str, str], ConnectionRow] = {}
        barriers: list[threading.Event] = []
        for item in items:
            if isinstance(item, threading.Event):
                barriers.append(item)
            elif isinstance(item, StreamRow):
                streams[item.stream_id] = item
            else:
                connections[item.transport, item.asset_class, item.family] = item
        try:
            for table, schema, rows in (
                (self._streams, STREAMS, list(streams.values())),
                (self._connections, CONNECTIONS, list(connections.values())),
            ):
                try:  # one table's failure never costs the other its rows
                    self._add(table, schema, rows)
                except Exception:
                    logger.exception("diagnostics: failed to update the %s table", schema.name)
        finally:
            for barrier in barriers:
                barrier.set()

    @staticmethod
    def _add(
        table: InputTable, schema: Schema, rows: list[ConnectionRow] | list[StreamRow]
    ) -> None:
        """Add rows to a table with one conversion and one `add`, skipping rows that do not fit.

        The whole batch is converted at once; only if that fails is each row converted on its
        own, so the rows that fit are still added and each one that does not is logged.

        Args:
            table: The target table.
            schema: Its schema.
            rows: The rows, at most one per key.
        """
        if not rows:
            return
        dicts = [as_row(row) for row in rows]
        try:
            batch = schema.to_arrow(dicts)
        except MassiveError:  # a row does not fit: find it, keep the others
            batches = []
            for one in dicts:
                try:
                    batches.append(schema.to_arrow([one]))
                except Exception:
                    logger.exception("diagnostics: failed to write a %s row", schema.name)
            if not batches:
                return
            batch = pa.concat_tables(batches)
        table.add(dh_arrow.to_table(batch))
