"""The live diagnostics tables behind `m.connections()` and `m.streams()` (DESIGN § 17).

Both are keyed Deephaven `input_table`s that the library writes and users read:

- **connections**: one row per WebSocket connection, plus one `Transport=REST` row per resolved
  (asset class, channel family) recording the host decision and its evidence. Keyed by
  `Transport`, `AssetClass`, and `Family`.
- **streams**: one row per stream (a live subscription with its own publisher) and per `poll()`
  table. Keyed by `StreamId`.

The tables live in one session-owned `LivenessScope`, created with the session on the caller
thread and released by `m.close()`. One daemon writer thread per session applies every row: the
rest of the library hands it immutable row snapshots through a `queue.SimpleQueue`, so no
caller, reaper, or flush thread ever waits on the update graph for bookkeeping, and nothing is
written from a Deephaven listener callback. Each row replaces the whole row for its key; the
queue is first in, first out, and the session queues each snapshot while holding its
`registry_lock`, so rows land in the order the registry changed. Once the writer is stopped,
rows are no longer queued.
"""

from __future__ import annotations

import logging
import queue
import threading
from dataclasses import dataclass, fields
from enum import StrEnum
from typing import TYPE_CHECKING, Final, TypeAlias

from deephaven import arrow as dh_arrow
from deephaven import input_table

from deephaven_massive._liveness import entered, new_scope
from deephaven_massive._schema import Column, ColumnType, Schema, pascal_case
from deephaven_massive._symbols import SymbolTable
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from deephaven.execution_context import ExecutionContext
    from deephaven.table_factory import InputTable

    from deephaven_massive._enums import AssetClass, ChannelFamily, Recency, Transport
    from deephaven_massive._symbols import NormalizedSymbols

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


_Item: TypeAlias = "ConnectionRow | StreamRow | threading.Event | None"


class Diagnostics:
    """The session's two diagnostics tables and the single thread that writes them.

    Created by `SessionCore` on the caller thread, inside the session's execution context, so
    the tables live in a session-owned liveness scope. A daemon writer thread enters the same
    execution context, takes row snapshots off a `queue.SimpleQueue` in order, and applies each
    with `InputTable.add`; no other thread waits on the update graph for diagnostics. `write`
    only enqueues and is safe from any thread, including one that holds the session's
    `registry_lock`: its internal lock is a leaf held only around a non-blocking `put`. A failed
    write is logged at ERROR and the thread carries on.

    `release()` lets the writer apply the rows already queued, release the scope, and exit, and
    waits for it; `stop()` does the same without waiting, for use from a finalizer.
    """

    __slots__ = (
        "_connections",
        "_exec_ctx",
        "_queue",
        "_release_lock",
        "_released",
        "_scope",
        "_streams",
        "_thread",
    )

    def __init__(self, exec_ctx: ExecutionContext, name: str) -> None:
        """Create both input tables inside a new session-owned scope and start the writer.

        Args:
            exec_ctx: The session's execution context, entered by the writer thread.
            name: The writer thread's name, for thread dumps.
        """
        self._exec_ctx = exec_ctx
        # Reentrant: the session's finalizer calls `stop()` from garbage collection, which can
        # run on a thread that is already inside `write()` holding this lock.
        self._release_lock = threading.RLock()
        self._released = False
        self._scope = new_scope()
        with self._scope.open():
            self._connections: InputTable = input_table(
                CONNECTIONS.col_defs(), key_cols=list(CONNECTION_KEYS)
            )
            self._streams: InputTable = input_table(STREAMS.col_defs(), key_cols=list(STREAM_KEYS))
        self._queue: queue.SimpleQueue[_Item] = queue.SimpleQueue()
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)
        self._thread.start()

    @property
    def connections(self) -> InputTable:
        """The table behind `m.connections()`."""
        return self._connections

    @property
    def streams(self) -> InputTable:
        """The table behind `m.streams()`."""
        return self._streams

    def write(self, row: ConnectionRow | StreamRow) -> None:
        """Queue one row's full current state; it replaces the row with the same key.

        Never blocks. After `release()` or `stop()` the row is not queued at all.

        Args:
            row: An immutable snapshot of the row.
        """
        with self._release_lock:
            if not self._released:
                self._queue.put(row)

    def barrier(self) -> threading.Event:
        """Return an event that is set once every row queued before this call is applied.

        Returns:
            The event; already set when the writer has been stopped.
        """
        event = threading.Event()
        with self._release_lock:
            if self._released:
                event.set()
            else:
                self._queue.put(event)
        return event

    def stop(self) -> bool:
        """Ask the writer to apply the queued rows, release the tables, and exit; no wait.

        Idempotent; used by the session's finalizer.

        Returns:
            True if this call stopped the writer, False if it was already stopping.
        """
        with self._release_lock:
            if self._released:
                return False
            self._released = True
            self._queue.put(None)
        return True

    def release(self) -> None:
        """Apply the queued rows, release the tables' scope, and end the writer. Idempotent.

        Blocks until the writer has finished, so it must not run on an update-graph thread.
        """
        self.stop()
        self._thread.join()

    def _run(self) -> None:
        """Writer loop: apply rows and set barriers until `stop()`, then release the scope."""
        with entered(self._exec_ctx):
            while self._apply_next():
                pass
            self._scope.release()

    def _apply_next(self) -> bool:
        """Take one item off the queue: apply a row or set a barrier.

        The item is a local of this call, so nothing stays referenced while the thread blocks
        for the next one.

        Returns:
            False when the item was the stop sentinel, True otherwise.
        """
        item = self._queue.get()
        if item is None:
            return False
        if isinstance(item, threading.Event):
            item.set()
        else:
            self._apply(item)
        return True

    def _apply(self, row: ConnectionRow | StreamRow) -> None:
        """Add one row to its table, logging a failure instead of raising.

        Args:
            row: The row to write.
        """
        stream = isinstance(row, StreamRow)
        table, schema = (self._streams, STREAMS) if stream else (self._connections, CONNECTIONS)
        try:
            table.add(dh_arrow.to_table(schema.to_arrow([as_row(row)])))
        except Exception:
            logger.exception("diagnostics: failed to write a %s row", schema.name)
