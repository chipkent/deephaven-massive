"""The live diagnostics tables behind `m.connections()` and `m.streams()` (DESIGN § 17).

Both are keyed Deephaven `input_table`s that the library writes and users read:

- **connections**: one row per WebSocket connection, plus one `Transport=REST` row per resolved
  (asset class, channel family) recording the host decision and its evidence. Keyed by
  `Transport`, `AssetClass`, and `Family`.
- **streams**: one row per stream (a live subscription with its own publisher) and per `poll()`
  table. Keyed by `StreamId`.

The tables live in one session-owned `LivenessScope`, created with the session on the caller
thread and released by `m.close()`. They are written only by library code, never from a
Deephaven listener callback (which runs on an update-graph thread). Each write replaces the
whole row for its key with the current state, and writes are serialized by a lock that is never
taken while the session's registry lock is held, so the last write always carries the latest
state. Writes after the scope is released are ignored.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Final

from deephaven import arrow as dh_arrow
from deephaven import input_table

from deephaven_massive._liveness import new_scope
from deephaven_massive._schema import Column, ColumnType, Schema
from deephaven_massive._symbols import SymbolTable
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from deephaven.table_factory import InputTable

    from deephaven_massive._symbols import NormalizedSymbols

__all__ = [
    "CONNECTIONS",
    "STREAMS",
    "ConnectionRow",
    "ConnectionState",
    "Diagnostics",
    "StreamRow",
    "StreamState",
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


@dataclass(frozen=True, kw_only=True, slots=True)
class ConnectionRow:
    """One row of `m.connections()`. Timestamps are epoch nanoseconds; None is null.

    Attributes:
        transport: `"WS"` for a connection, `"REST"` for a host-decision row.
        asset_class: The asset class value, such as `"stocks"`.
        family: `"TICK"` or `"FMV"`.
        host: The resolved host.
        path: The cluster path, such as `"/stocks"`.
        recency: `"REALTIME"`, `"DELAYED"`, or `"UNKNOWN"`.
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

    transport: str
    asset_class: str
    family: str
    host: str | None = None
    path: str | None = None
    recency: str | None = None
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

    def as_row(self) -> dict[str, object]:
        """Return the row keyed by column name.

        Returns:
            A `{column: value}` mapping for `CONNECTIONS`.
        """
        return {
            "Transport": self.transport,
            "AssetClass": self.asset_class,
            "Family": self.family,
            "Host": self.host,
            "Path": self.path,
            "Recency": self.recency,
            "Evidence": self.evidence,
            "State": self.state.value,
            "Topics": self.topics,
            "AckedTopics": self.acked_topics,
            "PendingTopics": self.pending_topics,
            "NoAckTopics": self.no_ack_topics,
            "Reconnects": self.reconnects,
            "LastMessageAt": self.last_message_at,
            "NextReconnectAt": self.next_reconnect_at,
            "UpdatedAt": self.updated_at,
            "LastError": self.last_error,
        }


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
        transport: `"WS"` or `"REST"`.
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
    transport: str
    host: str | None = None
    path: str | None = None
    rows_published: int = 0
    requests_per_cycle: int | None = None
    state: StreamState = StreamState.RUNNING
    error: str | None = None
    created_at: int

    def as_row(self) -> dict[str, object]:
        """Return the row keyed by column name.

        Returns:
            A `{column: value}` mapping for `STREAMS`.
        """
        return {
            "StreamId": self.stream_id,
            "Dataset": self.dataset,
            "SymbolCount": self.symbol_count,
            "Symbols": self.symbols,
            "HeldResults": self.held_results,
            "Deferred": self.deferred,
            "Transport": self.transport,
            "Host": self.host,
            "Path": self.path,
            "RowsPublished": self.rows_published,
            "RequestsPerCycle": self.requests_per_cycle,
            "State": self.state.value,
            "Error": self.error,
            "CreatedAt": self.created_at,
        }


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


class Diagnostics:
    """The session's two diagnostics tables and the only code that writes them.

    Created by `MassiveSession.__init__` on the caller thread, inside the session's execution
    context; the tables live in a session-owned liveness scope until `release()`. Writes are
    synchronous `input_table.add` calls (about a millisecond: the input table requests an
    update-graph refresh) serialized by an internal lock. Callers must not hold the session's
    registry lock, and must not be on an update-graph thread, while writing. Thread-safe
    otherwise.
    """

    __slots__ = ("_connections", "_lock", "_released", "_scope", "_streams")

    def __init__(self) -> None:
        """Create both input tables inside a new session-owned scope."""
        self._lock = threading.Lock()
        self._released = False
        self._scope = new_scope()
        with self._scope.open():
            self._connections: InputTable = input_table(
                CONNECTIONS.col_defs(), key_cols=list(CONNECTION_KEYS)
            )
            self._streams: InputTable = input_table(STREAMS.col_defs(), key_cols=list(STREAM_KEYS))

    @property
    def connections(self) -> InputTable:
        """The table behind `m.connections()`."""
        return self._connections

    @property
    def streams(self) -> InputTable:
        """The table behind `m.streams()`."""
        return self._streams

    def write_connection(self, row: ConnectionRow) -> None:
        """Insert or replace one connection row.

        Args:
            row: The row's full current state.
        """
        self._write(self._connections, CONNECTIONS, row.as_row())

    def write_stream(self, row: StreamRow) -> None:
        """Insert or replace one stream row.

        Args:
            row: The row's full current state.
        """
        self._write(self._streams, STREAMS, row.as_row())

    def release(self) -> None:
        """Release the tables' scope (`m.close()`); later writes are ignored. Idempotent."""
        with self._lock:
            if self._released:
                return
            self._released = True
            self._scope.release()

    def _write(self, table: InputTable, schema: Schema, row: dict[str, object]) -> None:
        """Add one row to an input table, unless the tables were released.

        Args:
            table: The target table.
            schema: Its schema.
            row: The row keyed by column name.
        """
        static = dh_arrow.to_table(schema.to_arrow([row]))
        with self._lock:
            if self._released:
                logger.debug("diagnostics released; dropping %s row", schema.name)
                return
            table.add(static)
