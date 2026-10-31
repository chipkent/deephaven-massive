"""The live diagnostics tables behind `m.connections()`, `m.streams()`, and `m.rate_limits()`.

All three are keyed Deephaven `input_table`s that the library writes and users read:

- **connections**: one row per WebSocket connection, plus one `Transport=REST` row per resolved
  (asset class, channel family) recording the host decision and its evidence. Keyed by
  `Transport`, `AssetClass`, and `Family`.
- **streams**: one row per stream (a live subscription with its own publisher), per `poll()`
  table, and per background read (`wait=False`), told apart by `Kind`, with the `repr` of the
  query that opened it in `Query`. Keyed by `StreamId`.
- **rate_limits**: one row per REST rate-limit bucket: each Massive subscription, and `other`
  for the datasets Massive assigns to none. Keyed by `Bucket`. Its rows are not queued:
  the bucket state lives in a governor that several sessions share, so the writer pulls a fresh
  snapshot through its pull (`RateLimitPull`) whenever it was told the state changed
  (`rate_limits_changed`), and a row can never be overtaken by an older one. The writer also
  pulls again when the state changes with time alone (a pause ends, a slot frees): it waits on
  its queue no longer than until the moment the pull names.

All of them spell a transport as every other text a user reads does (`TRANSPORT_LABELS`:
`WebSocket`, `REST`, `flat files`), in `Transport` and in a stream's `SchemaDrift` entries; the
rows hold the `Transport` member, and `as_row` writes its label. A stream's `Query` is cut to
`QUERY_SHOWN` characters (`shown_query`), as `Symbols` is cut to its first symbols, so a query
over a long list stays readable.

The tables live in one session-owned `LivenessScope`, created with the session on the caller
thread and released by `m.close()`. Users never get the tables themselves: `m.connections()` and
`m.streams()` return a view of one, tracked like a live result (`SessionCore.diagnostics_view`),
which keeps the session alive and current while it is held and keeps the table, with its final
rows, after the scope is released. One daemon writer thread per session applies every row: the
rest of the library hands it immutable row snapshots through a queue, so no caller, reaper,
or flush thread ever waits on the update graph for bookkeeping, and nothing is written from a
Deephaven listener callback. The writer applies whatever is queued in one batch per table,
keeping the latest row per key. Each row replaces the whole row for its key; the queue is first
in, first out, and the session queues each snapshot while holding its
`registry_lock`, so rows land in the order the registry changed. Once the writer is stopped,
rows are no longer queued.

The module also holds the session's `DriftRegistry`, the policy half of runtime schema drift.
The wire package detects drift (each decoder returns a `DriftReport` of the fields a payload had
beyond its documented schema, and the documented fields it lacked); every transport hands those
reports to the registry, which logs one WARNING per (dataset, transport, field) and keeps the
`SchemaDrift` text of each stream and connection, a column of both tables. Drift never fails a
read: a missing key, a value that does not convert, or a timestamp out of bounds fails loudly
where it is found, and never reaches the registry.
"""

from __future__ import annotations

import dataclasses
import functools
import logging
import re
import threading
import time
from dataclasses import dataclass, fields, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Final, TypeAlias, cast

import pyarrow as pa
from deephaven import arrow as dh_arrow
from deephaven import input_table

from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._datasets.schema import Column, ColumnType, Schema
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._errors import LIBRARY_BUG, MassiveError
from deephaven_massive._massive import Transport
from deephaven_massive._tables.columns import col_defs
from deephaven_massive._tables.liveness import QueueWorker, new_scope, release_scope
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable, Sequence

    from deephaven.execution_context import ExecutionContext
    from deephaven.table_factory import InputTable

    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._enums import AssetClass, ChannelFamily, Recency
    from deephaven_massive._massive import DriftReport

__all__ = [
    "CONNECTIONS",
    "CONNECTION_KEYS",
    "QUERY_SHOWN",
    "RATE_LIMITS",
    "RATE_LIMIT_KEYS",
    "SCHEMA_DRIFT_SHOWN",
    "STREAMS",
    "STREAM_KEYS",
    "ConnectionRow",
    "ConnectionState",
    "Diagnostics",
    "DriftRegistry",
    "LimitSource",
    "RateLimitPull",
    "RateLimitRow",
    "RateLimitState",
    "StreamKind",
    "StreamRow",
    "StreamState",
    "StreamUpdate",
    "as_row",
    "describe_symbols",
    "pascal_case",
    "shown_query",
    "utc_text",
]

logger = logging.getLogger(__name__)

_SYMBOLS_SHOWN: Final = 20
"""How many symbols the `Symbols` column lists before it truncates."""

QUERY_SHOWN: Final = 200
"""The longest `Query` text, in characters; a longer one is cut and ends in `…`."""

SCHEMA_DRIFT_SHOWN: Final = 200
"""The longest `SchemaDrift` text, in characters; a longer one is cut and ends in `…`."""


class ConnectionState(StrEnum):
    """The `State` of a row of `m.connections()`."""

    RESOLVING = "RESOLVING"
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    WAITING_SLOT = "WAITING_SLOT"
    RECONNECTING = "RECONNECTING"
    FAILED = "FAILED"
    CLOSED = "CLOSED"


class StreamKind(StrEnum):
    """The `Kind` of a row of `m.streams()`: what feeds the table the row describes.

    Attributes:
        STREAM: A WebSocket stream, shared by every live verb of an equal query.
        POLL: A `poll()` table, which re-reads a REST dataset on an interval.
        READ: A background read, `fetch(wait=False)` or `history(..., wait=False)`, whose
            table fills once, when its download completes.
    """

    STREAM = "stream"
    POLL = "poll"
    READ = "read"


class StreamState(StrEnum):
    """The `State` of a row of `m.streams()`.

    `RUNNING` is a stream or `poll()` table that publishes. A background read is `LOADING`
    while it downloads and `DONE` once its rows have landed; `DONE` is final, like `FAILED`.
    `RELEASED` means the library holds no result of the stream any more; the stream keeps
    publishing until its blink table dies (a derived table or an open panel may hold it), and a
    read released while loading keeps loading for such a table, becoming `DONE` if its rows
    land. `FAILED` can follow `RUNNING`, `LOADING`, or `RELEASED`; a failed stream's row keeps
    `FAILED` when its last result is released.
    """

    RUNNING = "RUNNING"
    LOADING = "LOADING"
    DONE = "DONE"
    FAILED = "FAILED"
    RELEASED = "RELEASED"
    CLOSED = "CLOSED"


_STOPPED: Final = frozenset({StreamState.FAILED, StreamState.CLOSED})
"""The states whose `Error` names why the stream stopped, which no later update replaces."""
_NOT_A_COLUMN: Final = "not_a_column"
"""The metadata key of a row field that `as_row` leaves out of the table."""


class RateLimitState(StrEnum):
    """The `State` of a row of `m.rate_limits()`: what a new request of the bucket meets now.

    `READY`: it starts at once. `PACED`: it waits behind the requests already queued for a slot
    of the bucket's per-minute limit. `PAUSED`: it waits for the pause Massive's latest 429
    asked for, unless its read will not wait until `PausedUntil` and fails with status 429.
    """

    READY = "READY"
    PACED = "PACED"
    PAUSED = "PAUSED"


class LimitSource(StrEnum):
    """Why a bucket of `m.rate_limits()` is paced: the `LimitSource` column.

    `DECLARED`: a session that shares the bucket names the subscription in
    `MassiveConfig.rest_basic_subscriptions`. `LEARNED`: Massive refused one of its requests
    for exceeding the maximum requests per minute, the mark of the Basic tier.
    """

    DECLARED = "DECLARED"
    LEARNED = "LEARNED"


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
            ("BufferedRows", _L),
            ("Reconnects", _I),
            ("LastMessageAt", _T),
            ("NextReconnectAt", _T),
            ("UpdatedAt", _T),
            ("LastError", _S),
            ("SchemaDrift", _S),
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
            ("Kind", _S),
            ("Dataset", _S),
            ("Query", _S),
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
            ("SchemaDrift", _S),
        )
    ),
)
"""The columns of `m.streams()`."""

STREAM_KEYS: Final = ("StreamId",)
"""The key column of `m.streams()`."""

RATE_LIMITS: Final = Schema(
    "rate_limits",
    tuple(
        Column(name, column_type)
        for name, column_type in (
            ("Bucket", _S),
            ("State", _S),
            ("RequestsPerMinute", _I),
            ("LimitSource", _S),
            ("InFlight", _I),
            ("Waiting", _I),
            ("RecentRequests", _I),
            ("Attempts", _L),
            ("Refusals", _L),
            ("PausedUntil", _T),
            ("LastRefusal", _S),
            ("UpdatedAt", _T),
        )
    ),
)
"""The columns of `m.rate_limits()`."""

RATE_LIMIT_KEYS: Final = ("Bucket",)
"""The key column of `m.rate_limits()`."""


@dataclass(frozen=True, kw_only=True, slots=True)
class ConnectionRow:
    """One row of `m.connections()`. Timestamps are epoch nanoseconds; None is null.

    Attributes:
        transport: `Transport.WS` for a connection, `Transport.REST` for a host-decision row;
            the table shows its label, `WebSocket` or `REST`.
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
        buffered_rows: Rows the connection received that no flush has taken yet; every family
            row of a WebSocket connection shows the connection's count. Rewritten at least
            once a second while it is above zero.
        reconnects: Reconnects so far.
        last_message_at: When the last frame arrived.
        next_reconnect_at: When the next reconnect attempt is due.
        updated_at: When the row was written.
        last_error: The last error text.
        schema_drift: The drift seen on the connection, as `DriftRegistry.record` writes it
            (`channel:field+` for an extra field, `channel:field-` for a missing one); null
            while there is none.
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
    buffered_rows: int = 0
    reconnects: int = 0
    last_message_at: int | None = None
    next_reconnect_at: int | None = None
    updated_at: int | None = None
    last_error: str | None = None
    schema_drift: str | None = None


@dataclass(frozen=True, kw_only=True, slots=True)
class StreamRow:
    """One row of `m.streams()`. Timestamps are epoch nanoseconds; None is null.

    Attributes:
        stream_id: The stream's identifier (the key); a background read's is its call id.
        kind: What feeds the table: a stream, a `poll()` table, or a background read.
        dataset: The dataset name, such as `"stocks.trades"`.
        query: The `repr` of the query whose verb opened the stream, `poll()`, or read, such as
            `"m.stocks.trades('AAPL')"`, which tells two streams
            of one dataset apart; cut to `QUERY_SHOWN` characters (`shown_query`).
        symbol_count: How many symbols the stream covers; null for `dm.ALL` and reactive
            tables until the transport fills it in.
        symbols: A truncated, readable list of the symbols.
        held_results: Results of the stream the library still holds.
        deferred: Options contracts deferred by the contract cap.
        transport: `Transport.WS` for a stream, `Transport.REST` for a `poll()`, and for a
            background read the source it reads, `Transport.REST` or `Transport.FLAT_FILES`;
            the table shows its label, `WebSocket`, `REST`, or `flat files`.
        host: The serving host; null for a background read.
        path: The cluster path; null for a background read.
        rows_published: Rows published so far; a background read's, once its rows land.
        requests_per_cycle: REST requests per `poll()` cycle.
        state: The stream state.
        error: The failure, or the transport's warning (a wait for a connection slot, deferred
            options contracts, a poll cycle's error).
        created_at: When the stream was created.
        schema_drift: The stream's drift, as `DriftRegistry.record` writes it
            (`transport:field+` for an extra field, `transport:field-` for a missing one);
            null while there is none. Separate from `error`, so drift never hides a failure.
        skipped: The symbol-table values the stream or poll skips because they are not
            Massive's tickers (`skipped_rows_text` in `_tables/watchlist.py`); null while none
            is. Not a column of its own: the `Error` column shows it after `error` while the
            stream runs (`as_row`), so neither source of a warning hides or clears the other.
    """

    stream_id: int
    kind: StreamKind
    dataset: str
    query: str
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
    schema_drift: str | None = None
    skipped: str | None = dataclasses.field(default=None, metadata={_NOT_A_COLUMN: True})


@dataclass(frozen=True, kw_only=True, slots=True)
class StreamUpdate:
    """A change to one stream's diagnostics row, for `SessionCore.update_streams`.

    Built by the WebSocket transport's flush thread after each flush, and by a `poll()` table's
    poller after each cycle; every field left as None keeps the row's current value. It never
    changes `State`: only `SessionCore.fail_stream` fails a stream, so an `error` here is a warning
    shown in the `Error` column, and `clear_error=True` removes it again (for example once the
    options contract cap no longer defers anything).

    Attributes:
        rows_published: The stream's total rows published so far.
        requests_per_cycle: The REST requests of the latest `poll()` cycle.
        deferred: Options contracts deferred by the contract cap.
        symbol_count: How many symbols the stream now covers.
        symbols: The readable, truncated list of those symbols, as `describe_symbols` writes it.
        host: The serving host.
        path: The cluster path.
        error: The transport's warning text (`StreamRow.error`).
        clear_error: True to set the transport's warning back to null; takes precedence over
            `error`.
        skipped: The skipped symbol-table values' text (`StreamRow.skipped`).
        clear_skipped: True to set that text back to null; takes precedence over `skipped`.

    `SchemaDrift` is not set here: `SessionCore.record_stream_drift` records a stream's drift
    and writes its row in one step, so concurrent reports cannot leave an older text behind.
    """

    rows_published: int | None = None
    requests_per_cycle: int | None = None
    deferred: int | None = None
    symbol_count: int | None = None
    symbols: str | None = None
    host: str | None = None
    path: str | None = None
    error: str | None = None
    clear_error: bool = False
    skipped: str | None = None
    clear_skipped: bool = False

    def apply(self, row: StreamRow) -> StreamRow:
        """Return `row` with the fields this update sets; the others keep their values.

        The `Error` of a `FAILED` or `CLOSED` row names why the stream stopped, so `error` and
        `clear_error` leave it alone: a flush or a poll cycle that races the stop never replaces
        the cause.

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
            requests_per_cycle=row.requests_per_cycle
            if self.requests_per_cycle is None
            else self.requests_per_cycle,
            deferred=row.deferred if self.deferred is None else self.deferred,
            symbol_count=row.symbol_count if self.symbol_count is None else self.symbol_count,
            symbols=row.symbols if self.symbols is None else self.symbols,
            host=row.host if self.host is None else self.host,
            path=row.path if self.path is None else self.path,
            error=self._error(row),
            skipped=None
            if self.clear_skipped
            else (row.skipped if self.skipped is None else self.skipped),
        )

    def _error(self, row: StreamRow) -> str | None:
        """Return the row's new `Error`.

        Args:
            row: The stream's current row.

        Returns:
            The current text for a stopped stream or an update that sets none, null for
            `clear_error`, and the update's text otherwise.
        """
        if row.state in _STOPPED or (self.error is None and not self.clear_error):
            return row.error
        return None if self.clear_error else self.error


@dataclass(frozen=True, kw_only=True, slots=True)
class RateLimitRow:
    """One row of `m.rate_limits()`. Timestamps are epoch nanoseconds; None is null.

    Every session that shares a governor (the same API key and REST base URL) shows the same
    rows, since Massive counts the requests of all of them together.

    Attributes:
        bucket: The bucket: a `dm.Subscription` value, or `other` for reference data and the
            other datasets Massive assigns to no subscription, which it counts on their own.
        state: What a new request meets now.
        requests_per_minute: The pace, 5 for a Basic subscription; null while unpaced.
        limit_source: Why it is paced; null while unpaced.
        in_flight: Requests sent and not yet answered.
        waiting: Requests waiting for a slot or for the end of a pause.
        recent_requests: Requests in flight or answered within the last 61 seconds: the
            slots a paced bucket counts against its limit.
        attempts: Attempts started since the governor was created; every retry and refused
            attempt counts, so one request may count several times.
        refusals: HTTP 429 responses since the governor was created.
        paused_until: When the current pause ends; null without one.
        last_refusal: The latest 429's message, with its HTTP status, path, and request id.
        updated_at: When the row was written.
    """

    bucket: str
    state: RateLimitState
    requests_per_minute: int | None = None
    limit_source: LimitSource | None = None
    in_flight: int = 0
    waiting: int = 0
    recent_requests: int = 0
    attempts: int = 0
    refusals: int = 0
    paused_until: int | None = None
    last_refusal: str | None = None
    updated_at: int


def as_row(row: ConnectionRow | StreamRow | RateLimitRow) -> dict[str, object]:
    """Return a row keyed by column name: each field's name in PascalCase.

    A `Transport` value becomes its user-facing label (`TRANSPORT_LABELS`), so the tables spell
    transports as `columns()` does: `Transport.WS` is shown as `WebSocket`. A stream's `Error`
    joins its two warning sources while it runs, the transport's warning and the skipped
    symbol-table values, with `"; "`; a stopped stream's shows only why it stopped.

    Args:
        row: A connection, stream, or rate-limit row.

    Returns:
        A `{column: value}` mapping for `CONNECTIONS`, `STREAMS`, or `RATE_LIMITS`.
    """
    shown = {
        pascal_case(f.name): _shown(getattr(row, f.name))
        for f in fields(row)
        if not f.metadata.get(_NOT_A_COLUMN)
    }
    if isinstance(row, StreamRow) and row.state not in _STOPPED and row.skipped is not None:
        shown["Error"] = "; ".join(text for text in (row.error, row.skipped) if text is not None)
    return shown


def _shown(value: object) -> object:
    """Return a row value as its table shows it: a `Transport` as its label, else unchanged.

    Args:
        value: One field of a row.

    Returns:
        The value to write.
    """
    return TRANSPORT_LABELS[value] if isinstance(value, Transport) else value


def shown_query(query: str) -> str:
    """Return a query's `repr` as the `Query` column shows it: cut to `QUERY_SHOWN` characters.

    Args:
        query: The query's `repr`, such as `"m.stocks.news(start='2025-01-02')"`.

    Returns:
        The text, or its first `QUERY_SHOWN - 1` characters and `…` when it is longer.
    """
    return query if len(query) <= QUERY_SHOWN else query[: QUERY_SHOWN - 1] + "…"


def utc_text(seconds: float) -> str:
    """Return an instant as the UTC text an `Error` note or a failure message shows.

    Args:
        seconds: The instant, in seconds since the epoch.

    Returns:
        The text, to the second, such as `"2026-10-30T14:03:05Z"`.
    """
    return datetime.fromtimestamp(seconds, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def _drift_context(drift: DriftReport, field: str, *, where: str, symbol: str | None) -> str:
    """Return where a drifted field was seen, for its WARNING.

    Args:
        drift: The payload's drift, with its samples.
        field: The drifted wire key.
        where: Where the payload came from.
        symbol: The payload's symbol when its sample has none, or None.

    Returns:
        `where`, then `; symbol <symbol>` when one is known and `; first value <value>` for an
        extra field's sample, such as `"wss://h/stocks channel T; symbol AAPL; first value 1"`.
    """
    sample = drift.samples.get(field)
    parts = [where]
    shown = sample.symbol if sample is not None and sample.symbol is not None else symbol
    if shown is not None:
        parts.append(f"symbol {shown}")
    if sample is not None and sample.value is not None:
        parts.append(f"first value {sample.value}")
    return "; ".join(parts)


class DriftRegistry:
    """The session's record of runtime schema drift: the WARNINGs and the `SchemaDrift` text.

    Massive's data can differ from the documented schema until the library's schemas are
    regenerated. The wire decoders report each payload's differences as a `DriftReport`: the
    fields it had that the schema does not declare (dropped) and the declared fields it lacked
    (null). Every transport routes those reports here:

    - `report` logs one WARNING per (dataset, transport, field) for the life of the session.
      A static verb (`fetch()`, `history()`) reports drift only this way.
    - `report_statuses` logs one WARNING per (REST path, status) for a REST envelope status
      outside the documented ones; the page's rows are kept.
    - `record` adds a report to the drift of one stream or connection, its owner, and returns
      the owner's new `SchemaDrift` text when it changed, for the caller to write into the
      owner's diagnostics row. `forget` drops an owner once it has left its registry.

    Drift is never a failure: a missing key, a value that does not convert, and a timestamp out
    of bounds fail where they are found and never reach the registry.

    `SessionCore` creates one per session, and any library thread calls it. Thread-safe: one
    leaf lock guards its sets, held for set operations and for building a text, never for
    logging, a Deephaven call, or a wait. `record` and `forget` may therefore be called with
    the session's `registry_lock` held. `report` and `report_statuses` must not be: they log,
    and a log handler may do I/O.

    Every text `record` returns is built under the lock from the owner's entries at that
    moment, but two calls for one owner can still return in either order, so the row write
    must be ordered with the call. A stream's row is: `SessionCore.record_stream_drift` calls
    `record` and writes the row in one `registry_lock` section, and calls `forget` when the
    stream leaves the registry. A connection's row is written only by its loop thread, which
    is also where its frames, and so its drift, are decoded.

    Example:
        ::

            from deephaven_massive._massive import DriftReport, Transport
            from deephaven_massive._tables.diagnostics import DriftRegistry

            drift = DriftReport(extra=frozenset({"halted"}), missing=frozenset({"trf_id"}))
            registry = DriftRegistry()
            where = "GET /v3/trades/{stockTicker}"
            registry.report("stocks.trades", Transport.REST, drift, where=where)  # two WARNINGs
            registry.report("stocks.trades", Transport.REST, drift, where=where)  # no log
            registry.record(7, "REST", drift)  # "REST:halted+, REST:trf_id-"
            registry.record(7, "REST", drift)  # unchanged: None
    """

    __slots__ = ("_lock", "_owners", "_statuses", "_warned")

    def __init__(self) -> None:
        """Start with no drift seen."""
        self._lock = threading.Lock()
        self._warned: set[tuple[str, Transport, str]] = set()
        self._statuses: set[tuple[str, str]] = set()
        self._owners: dict[Hashable, frozenset[str]] = {}

    def report(
        self,
        dataset: str,
        transport: Transport,
        drift: DriftReport,
        *,
        where: str,
        symbol: str | None = None,
    ) -> None:
        """Log a WARNING for each field of `drift` not yet reported for the dataset and transport.

        Each WARNING names the dataset, the transport, and the field, then where the payload
        came from, the symbol of the first row that showed the field, and for an extra field
        that row's value (`DriftReport.samples`), so the log says enough to reproduce it: `...
        (schema drift) at wss://delayed.massive.com/stocks channel T; symbol AAPL; first value
        182.33`. A field is reported once per process and dataset; the first sample is the one
        a fix needs. Ignores `drift.statuses`; see `report_statuses`.

        Args:
            dataset: The dataset name, such as `"stocks.trades"`.
            transport: The transport the payload arrived on.
            drift: The payload's drift, as a wire decoder returned it.
            where: Where the payload came from: the REST request (`"GET /v3/trades/AAPL"`),
                the WebSocket URL and channel, or the flat file.
            symbol: The symbol the payload is about when its sample cannot say (a REST
                request for one symbol), or None.
        """
        changes = [(field, True) for field in drift.extra]
        changes += [(field, False) for field in drift.missing]
        with self._lock:
            new = [
                (f, extra) for f, extra in changes if (dataset, transport, f) not in self._warned
            ]
            self._warned.update((dataset, transport, f) for f, _ in new)
        for field, extra in sorted(new):
            context = _drift_context(drift, field, where=where, symbol=symbol)
            if extra:
                logger.warning(
                    "%s: %s field %r is not in the schema; it is dropped (schema drift) at %s",
                    dataset,
                    transport.value,
                    field,
                    context,
                )
            else:
                logger.warning(
                    "%s: %s field %r is missing; it reads as null (schema drift) at %s",
                    dataset,
                    transport.value,
                    field,
                    context,
                )

    def report_statuses(self, path: str, statuses: frozenset[str]) -> None:
        """Log a WARNING for each undocumented REST envelope status not yet seen for the path.

        Args:
            path: The REST path template, such as `"/v3/trades/{stockTicker}"`.
            statuses: The page's statuses outside the documented ones (`DriftReport.statuses`).
        """
        with self._lock:
            new = sorted(status for status in statuses if (path, status) not in self._statuses)
            self._statuses.update((path, status) for status in new)
        for status in new:
            logger.warning(
                "%s: undocumented response status %r; the rows are kept (schema drift)",
                path,
                status,
            )

    def record(self, owner: Hashable, prefix: str, drift: DriftReport) -> str | None:
        """Add a report to one owner's drift and return its new `SchemaDrift` text, if changed.

        Each extra field becomes the entry `<prefix>:<field>+` and each missing one
        `<prefix>:<field>-`. An owner's entries only accumulate: drift seen once stays shown.

        Args:
            owner: The stream's id, or the connection's key (a tuple), whose row shows the
                drift.
            prefix: The transport's label (`"WebSocket"`, `"REST"`; `TRANSPORT_LABELS`) for
                a stream, or the channel (`"T"`, `"AM"`) for a connection.
            drift: The payload's drift; its `statuses` are not shown.

        Returns:
            The owner's entries, sorted, joined by `", "`, and cut to `SCHEMA_DRIFT_SHOWN`
            characters (ending in `…` when cut); None when the report added no entry.
        """
        entries = {f"{prefix}:{field}+" for field in drift.extra}
        entries |= {f"{prefix}:{field}-" for field in drift.missing}
        with self._lock:
            seen = self._owners.get(owner, frozenset())
            if entries <= seen:
                return None
            seen |= entries
            self._owners[owner] = seen
            # Built under the lock, so the text is the owner's entries at this moment.
            text = ", ".join(sorted(seen))
        if len(text) > SCHEMA_DRIFT_SHOWN:
            text = text[: SCHEMA_DRIFT_SHOWN - 1] + "…"
        return text

    def forget(self, owner: Hashable) -> None:
        """Drop an owner's drift once it has left its registry; its last row keeps the text.

        Args:
            owner: The owner `record` was given; unknown owners are ignored.
        """
        with self._lock:
            self._owners.pop(owner, None)


RateLimitPull: TypeAlias = "Callable[[], tuple[Sequence[RateLimitRow], float | None]]"
"""Returns the `m.rate_limits()` rows and the seconds until they change with time alone."""

_RATE_LIMITS_CHANGED: Final = object()
"""The queue item that asks the writer to pull fresh `m.rate_limits()` rows."""

_Item: TypeAlias = "ConnectionRow | StreamRow | threading.Event | object"


class Diagnostics:
    """The session's three diagnostics tables and the single thread that writes them.

    Created by `SessionCore` on the caller thread, inside the session's execution context, so
    the tables live in a session-owned liveness scope. A daemon writer (`QueueWorker`) takes
    row snapshots off its queue in batches: everything queued since it last woke is coalesced
    to the latest row per key and applied with one `InputTable.add` per table, so the queue
    cannot grow without bound when rows arrive faster than update-graph cycles. No other thread
    waits on the update graph for diagnostics. `write` only enqueues and is safe from any
    thread, including one that holds the session's `registry_lock`: its internal lock is a leaf
    held only around a non-blocking `put`. A row that does not fit its schema is logged at ERROR
    and skipped; the rest of its batch is applied. The rate-limit rows are pulled rather than
    queued: `watch_rate_limits` names their pull, and `rate_limits_changed` (safe from any
    thread, never blocking) asks the writer to pull them again, at most once per batch.

    `release()` lets the writer apply the rows already queued, release the scope, and exit, and
    waits for it unless told not to (a caller that holds the update graph's lock must not: the
    writer's adds cannot finish until it lets go); `stop()` does the same without waiting and
    without taking any lock, for use from a finalizer. A writer that dies on something it
    cannot carry on after calls `on_death` first (the session fails its live tables there).
    """

    __slots__ = (
        "_connections",
        "_rate_due",
        "_rate_limits",
        "_rate_pending",
        "_rate_pull",
        "_release_lock",
        "_released",
        "_scope",
        "_streams",
        "_writer",
    )

    def __init__(
        self,
        exec_ctx: ExecutionContext,
        name: str,
        *,
        on_death: Callable[[BaseException], None] | None = None,
    ) -> None:
        """Create the three input tables inside a new session-owned scope and start the writer.

        If creating a table or starting the writer fails, the scope (with any table already
        created in it) is released and the exception propagates.

        Args:
            exec_ctx: The session's execution context, entered by the writer thread.
            name: The writer thread's name, for thread dumps.
            on_death: Runs if the writer dies on something it cannot carry on after
                (`QueueWorker`).
        """
        self._release_lock = threading.Lock()
        self._released = False
        # Set once by `watch_rate_limits`; read by the writer. Guarded by `_release_lock`.
        self._rate_pull: RateLimitPull | None = None
        self._rate_pending = False
        # When the rate-limit rows next change with time alone (`time.monotonic()`), or None;
        # read and written only by the writer thread.
        self._rate_due: float | None = None
        self._scope = new_scope()
        try:
            with self._scope.open():
                self._connections: InputTable = input_table(
                    col_defs(CONNECTIONS), key_cols=list(CONNECTION_KEYS)
                )
                self._streams: InputTable = input_table(
                    col_defs(STREAMS), key_cols=list(STREAM_KEYS)
                )
                self._rate_limits: InputTable = input_table(
                    col_defs(RATE_LIMITS), key_cols=list(RATE_LIMIT_KEYS)
                )
            self._writer: QueueWorker[_Item] = QueueWorker(
                exec_ctx,
                name,
                handle=self._apply,
                on_exit=functools.partial(release_scope, exec_ctx, self._scope),
                timeout=self._rate_wait,
                on_timeout=self._rate_due_now,
                on_death=on_death,
            )
        except BaseException:  # also KeyboardInterrupt: the scope must not leak
            release_scope(exec_ctx, self._scope)
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
    def rate_limits(self) -> InputTable:
        """The table behind `m.rate_limits()`."""
        return self._rate_limits

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

    def watch_rate_limits(self, pull: RateLimitPull) -> None:
        """Name how the `m.rate_limits()` rows are read, and read them once.

        Args:
            pull: Returns the current row of every bucket and the seconds until they change
                with time alone (None when they do not). The writer calls it, on its own
                thread and without any lock of this object, and pulls again at that moment if
                nothing woke it sooner (its queue wait times out then); it must not block or
                call Deephaven.

        Raises:
            RuntimeError: If a pull is already set.
        """
        with self._release_lock:
            if self._rate_pull is not None:
                raise RuntimeError("the rate-limit rows already have a pull")
            self._rate_pull = pull
        self.rate_limits_changed()

    def rate_limits_changed(self) -> None:
        """Ask the writer to pull the rate-limit rows again; never blocks.

        Safe from any thread, including one holding a leaf lock: it only queues a marker, at
        most one until the writer takes it. Does nothing before `watch_rate_limits` or after
        `release()`.
        """
        with self._release_lock:
            if self._released or self._rate_pending or self._rate_pull is None:
                return
            self._rate_pending = True
            self._writer.put(_RATE_LIMITS_CHANGED)

    def barrier(self) -> threading.Event:
        """Return an event that is set once every row queued before this call is applied.

        Only for a live session's diagnostics, whose writer is running (tests use it to wait
        for rows). After `release()` the event comes back already set, even when a release
        that did not wait still has rows to apply: join `thread` to wait for those. After
        `stop()` (the finalizer of a collected session) the writer may already have exited, so
        the event might never be set: do not call it then.

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

        Takes no lock, so the session's finalizer can call it from garbage collection. Later
        rows and rate-limit notices are not queued, as after `release()`.
        """
        self._released = True  # one attribute store; `write` and the rest read it under a lock
        self._writer.stop()

    def release(self, *, wait: bool = True) -> None:
        """Apply the queued rows, release the tables' scope, and end the writer. Idempotent.

        No row is queued after it. The writer applies the rows queued before it, then releases
        the scope, under the update graph's shared lock (`release_scope`), and exits.

        Args:
            wait: True to block until the writer has finished, so the rows are in the tables
                when it returns; it must then not run on an update-graph thread, nor on a
                thread that holds the update graph's lock, where the writer's adds could never
                finish. False to return at once, as a caller holding that lock must: the
                writer finishes once the lock is released.
        """
        with self._release_lock:
            self._released = True
        self._writer.stop()
        if wait:
            self._writer.thread.join()

    def _apply(self, items: list[_Item]) -> None:
        """Apply one batch: the latest row per key, one `add` per table, then the barriers.

        The rate-limit rows are pulled when a marker is in the batch, or when their
        time-driven change has come, so a queue that never goes quiet cannot hold them back.

        Args:
            items: Rows, barrier events, and rate-limit markers, in queue order.
        """
        streams: dict[int, StreamRow] = {}
        connections: dict[tuple[str, str, str], ConnectionRow] = {}
        barriers: list[threading.Event] = []
        # The rows' time-driven change is due: a queue too busy for the timed wait to expire
        # must not starve that pull.
        due = self._rate_due
        pull = due is not None and time.monotonic() >= due
        for item in items:
            if isinstance(item, threading.Event):
                barriers.append(item)
            elif isinstance(item, StreamRow):
                streams[item.stream_id] = item
            elif isinstance(item, ConnectionRow):
                connections[item.transport, item.asset_class, item.family] = item
            else:
                pull = True
        try:
            for table, schema, rows in (
                (self._streams, STREAMS, lambda: list(streams.values())),
                (self._connections, CONNECTIONS, lambda: list(connections.values())),
                (self._rate_limits, RATE_LIMITS, self._pull_rate_limits if pull else list),
            ):
                try:  # one table's failure never costs the others their rows
                    self._add(table, schema, rows())
                except Exception:
                    logger.exception(
                        "diagnostics: failed to update the %s table. %s", schema.name, LIBRARY_BUG
                    )
        finally:
            for barrier in barriers:
                barrier.set()

    def _pull_rate_limits(self) -> list[RateLimitRow]:
        """Take the pending marker, pull the rate-limit rows, and note when they next change.

        Runs on the writer thread, the only one that touches `_rate_due`.

        Returns:
            The pull's rows. A marker is queued only once the pull is set, and the pull is
            never unset.
        """
        with self._release_lock:
            self._rate_pending = False
            pull = cast("RateLimitPull", self._rate_pull)
        rows, change = pull()
        self._rate_due = None if change is None else time.monotonic() + change
        return list(rows)

    def _rate_wait(self) -> float | None:
        """Return how long the writer may wait for an item: until the rows next change.

        Returns:
            Seconds until the rate-limit rows change with time alone, or None to wait for an
            item however long.
        """
        due = self._rate_due
        return None if due is None else due - time.monotonic()

    def _rate_due_now(self) -> None:
        """Pull the rate-limit rows again: their time-driven change has come."""
        self._rate_due = None
        self._apply([_RATE_LIMITS_CHANGED])

    @staticmethod
    def _add(
        table: InputTable,
        schema: Schema,
        rows: list[ConnectionRow] | list[StreamRow] | list[RateLimitRow],
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
                    logger.exception(
                        "diagnostics: failed to write a %s row. %s", schema.name, LIBRARY_BUG
                    )
            if not batches:
                return
            batch = pa.concat_tables(batches)
        table.add(dh_arrow.to_table(batch))


def pascal_case(path: str) -> str:
    """Turn a snake-case name or a dotted path of them into a PascalCase column name.

    Every underscore- or dot-separated word is capitalized and joined: `sip_timestamp` becomes
    `SipTimestamp` and `last_trade.sip_timestamp` becomes `LastTradeSipTimestamp`. Words that
    are already capitalized keep their other letters. The diagnostics tables name their columns
    with it.

    Args:
        path: A name or a dot-separated path of names.

    Returns:
        The PascalCase column name.
    """
    words = re.split(r"[._]+", path)
    return "".join(word[:1].upper() + word[1:] for word in words if word)
