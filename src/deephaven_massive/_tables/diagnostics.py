"""The live diagnostics tables: the session's connections, streams, rate limits, reads, problems.

They are the tables behind `m.connections()`, `m.streams()`, `m.rate_limits()`,
`m.symbol_reads()`, and `m.problems()`. Each is a keyed table that the library writes and
users read: a blink table that the library's writer adds rows to through its publisher
(`blink_publisher`), and the latest row of each key of it (`last_by` on the keys), which a
later row of the key replaces:

- **connections**: one row per WebSocket connection, plus one `Transport=REST` row per resolved
  (asset class, channel family) recording the host decision and its evidence. Keyed by
  `Transport`, `AssetClass`, and `Family`.
- **streams**: one row per stream (a live subscription with its own publisher), per `poll()`
  table, per background read (a `fetch()` or `history()` without `wait=True`), and per followed
  read (a `history()` or `fetch()` that follows a refreshing symbol table), told apart by `Kind`,
  with the `repr` of the query that opened it in `Query`. Keyed by `StreamId`.
- **symbol_reads**: one row per symbol of each followed read: whether it is queued, loading,
  waiting to retry, done, failed, removed, stopped, or closed, with its source, its rows, its
  attempts, and its latest error. Keyed by `StreamId` and `Symbol`. The session's follow
  coordinator (`_tables/follow_coordinator.py`) makes the rows (`_tables/symbol_read_rows.py`)
  and the session core queues them (`SessionCore.write_symbol_reads`).
- **rate_limits**: one row per REST rate-limit bucket: each Massive subscription, and `other`
  for the datasets Massive assigns to none. Keyed by `Bucket`. Its rows are not queued:
  the bucket state lives in a governor that several sessions share, so the writer pulls a fresh
  snapshot through its pull (`RateLimitPull`) whenever it was told the state changed
  (`rate_limits_changed`), and a row can never be overtaken by an older one. The writer also
  pulls again when the state changes with time alone (a pause ends, a slot frees): it waits on
  its queue no longer than until the moment the pull names. Each pause of a bucket is also a
  `rate_limited` span of `m.problems()`, from its start to its end as the governor tells them
  (`pause_changed`, from `RequestGovernor.subscribe_pauses`), not as pulls see them, so a pause
  that starts and ends between two pulls still has its row.
- **problems**: every problem with the feed: one row per (table, cause, symbol, field, span),
  merged by the session's `ProblemLedger` (`_tables/problem_ledger.py`) from the problems the
  session's reporter queues (`report_problems`). Keyed by `PROBLEM_KEYS`.

All of them spell a transport as every other text a user reads does (`TRANSPORT_LABELS`:
`WebSocket`, `REST`, `flat files`) in `Transport`; the
rows hold the `Transport` member, and `as_row` writes its label. A stream's `Query` is cut to
`QUERY_SHOWN` characters (`shown_query`), as `Symbols` is cut to its first symbols, so a query
over a long list stays readable.

The tables are not input tables: nobody can edit them in the Deephaven IDE, and the library never
deletes a row, so each keeps one row per key it was ever written (a stream ended long ago
keeps its last row), as the session's history of what it opened.

The tables live in one session-owned `LivenessScope`, created with the session on the caller
thread and released by `m.close()`. Users never get the tables themselves: each method of
them (`m.streams()`, `m.problems()`, and the others) returns a view of one, tracked like a
live result (`SessionCore.diagnostics_view`), which keeps the session alive and current while
it is held and keeps the table, with its final rows, after the scope is released. One daemon writer
thread per session adds every row: the rest of the library hands it immutable row snapshots
through a queue, so no caller, reaper, or flush thread ever waits on the update graph for
bookkeeping, and nothing is written from a Deephaven listener callback. The writer adds
whatever is queued in one batch per table, keeping the latest row per key, and waits at least
`WRITE_INTERVAL` between batches, so a burst of rows costs a few adds. An add returns at once;
its rows reach the table on the next update cycle (`Diagnostics.wait_until_shown` waits for
that). Each row replaces the whole row for its key; the queue is first in, first out, and the
session queues each snapshot while holding its `registry_lock`, so rows land in the order the
registry changed. Once the writer is stopped, rows are no longer queued.

The module also holds the session's `DriftRegistry`, which logs each REST envelope status
outside Massive's documented ones once per path. Every other drift (a field Massive's
documentation lacks) is a problem of `m.problems()`.

"""

from __future__ import annotations

import functools
import logging
import threading
import time
from dataclasses import dataclass, field, fields, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Final, TypeAlias

import pyarrow as pa
from deephaven import arrow as dh_arrow

from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._datasets.schema import Column, ColumnType, Schema
from deephaven_massive._datasets.symbols import AllSymbols, SymbolTable
from deephaven_massive._errors import (
    LIBRARY_BUG,
    NEW_SESSION,
    BrokenGuaranteeError,
    ErrorSource,
    MassiveError,
    library_bug,
    required,
    source_of,
    unmatched_form,
)
from deephaven_massive._massive import PauseNotice, Transport
from deephaven_massive._tables.columns import col_defs
from deephaven_massive._tables.execution_context import wait_for_update_cycle
from deephaven_massive._tables.liveness import QueueWorker, new_scope, release_scope
from deephaven_massive._tables.problem_events import (
    ProblemCause,
    ProblemEvent,
    ProblemSpans,
    SpanTracker,
)
from deephaven_massive._tables.problem_ledger import (
    PROBLEM_KEYS,
    PROBLEMS,
    ProblemKey,
    ProblemLedger,
    ProblemRow,
    problem_values,
)
from deephaven_massive._tables.publisher import blink_publisher
from deephaven_massive._tables.symbol_read_rows import (
    SYMBOL_READ_KEYS,
    SYMBOL_READS,
    SymbolReadRow,
    symbol_read_values,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from deephaven.execution_context import ExecutionContext
    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table

    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._enums import AssetClass, ChannelFamily, Recency
    from deephaven_massive._massive import Redactor
    from deephaven_massive._tables.problem_events import Problem
    from deephaven_massive._tables.publisher import Publisher

__all__ = [
    "CONNECTIONS",
    "CONNECTION_KEYS",
    "FINAL_STATES",
    "FLAT_FILES_AND_REST",
    "QUERY_SHOWN",
    "RATE_LIMITS",
    "RATE_LIMIT_KEYS",
    "STREAMS",
    "STREAM_KEYS",
    "SYMBOLS_LISTED",
    "WRITE_INTERVAL",
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
    "shown_query",
    "symbol_list_text",
    "thread_died",
    "utc_text",
]

logger = logging.getLogger(__name__)

SYMBOLS_LISTED: Final = 20
"""How many symbols the `Symbols` column lists before it truncates."""

QUERY_SHOWN: Final = 200
"""The longest `Query` text, in characters; a longer one is cut and ends in `…`."""

WRITE_INTERVAL: Final = 0.1
"""The fewest seconds between two batches of the diagnostics writer: what is queued meanwhile
is added as one batch, so a burst of rows costs each table one add per interval."""


class ConnectionState(StrEnum):
    """The `State` of a row of `m.connections()`.

    `WAITING` and `WAITING_SLOT` differ: a `WAITING` connection has opened no socket, since none
    of its live tables has a symbol to subscribe yet, while a `WAITING_SLOT` connection tried,
    and Massive refused it for want of a free connection on the account.
    """

    RESOLVING = "RESOLVING"
    WAITING = "WAITING"
    """No socket is open: no live table on the connection has a symbol to subscribe yet."""
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
        READ: A background read, `fetch()` or `history()` without `wait=True`, whose
            table fills once, when its download completes; or a followed read, a `history()`
            or `fetch()` of a refreshing symbol table, whose table gains and drops each symbol's
            rows as the symbol table changes.
    """

    STREAM = "stream"
    POLL = "poll"
    READ = "read"


class StreamState(StrEnum):
    """The `State` of a row of `m.streams()`.

    `RUNNING` is a stream or `poll()` table that publishes. A background read is `LOADING`
    while it downloads and `DONE` once its rows have landed; `DONE` is final, like `FAILED`.
    A followed read is `LOADING` while any of its current symbols is queued, loading, or
    waiting to retry, and `RUNNING` once every one has landed or failed; it keeps following.
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


FINAL_STATES: Final = frozenset({StreamState.FAILED, StreamState.CLOSED, StreamState.DONE})
"""The states no failure changes any more: a failed or closed table, and a read whose rows have
landed. `close()` and a dying library thread fail only the tables in none of them."""


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
    """Why a row of `m.rate_limits()` is paced: the `LimitSource` column.

    `DECLARED`: a session in this server that uses the same API key gives the subscription's
    limit in `MassiveConfig.rest_requests_per_minute`. `LEARNED`: Massive refused one of its
    requests for exceeding the maximum requests per minute. The limit is the number of its
    requests Massive answered in the minute of that refusal.
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
            ("Recency", _S),
            ("RowsPublished", _L),
            ("Loading", _I),
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
        deferred: The stream's own options topics the cap defers (one per contract, since a
            stream reads one channel).
        transport: `Transport.WS` for a stream, `Transport.REST` for a `poll()`, and for a
            background read the source it reads, `Transport.REST` or `Transport.FLAT_FILES`;
            the table shows its label, `WebSocket`, `REST`, or `flat files`. A followed read
            whose flat-file reads also read days from REST holds the text
            `FLAT_FILES_AND_REST` instead.
        host: The serving host; null for a background read.
        path: The cluster path; null for a background read.
        recency: Whether the table's data is real-time or delayed: a stream's host's
            (`REALTIME` or `DELAYED`; on a pinned host, as Massive's snapshot labels it, and
            `UNKNOWN` without a label);
            for a `poll()` table of current data, what Massive's labels on its latest cycle
            showed (`REALTIME`, or `DELAYED` for a subscription in
            `MassiveConfig.delayed_subscriptions`; a delayed label on any other fails the
            table), and `UNKNOWN` until a label shows either; null for reference, economy,
            and partner data and for a background read. `UNKNOWN` is never real-time.
        rows_published: Rows published so far; a background read's, once its rows land; a
            followed read's, the rows now in its result.
        loading: A followed read's symbols that are queued, loading, or waiting to retry;
            None (null) on every other row.
        requests_per_cycle: REST requests per `poll()` cycle.
        state: The stream state.
        error: Why the stream stopped, once it has; null while it runs. A running stream's
            problems are rows of `m.problems()`.
        created_at: When the stream was created.
    """

    stream_id: int
    kind: StreamKind
    dataset: str
    query: str
    symbol_count: int | None
    symbols: str
    held_results: int
    deferred: int = 0
    transport: Transport | str
    host: str | None = None
    path: str | None = None
    recency: Recency | None = None
    rows_published: int = 0
    loading: int | None = None
    requests_per_cycle: int | None = None
    state: StreamState = StreamState.RUNNING
    error: str | None = None
    created_at: int


@dataclass(frozen=True, kw_only=True, slots=True)
class StreamUpdate:
    """A change to one stream's diagnostics row, for `SessionCore.update_streams`.

    Built by the WebSocket transport's flush thread after each flush, by a `poll()` table's poller
    after each cycle, by a connection's loop thread, and by the follow coordinator for a followed
    read; every field left as None keeps the row's current value. It never changes `State` or the
    failure: only `SessionCore.fail_stream` fails a stream. A stream's problems are not set here:
    they go to `m.problems()` (`SessionCore.problems`).

    Attributes:
        rows_published: The stream's total rows published so far.
        requests_per_cycle: The REST requests of the latest `poll()` cycle.
        deferred: The stream's own options topics the cap defers (one per contract, since a
            stream reads one channel).
        symbol_count: How many symbols the stream now covers.
        symbols: The readable, truncated list of those symbols, as `describe_symbols` writes it.
        recency: What the latest `poll()` cycle's labels showed (`StreamRow.recency`).
        loading: A followed read's count of symbols queued, loading, or waiting to retry.
        transport: A followed read's sources so far (`StreamRow.transport`).
    """

    rows_published: int | None = None
    requests_per_cycle: int | None = None
    deferred: int | None = None
    symbol_count: int | None = None
    symbols: str | None = None
    recency: Recency | None = None
    loading: int | None = None
    transport: Transport | str | None = None

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
            requests_per_cycle=row.requests_per_cycle
            if self.requests_per_cycle is None
            else self.requests_per_cycle,
            deferred=row.deferred if self.deferred is None else self.deferred,
            symbol_count=row.symbol_count if self.symbol_count is None else self.symbol_count,
            symbols=row.symbols if self.symbols is None else self.symbols,
            recency=row.recency if self.recency is None else self.recency,
            loading=row.loading if self.loading is None else self.loading,
            transport=row.transport if self.transport is None else self.transport,
        )


FLAT_FILES_AND_REST: Final = "flat files and REST"
"""The `Transport` text of a followed read whose flat-file read also read days from REST: the
files of some days were missing, and REST served them."""


@dataclass(frozen=True, kw_only=True, slots=True)
class RateLimitRow:
    """One row of `m.rate_limits()`. Timestamps are epoch nanoseconds; None is null.

    Every session that shares a governor (the same API key and REST base URL) shows the same
    rows, since Massive counts the requests of all of them together.

    Attributes:
        bucket: The bucket: a `dm.Subscription` value, or `other` for reference data and the
            other datasets Massive assigns to no subscription, which it counts on their own.
        state: What a new request meets now.
        requests_per_minute: The declared or learned limit; null while unpaced.
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


_Row: TypeAlias = "ConnectionRow | StreamRow | RateLimitRow | SymbolReadRow | ProblemRow"


def as_row(row: _Row) -> dict[str, object]:
    """Return a row keyed by column name: each field's name in PascalCase.

    A `Transport` value becomes its user-facing label (`TRANSPORT_LABELS`), so the tables spell
    transports as `columns()` does: `Transport.WS` is shown as `WebSocket`. A symbol-read row
    is written by `symbol_read_values`, and a problem row by `problem_values`.

    Args:
        row: A connection, stream, rate-limit, symbol-read, or problem row.

    Returns:
        A `{column: value}` mapping for `CONNECTIONS`, `STREAMS`, `RATE_LIMITS`,
        `SYMBOL_READS`, or `PROBLEMS`.
    """
    if isinstance(row, SymbolReadRow):
        return symbol_read_values(row)
    if isinstance(row, ProblemRow):
        return problem_values(row)
    return {_pascal_case(f.name): _shown(getattr(row, f.name)) for f in fields(row)}


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

    Raises:
        BrokenGuaranteeError: For a form of symbols with no rule here.
    """
    match symbols:
        case tuple():
            return symbol_list_text(len(symbols), symbols[:SYMBOLS_LISTED])
        case AllSymbols():
            return None, "ALL"
        case SymbolTable():
            return None, "reactive table"
        case _:
            raise BrokenGuaranteeError(unmatched_form("a stream's symbol summary", symbols))


def symbol_list_text(count: int, first: Sequence[str]) -> tuple[int, str]:
    """Summarize a list of symbols from its length and its first symbols alone.

    For a caller that keeps a large, changing set of symbols and finds its first ones without
    sorting it all (the follow coordinator).

    Args:
        count: How many symbols the list holds.
        first: Its first `SYMBOLS_LISTED` symbols, in order (all of them for a shorter list).

    Returns:
        `(count, text)`: the text joins `first` by commas, with a `(+N more)` suffix when the
        list holds more.
    """
    shown = ", ".join(first)
    hidden = count - len(first)
    return count, f"{shown} (+{hidden} more)" if hidden > 0 else shown


class DriftRegistry:
    """The session's record of the undocumented REST envelope statuses it has logged.

    A REST page's envelope holds a status (`OK`, `DELAYED`); one outside Massive's documented
    ones is drift of the envelope, not of a table's rows, and the page's rows are kept.
    `report_statuses` logs one WARNING per (REST path, status) for the life of the session.
    Every other drift (a field a payload had that the schema does not declare) is a
    `schema_drift` problem of the table that received it, in `m.problems()`.

    `SessionCore` creates one per session, and any library thread calls it. Thread-safe: one
    leaf lock guards its set, held only to read and update it, never to log; a caller must not
    hold `registry_lock`, since a log handler may do I/O.
    """

    __slots__ = ("_lock", "_statuses")

    def __init__(self) -> None:
        """Start with no status seen."""
        self._lock = threading.Lock()
        self._statuses: set[tuple[str, str]] = set()

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


RateLimitPull: TypeAlias = "Callable[[], tuple[Sequence[RateLimitRow], float | None]]"
"""Returns the `m.rate_limits()` rows and the seconds until they change with time alone."""

_RATE_LIMITS_CHANGED: Final = object()
"""The queue item that asks the writer to pull fresh `m.rate_limits()` rows."""


@dataclass(frozen=True, slots=True)
class _StreamGone:
    """A table that left its registry, whose open problem spans end (`Diagnostics.stream_gone`).

    Attributes:
        stream_id: The table's `StreamId`.
    """

    stream_id: int


@dataclass(frozen=True, slots=True)
class _ReadQuery:
    """The query of a read that has no `m.streams()` row (`Diagnostics.name_read`).

    Attributes:
        call: The read's call id, the `StreamId` its problems carry.
        query: Its query, as `shown_query` cuts it.
    """

    call: int
    query: str


@dataclass(slots=True)
class _Batch:
    """One writer batch's items, sorted by kind (`Diagnostics._apply`).

    Attributes:
        streams: The latest stream row per key.
        connections: The latest connection row per key.
        symbol_reads: The latest symbol-read row per key.
        problems: The problems and the rate-limit pause notices, in order.
        reads: The read queries, in order.
        gone: The tables that left their registries.
        added: The `wait_until_shown` events.
        pull: Whether a rate-limit marker asks for a pull.
    """

    streams: dict[int, StreamRow] = field(default_factory=dict)
    connections: dict[tuple[str, str, str], ConnectionRow] = field(default_factory=dict)
    symbol_reads: dict[tuple[int, str], SymbolReadRow] = field(default_factory=dict)
    problems: list[Problem | PauseNotice] = field(default_factory=list)
    reads: list[_ReadQuery] = field(default_factory=list)
    gone: list[int] = field(default_factory=list)
    added: list[threading.Event] = field(default_factory=list)
    pull: bool = False

    @classmethod
    def of(cls, items: Sequence[_Item]) -> _Batch:
        """Sort a batch's items.

        Args:
            items: The items, in queue order.

        Returns:
            The batch.

        Raises:
            BrokenGuaranteeError: If an item is of no kind the writer takes.
        """
        batch = cls()
        for item in items:
            if isinstance(item, threading.Event):
                batch.added.append(item)
            elif isinstance(item, StreamRow):
                batch.streams[item.stream_id] = item
            elif isinstance(item, ConnectionRow):
                batch.connections[item.transport, item.asset_class, item.family] = item
            elif isinstance(item, SymbolReadRow):
                batch.symbol_reads[item.stream_id, item.symbol] = item
            elif isinstance(item, (ProblemEvent, ProblemSpans, PauseNotice)):
                batch.problems.append(item)
            elif isinstance(item, _ReadQuery):
                batch.reads.append(item)
            elif isinstance(item, _StreamGone):
                batch.gone.append(item.stream_id)
            elif item is _RATE_LIMITS_CHANGED:
                batch.pull = True
            else:
                raise BrokenGuaranteeError(library_bug(f"the diagnostics queue holds {item!r}"))
        return batch


_Item: TypeAlias = (
    "ConnectionRow | StreamRow | SymbolReadRow | Problem | PauseNotice | _ReadQuery"
    " | _StreamGone | threading.Event | object"
)


class _KeyedTable:
    """One diagnostics table: a blink table's publisher and the latest row per key of it.

    The writer adds each batch of rows to the blink table through its publisher, and the table
    users see is `last_by` of the blink table on the keys: it keeps the latest row of each key,
    copied out of the blink rows, so a later row of a key replaces the earlier one as a modify
    (also when two adds of one key land in one update cycle), and a new key is appended after
    the keys seen before it. Rows are never deleted, so the table keeps one row per key ever
    written. It is not an input table: nobody can edit it in the Deephaven IDE, and no row can be
    removed.

    Created by `Diagnostics` on the caller thread, inside the diagnostics scope and the
    session's execution context. Only the writer thread calls `add`, so the publisher needs no
    lock of its own.

    Attributes:
        name: The publisher's name, such as `massive #1 diagnostics streams`, for the log.
        schema: The table's columns, keys first.
        publisher: The blink table's publisher.
        table: The `last_by` table users see a view of.
    """

    __slots__ = ("name", "publisher", "schema", "table")

    def __init__(
        self,
        name: str,
        schema: Schema,
        keys: Sequence[str],
        *,
        release_later: Callable[[LivenessScope], None],
    ) -> None:
        """Create the blink table, its publisher, and its `last_by` on the keys.

        Args:
            name: The publisher's name.
            schema: The table's columns, keys first.
            keys: The key columns.
            release_later: Releases a scope on the session's reaper (`blink_publisher`).
        """
        self.name: Final = name
        self.schema: Final = schema
        blink, publisher = blink_publisher(
            name,
            col_defs(schema),
            on_shutdown=functools.partial(_log_shut_down, name),
            release_later=release_later,
        )
        self.publisher: Publisher = publisher
        self.table: Final = blink.last_by(list(keys))

    def add(self, rows: Sequence[_Row]) -> None:
        """Add rows with one conversion and one `add`, skipping rows that do not fit.

        The whole batch is converted at once; only if that fails is each row converted on its
        own, so the rows that fit are still added and each one that does not is logged at
        ERROR. The rows are in the table after the next update cycle.

        Args:
            rows: The rows, at most one per key.
        """
        if not rows:
            return
        schema = self.schema
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
        self.publisher.add(dh_arrow.to_table(batch))


def _log_shut_down(name: str) -> None:
    """A diagnostics blink table's shutdown callback: it only logs.

    Args:
        name: The table's publisher name.
    """
    logger.debug("%s: its blink table has shut down", name)


class Diagnostics:
    """The session's five diagnostics tables and the single thread that writes them.

    Created by `SessionCore` on the caller thread, inside the session's execution context, so
    the tables live in a session-owned liveness scope. A daemon writer (`QueueWorker`) takes
    row snapshots off its queue in batches: everything queued since it last woke is coalesced
    to the latest row per key and added with one `BlinkPublisher.add` per table, and the
    writer starts its next batch no sooner than `WRITE_INTERVAL` after the last, so a burst of
    rows costs few adds and the queue cannot grow without bound. No thread waits on the
    update graph for diagnostics: an add returns at once, and its rows are in the table after
    the next update cycle. `write` only enqueues and is safe from any thread, including one
    that holds the session's `registry_lock`: its internal lock is a leaf held only around a
    non-blocking `put`. A row that does not fit its schema is logged at ERROR and skipped; the
    rest of its batch is added. The rate-limit rows are pulled rather than queued:
    `watch_rate_limits` names their pull, and `rate_limits_changed` (safe from any thread,
    never blocking) asks the writer to pull them again, at most once per batch. Each pause of a
    bucket is a `rate_limited` span, made from the governor's notices (`pause_changed`).
    Problems are queued by `report_problems` and merged by the writer's `ProblemLedger`, which
    also learns each table's query from its `m.streams()` row, or from `name_read` for a read
    with none, and ends a table's open spans once its row is final (`FINAL_STATES`).

    `release()` lets the writer add the rows already queued, release the scope, and exit, and
    waits for it unless told not to (a caller that holds the update graph's lock must not: the
    writer releases the scope under that lock).

    A writer that dies on something it cannot carry on after (`QueueWorker`) calls `on_death`
    first (the session fails its live tables there), then fails the five tables, so every view
    of them and every table derived from one fails too instead of showing frozen rows as if
    they were current, and hands the scope to `release_later`, since the writer will never
    release it. No row is queued after the death; `died` names it.
    """

    __slots__ = (
        "_connections",
        "_died",
        "_exec_ctx",
        "_label",
        "_ledger",
        "_on_death",
        "_paused",
        "_pauses",
        "_problems",
        "_rate_due",
        "_rate_limits",
        "_rate_pending",
        "_rate_pull",
        "_release_later",
        "_release_lock",
        "_released",
        "_scope",
        "_scope_released",
        "_streams",
        "_symbol_reads",
        "_writer",
    )

    def __init__(
        self,
        exec_ctx: ExecutionContext,
        name: str,
        *,
        label: str,
        redactor: Redactor,
        release_later: Callable[[LivenessScope], None],
        on_death: Callable[[BaseException], None],
    ) -> None:
        """Create the five tables inside a new session-owned scope and start the writer.

        Each blink table is created under the update graph's shared lock (`blink_publisher`),
        so this waits for a running update cycle to end; it must not run on an update-graph
        thread or under a library lock. If creating a table or starting the writer fails, the
        scope (with any table already created in it) is released and the exception propagates.

        Args:
            exec_ctx: The session's execution context, entered by the writer thread.
            name: The writer thread's name, for thread dumps, such as
                `massive #1 diagnostics`; each table's publisher adds the table's name.
            label: The session's label, such as `"MassiveSession #1"`, which the tables'
                failure names if the writer dies.
            redactor: The session's secrets, which no `m.problems()` text may hold.
            release_later: Releases a scope on the session's reaper, under the update graph's
                shared lock (`SessionCore.release_later`): for the blink tables' publishers,
                and for the tables' scope if the writer dies. The reaper must outlive the
                writer.
            on_death: Runs first if the writer dies on something it cannot carry on after
                (`QueueWorker`).
        """
        self._exec_ctx = exec_ctx
        self._label = label
        self._release_later = release_later
        self._on_death = on_death
        # What killed the writer, set once by the dying writer; guarded by `_release_lock`.
        self._died: BaseException | None = None
        # Whether the writer began to release the scope (`_release_tables`): a death after that
        # must not hand it over again. Only the writer thread touches it.
        self._scope_released = False
        self._release_lock = threading.Lock()
        self._released = False
        # Set once by `watch_rate_limits`; read by the writer. Guarded by `_release_lock`.
        self._rate_pull: RateLimitPull | None = None
        self._rate_pending = False
        # When the rate-limit rows next change with time alone (`time.monotonic()`), or None;
        # read and written only by the writer thread.
        self._rate_due: float | None = None
        # Read and written only by the writer thread: the problems table's rows, the
        # rate-limit pauses it shows, and each open pause's latest notice by its bucket.
        self._ledger = ProblemLedger(redactor)
        self._paused = SpanTracker(
            ProblemCause.RATE_LIMITED, dataset=None, stream_id=None, where="m.rate_limits()"
        )
        self._pauses: dict[str, PauseNotice] = {}
        self._scope = new_scope()
        try:
            with self._scope.open():
                self._connections = _KeyedTable(
                    f"{name} {CONNECTIONS.name}",
                    CONNECTIONS,
                    CONNECTION_KEYS,
                    release_later=release_later,
                )
                self._streams = _KeyedTable(
                    f"{name} {STREAMS.name}", STREAMS, STREAM_KEYS, release_later=release_later
                )
                self._rate_limits = _KeyedTable(
                    f"{name} {RATE_LIMITS.name}",
                    RATE_LIMITS,
                    RATE_LIMIT_KEYS,
                    release_later=release_later,
                )
                self._symbol_reads = _KeyedTable(
                    f"{name} {SYMBOL_READS.name}",
                    SYMBOL_READS,
                    SYMBOL_READ_KEYS,
                    release_later=release_later,
                )
                self._problems = _KeyedTable(
                    f"{name} {PROBLEMS.name}", PROBLEMS, PROBLEM_KEYS, release_later=release_later
                )
            self._writer: QueueWorker[_Item] = QueueWorker(
                exec_ctx,
                name,
                handle=self._apply,
                on_exit=self._release_tables,
                timeout=self._rate_wait,
                on_timeout=self._rate_due_now,
                on_death=self._writer_died,
                interval=WRITE_INTERVAL,
            )
        except BaseException:  # also KeyboardInterrupt: the scope must not leak
            release_scope(exec_ctx, self._scope)
            raise

    @property
    def connections(self) -> Table:
        """The table behind `m.connections()`: the latest row per key (`CONNECTION_KEYS`)."""
        return self._connections.table

    @property
    def streams(self) -> Table:
        """The table behind `m.streams()`: the latest row per key (`STREAM_KEYS`)."""
        return self._streams.table

    @property
    def rate_limits(self) -> Table:
        """The table behind `m.rate_limits()`: the latest row per key (`RATE_LIMIT_KEYS`)."""
        return self._rate_limits.table

    @property
    def symbol_reads(self) -> Table:
        """The table behind `m.symbol_reads()`: the latest row per key (`SYMBOL_READ_KEYS`)."""
        return self._symbol_reads.table

    @property
    def problems(self) -> Table:
        """The table behind `m.problems()`: the latest row per key (`PROBLEM_KEYS`)."""
        return self._problems.table

    @property
    def thread(self) -> threading.Thread:
        """The writer thread."""
        return self._writer.thread

    @property
    def died(self) -> BaseException | None:
        """What killed the writer, once it died (the tables have failed); None while it has not."""
        with self._release_lock:
            return self._died

    def write(self, row: ConnectionRow | StreamRow | SymbolReadRow) -> None:
        """Queue one row's full current state; it replaces the row with the same key.

        Never blocks. After `release()` the row is not queued at all.

        Args:
            row: An immutable snapshot of the row.
        """
        with self._release_lock:
            if not self._released:
                self._writer.put(row)

    def report_problems(self, problems: Sequence[Problem]) -> None:
        """Queue problems for `m.problems()`. Never blocks; after `release()`, queues nothing.

        Args:
            problems: Events and span snapshots, in order.
        """
        with self._release_lock:
            if not self._released:
                for problem in problems:
                    self._writer.put(problem)

    def pause_changed(self, notice: PauseNotice) -> None:
        """Queue a rate-limit pause's start, lengthening, or end for `m.problems()`.

        The governor calls it under its lock (`RequestGovernor.subscribe_pauses`), so it only
        queues: a leaf lock around a non-blocking `put`. After `release()`, it queues nothing.

        Args:
            notice: The pause's notice.
        """
        with self._release_lock:
            if not self._released:
                self._writer.put(notice)

    def stream_gone(self, stream_id: int) -> None:
        """End the open problem spans of a table that left its registry. Never blocks.

        Safe under `registry_lock`, as `write` is: a leaf lock around a non-blocking `put`.

        Args:
            stream_id: The table's `StreamId`.
        """
        with self._release_lock:
            if not self._released:
                self._writer.put(_StreamGone(stream_id))

    def name_read(self, call: int, query: str) -> None:
        """Name the query of a read with no `m.streams()` row, for its problems' `Query`.

        A `wait=True` read calls it before it reads, so its problems carry its query. Never
        blocks.

        Args:
            call: The read's call id.
            query: The query's `repr`.
        """
        with self._release_lock:
            if not self._released:
                self._writer.put(_ReadQuery(call, shown_query(query)))

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

    def wait_until_shown(self, timeout: float) -> bool:
        """Wait until every row queued before the call is in the tables, and so in every view.

        It waits for the writer to add those rows (after `release()`, for the writer to end),
        then for an update cycle that started after that (`wait_for_update_cycle`), which takes
        the rows into the tables and updates every view of them. For tests and for a caller
        that must read the rows right away; nothing in the library's normal work waits on it.

        Do not call it on an update-graph thread or while holding the update graph's lock, under
        which no cycle runs, nor after `stop()` (the finalizer of a collected session), whose
        writer may already be gone.

        Args:
            timeout: The most seconds to wait, in all.

        Returns:
            True once the rows are shown; False, after a WARNING, if the writer or the update
            cycle took longer than `timeout`.

        Raises:
            ValueError: On an update-graph thread (`wait_for_update_cycle`).
            RuntimeError: While the calling thread holds the update graph's lock.
        """
        deadline = time.monotonic() + timeout
        added = threading.Event()
        with self._release_lock:
            released = self._released
            if not released:
                self._writer.put(added)
        if released:
            self._writer.thread.join(timeout)
            done = not self._writer.thread.is_alive()
        else:
            done = added.wait(timeout)
        if not done:
            logger.warning(
                "%s: the diagnostics writer did not add the queued rows within %.1f s",
                self._writer.thread.name,
                timeout,
            )
            return False
        return wait_for_update_cycle(self._exec_ctx, max(deadline - time.monotonic(), 0.0))

    def release(self, *, wait: bool = True) -> None:
        """Add the queued rows, release the tables' scope, and end the writer. Idempotent.

        No row is queued after it. The writer adds the rows queued before it, then releases
        the scope, under the update graph's shared lock (`release_scope`), and exits. The added
        rows reach the tables on the next update cycle: a table that a held view still keeps
        alive takes them in then; a table nothing keeps is destroyed by the release, and its
        rows with it, since nobody can see them.

        Args:
            wait: True to block until the writer has ended; it must then not run on an
                update-graph thread, nor on a thread that holds the update graph's lock, where
                the writer's release of the scope could never take the lock. False to return
                at once, as a caller holding that lock must: the writer ends once the lock is
                released.
        """
        with self._release_lock:
            self._released = True
        self._writer.stop()
        if wait:
            self._writer.thread.join()

    def _release_tables(self) -> None:
        """The writer's last step (`on_exit`): end the open pauses, then release the scope.

        A rate-limit pause belongs to no table, so no final stream row ends it: the session's
        end does, so a view kept from before the release shows no pause still open. Each ends
        at the close, or at its own end if that came first and its notice never arrived.
        """
        try:
            now = time.time_ns()
            ended = []
            for bucket, notice in list(self._pauses.items()):
                del self._pauses[bucket]
                at = min(now, round(notice.ends * 1e9))
                ended.append(self._paused.update(self._held_pauses(), at=at))
            self._problems.add(self._ledger.apply(s for s in ended if s is not None))
        except Exception:  # a failed last row never costs the other tables theirs
            logger.exception(
                "diagnostics: failed to end the open rate-limit pauses in the problems table. %s",
                LIBRARY_BUG,
            )
        finally:  # the scope is released whatever the last rows did
            self._scope_released = True
            release_scope(self._exec_ctx, self._scope)

    def _writer_died(self, death: BaseException) -> None:
        """The writer's `on_death`: fail the tables and hand their scope to the reaper.

        Runs on the dying writer, after the owner's `on_death`, even if that raised. Each table
        fails through its publisher (`BlinkPublisher.publish_failure`), which pins it until the
        failure is delivered and hands the pin to `release_later`; a table whose failure raises
        is logged at ERROR and the others still fail. The scope goes to `release_later` unless
        the writer died releasing it, since releasing a scope twice raises.

        Args:
            death: What ended the writer.
        """
        with self._release_lock:
            self._released = True  # nothing would ever take a row queued from now on
            self._died = death
        error = thread_died(self._label, "diagnostics writer", death)
        try:
            self._on_death(death)
        finally:
            tables = (
                self._connections,
                self._streams,
                self._rate_limits,
                self._symbol_reads,
                self._problems,
            )
            for table in tables:
                try:
                    table.publisher.publish_failure(error)
                except Exception:
                    logger.exception(
                        "%s: could not fail the %s table. %s",
                        self._writer.thread.name,
                        table.schema.name,
                        LIBRARY_BUG,
                    )
            if not self._scope_released:
                self._release_later(self._scope)
            logger.debug(
                "%s: the diagnostics tables failed with the writer (%r); their scope %s",
                self._writer.thread.name,
                death,
                "was being released" if self._scope_released else "is handed to the reaper",
            )

    def _apply(self, items: list[_Item]) -> None:
        """Add one batch: the latest row per key, one `add` per table, then mark the waits.

        The rate-limit rows are pulled when a marker is in the batch, or when their
        time-driven change has come, so a queue that never goes quiet cannot hold them back.

        The problems are merged last, after the streams' rows have named their queries; the
        rate-limit pull, which reports each pause it finds over (`RequestGovernor.snapshot`),
        comes before them, so a pause whose end it finds is queued for the next batch.

        Args:
            items: Rows, problems, pause notices, read queries, `wait_until_shown` events, and
                rate-limit markers, in queue order.

        Raises:
            BrokenGuaranteeError: If an item is none of those, rather than taking it for one.
        """
        batch = _Batch.of(items)
        # The rows' time-driven change is due: a queue too busy for the timed wait to expire
        # must not starve that pull.
        due = self._rate_due
        pull = batch.pull or (due is not None and time.monotonic() >= due)
        streams = batch.streams
        try:
            for table, rows in (
                (self._streams, lambda: list(streams.values())),
                (self._connections, lambda: list(batch.connections.values())),
                (self._rate_limits, self._pull_rate_limits if pull else list),
                (self._symbol_reads, lambda: list(batch.symbol_reads.values())),
                (
                    self._problems,
                    functools.partial(self._problem_rows, list(streams.values()), batch),
                ),
            ):
                try:  # one table's failure never costs the others their rows
                    table.add(rows())
                except Exception:
                    logger.exception(
                        "diagnostics: failed to update the %s table. %s",
                        table.schema.name,
                        LIBRARY_BUG,
                    )
        finally:
            for event in batch.added:
                event.set()

    def _pull_rate_limits(self) -> list[RateLimitRow]:
        """Take the pending marker, pull the rate-limit rows, and note when they next change.

        Runs on the writer thread, the only one that touches `_rate_due`.

        Returns:
            The pull's rows. A marker is queued only once the pull is set, and the pull is
            never unset.
        """
        with self._release_lock:
            self._rate_pending = False
            pull = required(self._rate_pull, "the rate-limit pull")
        rows, change = pull()
        self._rate_due = None if change is None else time.monotonic() + change
        return list(rows)

    def _problem_rows(self, streams: Sequence[StreamRow], batch: _Batch) -> list[ProblemRow]:
        """Merge a batch's problems into the ledger, after its streams' rows and read queries.

        Args:
            streams: The batch's latest stream rows, which name their tables' queries; a final
                row ends the table's open spans, after the batch's problems.
            batch: The batch, with its read queries, problems, and tables gone.

        Returns:
            The problem rows that changed.
        """
        ledger = self._ledger
        changed: dict[ProblemKey, ProblemRow] = {}
        named = [(read.call, read.query) for read in batch.reads]
        named += [(stream.stream_id, stream.query) for stream in streams]
        for call, query in named:
            for row in ledger.stream_seen(call, query):
                changed[row.key] = row
        problems: list[Problem] = []
        for problem in batch.problems:
            if isinstance(problem, PauseNotice):
                problems.extend(self._pause_spans(problem))
            else:
                problems.append(problem)
        for row in ledger.apply(problems):
            changed[row.key] = row
        # Last, so a span the batch opened for a table whose row is now final ends too.
        now = time.time_ns()
        ended = [s.stream_id for s in streams if s.state in FINAL_STATES] + batch.gone
        for stream_id in ended:
            for row in ledger.stream_ended(stream_id, now):
                changed[row.key] = row
        return list(changed.values())

    def _pause_spans(self, notice: PauseNotice) -> list[ProblemSpans]:
        """Turn one pause notice into the `rate_limited` snapshot it makes.

        The governor tells each pause's start before its end (`RequestGovernor.subscribe_pauses`),
        so an end always finds its pause open; one that does not is a library bug, logged.

        Args:
            notice: The notice, in the order the governor made it.

        Returns:
            The snapshot; none when the notice changes nothing shown.
        """
        at = round(notice.at * 1e9)
        if notice.ended:
            if self._pauses.pop(notice.bucket, None) is None:
                logger.error(
                    "diagnostics: the end of a pause never begun: %r. %s", notice, LIBRARY_BUG
                )
                return []
            spans = self._paused.update(self._held_pauses(), at=at)
        else:
            self._pauses[notice.bucket] = notice
            spans = self._paused.update(
                self._held_pauses(), at=at, since=round(notice.started * 1e9)
            )
        return [] if spans is None else [spans]

    def _held_pauses(self) -> dict[str, tuple[str | None, str | None, str, int]]:
        """Return the open pauses as `rate_limited` spans, by bucket.

        Returns:
            Each span's symbol (none), field (the bucket, which keeps the pauses of two
            buckets rows of their own), detail, and rows (none).
        """
        return {
            bucket: (None, bucket, _pause_detail(notice), 0)
            for bucket, notice in self._pauses.items()
        }

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


def _pause_detail(notice: PauseNotice) -> str:
    """Return the `Detail` of a bucket's `rate_limited` span.

    Args:
        notice: The pause's latest notice.

    Returns:
        Such as `"bucket stocks paused until 2026-11-02T14:05:10Z: <Massive's refusal>"`.
    """
    return f"bucket {notice.bucket} paused until {utc_text(notice.ends)}: {notice.refusal}"


def thread_died(label: str, thread: str, death: BaseException) -> MassiveError:
    """Return the error a session's tables fail with when one of its library threads died.

    Args:
        label: The session's label, such as `"MassiveSession #1"`.
        thread: The thread, as the message names it, such as `"reaper"`.
        death: What ended the thread.

    Returns:
        Such as `MassiveError("MassiveSession #1: its reaper thread died (KeyboardInterrupt());
        this table no longer updates; open a new dm.MassiveSession() to read again")`. A death
        of an `Exception` is a bug, whose message says how to report it instead.
    """
    source = source_of(death)
    text = f"{label}: its {thread} thread died ({death!r}); this table no longer updates"
    message = library_bug(text) if source is ErrorSource.LIBRARY else f"{text}. {NEW_SESSION}"
    return MassiveError(message, source=source)


def _pascal_case(name: str) -> str:
    """Turn a snake-case field name into a PascalCase column name.

    Every underscore-separated word is capitalized and joined: `sip_timestamp` becomes
    `SipTimestamp`. Words that are already capitalized keep their other letters. The
    diagnostics tables name their columns with it.

    Args:
        name: A row field's name.

    Returns:
        The PascalCase column name.
    """
    return "".join(word[:1].upper() + word[1:] for word in name.split("_") if word)
