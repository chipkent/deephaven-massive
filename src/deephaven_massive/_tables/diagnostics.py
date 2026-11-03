"""The live diagnostics tables: the session's connections, streams, rate limits, and symbol reads.

They are the tables behind `m.connections()`, `m.streams()`, `m.rate_limits()`, and
`m.symbol_reads()`. Each is a keyed table that the library writes and users read: a blink
table that the library's writer adds rows to through its publisher (`blink_publisher`), and
the latest row of each key of it (`last_by` on the keys), which a later row of the key
replaces:

- **connections**: one row per WebSocket connection, plus one `Transport=REST` row per resolved
  (asset class, channel family) recording the host decision and its evidence. Keyed by
  `Transport`, `AssetClass`, and `Family`.
- **streams**: one row per stream (a live subscription with its own publisher), per `poll()`
  table, per background read (`wait=False`), and per followed read (a `history()` or `fetch()` that
  follows a refreshing symbol table), told apart by `Kind`, with the `repr` of the query that opened
  it in `Query`. Keyed by `StreamId`.
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
  its queue no longer than until the moment the pull names.

All of them spell a transport as every other text a user reads does (`TRANSPORT_LABELS`:
`WebSocket`, `REST`, `flat files`), in `Transport` and in a stream's `SchemaDrift` entries; the
rows hold the `Transport` member, and `as_row` writes its label. A stream's `Query` is cut to
`QUERY_SHOWN` characters (`shown_query`), as `Symbols` is cut to its first symbols, so a query
over a long list stays readable.

The tables are not input tables: nobody can edit them in the web UI, and the library never
deletes a row, so each keeps one row per key it was ever written (a stream ended long ago
keeps its last row), as the session's history of what it opened.

The tables live in one session-owned `LivenessScope`, created with the session on the caller
thread and released by `m.close()`. Users never get the tables themselves: `m.connections()`,
`m.streams()`, `m.rate_limits()`, and `m.symbol_reads()` return a view of one, tracked like a
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

The module also holds the session's `DriftRegistry`, the policy half of runtime schema drift.
The wire package detects drift (each decoder returns a `DriftReport` of the fields a payload had
beyond its documented schema, the documented fields it lacked, the fields that held a value that
does not fit their type, and the rows it dropped because a key could not be read); every
transport hands those reports to the registry, which logs one WARNING per (dataset, transport,
field) and per dataset with dropped rows, and keeps the `SchemaDrift` text of each stream and
connection, a column of both tables. Drift never fails a read: only a payload whose every row
was dropped fails, where it is found, as the sign of a change in Massive's format.
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

from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._datasets.schema import Column, ColumnType, Schema
from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._errors import LIBRARY_BUG, MassiveError
from deephaven_massive._massive import Transport
from deephaven_massive._tables.columns import col_defs
from deephaven_massive._tables.execution_context import wait_for_update_cycle
from deephaven_massive._tables.liveness import QueueWorker, new_scope, release_scope
from deephaven_massive._tables.publisher import blink_publisher
from deephaven_massive._tables.symbol_read_rows import (
    SYMBOL_READ_KEYS,
    SYMBOL_READS,
    SymbolReadRow,
    symbol_read_values,
)
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from collections.abc import Callable, Hashable, Mapping, Sequence

    from deephaven.execution_context import ExecutionContext
    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table

    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._enums import AssetClass, ChannelFamily, Recency
    from deephaven_massive._massive import DriftReport

__all__ = [
    "CONNECTIONS",
    "CONNECTION_KEYS",
    "DROPPED_COUNT_INTERVAL",
    "FLAT_FILES_AND_REST",
    "GAPS_SHOWN",
    "QUERY_SHOWN",
    "RATE_LIMITS",
    "RATE_LIMIT_KEYS",
    "SCHEMA_DRIFT_SHOWN",
    "STREAMS",
    "STREAM_KEYS",
    "WRITE_INTERVAL",
    "ConnectionRow",
    "ConnectionState",
    "Diagnostics",
    "DriftRegistry",
    "LimitSource",
    "NotePart",
    "RateLimitPull",
    "RateLimitRow",
    "RateLimitState",
    "StreamKind",
    "StreamRow",
    "StreamState",
    "StreamUpdate",
    "as_row",
    "describe_symbols",
    "gaps_text",
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

GAPS_SHOWN: Final = 5
"""How many of a live table's latest gaps the `Gaps` column lists; it counts the earlier ones."""

WRITE_INTERVAL: Final = 0.1
"""The fewest seconds between two batches of the diagnostics writer: what is queued meanwhile
is added as one batch, so a burst of rows costs each table one add per interval."""

DROPPED_COUNT_INTERVAL: Final = 60.0
"""The fewest seconds between two DEBUG lines of one dataset's running count of rows dropped
for a key (`DriftRegistry.report`): the same as a connection's traffic summary."""


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
            table fills once, when its download completes; or a followed read, a `history()`
            of a refreshing symbol table, whose table gains and drops each symbol's rows as the
            symbol table changes.
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


class NotePart(StrEnum):
    """One named part of a running stream's `Error`: a warning that leaves the stream running.

    Several sources warn about one stream independently: its connection (an outage, silence,
    the options cap, unacknowledged topics), its flush thread (`dm.ALL` symbols whose rows are
    dropped), its session's `symbol changes` worker (crypto pairs it cannot split), and a
    `poll()` table's poller (a failed cycle). Each sets or clears only its own
    part with `StreamUpdate.notes`, so no source replaces or clears another's warning, and the
    `Error` column joins the current parts in this order, with the skipped symbol-table values
    last. A stopped stream's `Error` shows only why it stopped. A note goes once its warning
    ends, so a warning about rows that may be missing also leaves a lasting record in the row's
    `Gaps` (`StreamUpdate.gap`).
    """

    OUTAGE = "outage"
    """The stream's connection is down, and its data has not flowed again since."""
    QUIET = "quiet"
    """The connection has received nothing for a while during market hours."""
    CAP = "cap"
    """The options contract cap defers some of the stream's contracts."""
    NO_ACK = "no_ack"
    """Massive has not acknowledged some of the stream's topics after a re-send."""
    STALE_SYMBOLS = "stale_symbols"
    """`dm.ALL` symbols whose rows are being dropped, so their last values may be stale."""
    SKIPPED_PAIRS = "skipped_pairs"
    """Crypto pairs a symbol table added that the stream skips: Massive's crypto ticker list
    does not split them, or could not be read."""
    POLL_CYCLE = "poll_cycle"
    """A `poll()` cycle failed; the next one runs as scheduled."""
    LEFT_OUT = "left_out"
    """Symbols the latest good `poll()` cycle left out: no path of theirs takes a parameter the
    poll sends, or Massive answered HTTP 404 for them."""
    RETRYING_SYMBOLS = "retrying_symbols"
    """A followed read's symbols whose read failed in a way that may pass, read again later,
    grouped by cause."""
    FAILED_SYMBOLS = "failed_symbols"
    """A followed read's symbols whose read failed for good, grouped by cause; deleting and
    re-adding a symbol's row reads it again."""


_NOTE_ORDER: Final = {part: index for index, part in enumerate(NotePart)}
"""Each `NotePart`'s place in a stream's `Error`."""

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
            ("Recency", _S),
            ("RowsPublished", _L),
            ("Loading", _I),
            ("RequestsPerCycle", _I),
            ("State", _S),
            ("Error", _S),
            ("Gaps", _S),
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
            (`channel:field+` for an extra field, `channel:field-` for a missing one,
            `channel:field?` for one that held a value that does not fit); null while there is
            none.
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
            (`REALTIME` or `DELAYED`; `UNKNOWN` on a pinned host the library does not know);
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
        error: Why the stream stopped, once it has; null while it runs, when the `Error`
            column shows its `notes` and `skipped` instead (`as_row`).
        created_at: When the stream was created.
        schema_drift: The stream's drift, as `DriftRegistry.record` writes it
            (`transport:field+` for an extra field, `transport:field-` for a missing one,
            `transport:field?` for one that held a value that does not fit); null while there
            is none. Separate from `error`, so drift never hides a failure.
        skipped: The symbol-table values the stream or poll skips because they are not
            Massive's tickers (`skipped_rows_text` in `_tables/watchlist.py`); null while none
            is. Not a column of its own: the `Error` column shows it after `notes` while the
            stream runs (`as_row`).
        notes: The current warnings that leave the stream running, each a named part, in
            `NotePart` order. Not a column of its own: the `Error` column joins them.
        gaps: Every time span over which the stream's rows may be missing, oldest first: a
            WebSocket outage it recovered from (from the last frame before the drop to the
            data's return), or a run of failed `poll()` cycles before a good one (from the
            start of the run's first failed cycle, which published nothing, to the good
            cycle's start). Each is a pair of epoch
            nanoseconds. Not a column of its own: the `Gaps` column shows them (`gaps_text`),
            on a stopped stream too, so the record outlasts the `Error` note that announced
            each one.
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
    schema_drift: str | None = None
    skipped: str | None = dataclasses.field(default=None, metadata={_NOT_A_COLUMN: True})
    notes: tuple[tuple[NotePart, str], ...] = dataclasses.field(
        default=(), metadata={_NOT_A_COLUMN: True}
    )
    gaps: tuple[tuple[int, int], ...] = dataclasses.field(
        default=(), metadata={_NOT_A_COLUMN: True}
    )


@dataclass(frozen=True, kw_only=True, slots=True)
class StreamUpdate:
    """A change to one stream's diagnostics row, for `SessionCore.update_streams`.

    Built by the WebSocket transport's flush thread after each flush, by a `poll()` table's poller
    after each cycle, by a connection's loop thread, and by the follow coordinator for a followed
    read; every field left as None keeps the row's current value. It never changes `State` or the
    failure: only `SessionCore.fail_stream` fails a stream. A warning is one named part of the
    `Error` (`notes`): an update sets or clears only the parts it names, so the other sources'
    warnings stay (for example, an outage's note leaves the options-cap warning in place).

    Attributes:
        rows_published: The stream's total rows published so far.
        requests_per_cycle: The REST requests of the latest `poll()` cycle.
        deferred: The stream's own options topics the cap defers (one per contract, since a
            stream reads one channel).
        symbol_count: How many symbols the stream now covers.
        symbols: The readable, truncated list of those symbols, as `describe_symbols` writes it.
        recency: What the latest `poll()` cycle's labels showed (`StreamRow.recency`).
        notes: The warning parts this update sets (to a text) or clears (to None); the parts
            it leaves out keep their text.
        skipped: The skipped symbol-table values' text (`StreamRow.skipped`).
        clear_skipped: True to set that text back to null; takes precedence over `skipped`.
        gap: A gap that has just ended, `(start, end)` in epoch nanoseconds, appended to the
            row's `gaps`; None for none. A gap is recorded on a stopped row too: it is history.
        loading: A followed read's count of symbols queued, loading, or waiting to retry.
        transport: A followed read's sources so far (`StreamRow.transport`).

    `SchemaDrift` is not set here: `SessionCore.record_stream_drift` records a stream's drift
    and writes its row in one step, so concurrent reports cannot leave an older text behind.
    """

    rows_published: int | None = None
    requests_per_cycle: int | None = None
    deferred: int | None = None
    symbol_count: int | None = None
    symbols: str | None = None
    recency: Recency | None = None
    notes: Mapping[NotePart, str | None] | None = None
    skipped: str | None = None
    clear_skipped: bool = False
    gap: tuple[int, int] | None = None
    loading: int | None = None
    transport: Transport | str | None = None

    def apply(self, row: StreamRow) -> StreamRow:
        """Return `row` with the fields this update sets; the others keep their values.

        The `Error` of a `FAILED` or `CLOSED` row names why the stream stopped, and its notes
        are not shown, so a flush or a poll cycle that races the stop never replaces the cause.

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
            notes=self._notes(row),
            skipped=None
            if self.clear_skipped
            else (row.skipped if self.skipped is None else self.skipped),
            gaps=row.gaps if self.gap is None else (*row.gaps, self.gap),
            loading=row.loading if self.loading is None else self.loading,
            transport=row.transport if self.transport is None else self.transport,
        )

    def _notes(self, row: StreamRow) -> tuple[tuple[NotePart, str], ...]:
        """Return the row's warning parts with this update's set or cleared.

        Args:
            row: The stream's current row.

        Returns:
            The parts, in `NotePart` order.
        """
        if not self.notes:
            return row.notes
        notes = dict(row.notes)
        for part, text in self.notes.items():
            if text is None:
                notes.pop(part, None)
            else:
                notes[part] = text
        return tuple(sorted(notes.items(), key=lambda item: _NOTE_ORDER[item[0]]))


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


def as_row(row: ConnectionRow | StreamRow | RateLimitRow | SymbolReadRow) -> dict[str, object]:
    """Return a row keyed by column name: each field's name in PascalCase.

    A `Transport` value becomes its user-facing label (`TRANSPORT_LABELS`), so the tables spell
    transports as `columns()` does: `Transport.WS` is shown as `WebSocket`. A running stream's
    `Error` joins its warning parts (`StreamRow.notes`) and then the skipped symbol-table values,
    with `"; "`, or is null when there is none; a stopped stream's shows only why it stopped.
    A stream's `Gaps` is its `gaps` as `gaps_text` writes them, whatever its state.

    A symbol-read row is written by `symbol_read_values`.

    Args:
        row: A connection, stream, rate-limit, or symbol-read row.

    Returns:
        A `{column: value}` mapping for `CONNECTIONS`, `STREAMS`, `RATE_LIMITS`, or
        `SYMBOL_READS`.
    """
    if isinstance(row, SymbolReadRow):
        return symbol_read_values(row)
    shown = {
        pascal_case(f.name): _shown(getattr(row, f.name))
        for f in fields(row)
        if not f.metadata.get(_NOT_A_COLUMN)
    }
    if not isinstance(row, StreamRow):
        return shown
    shown["Gaps"] = gaps_text(row.gaps)
    if row.state not in _STOPPED:
        texts = [text for _, text in row.notes]
        if row.skipped is not None:
            texts.append(row.skipped)
        shown["Error"] = "; ".join(texts) if texts else row.error
    return {name: shown[name] for name in STREAMS.column_names}  # `Gaps` in its place


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


def gaps_text(gaps: Sequence[tuple[int, int]]) -> str | None:
    """Return a live table's gaps as its `Gaps` column shows them.

    Args:
        gaps: Each gap's start and end, epoch nanoseconds, oldest first (`StreamRow.gaps`).

    Returns:
        None for no gap. Otherwise each of the latest `GAPS_SHOWN` gaps as `<start> to <end>`
        in UTC to the second, joined by `"; "`, after `"<n> earlier gaps; "` when there are
        more, such as `"2026-11-02T14:03:05Z to 2026-11-02T14:05:10Z"`.
    """
    if not gaps:
        return None
    shown = [f"{utc_text(start / 1e9)} to {utc_text(end / 1e9)}" for start, end in gaps]
    earlier = len(shown) - GAPS_SHOWN
    text = "; ".join(shown[-GAPS_SHOWN:])
    if earlier <= 0:
        return text
    return f"{earlier} earlier {'gap' if earlier == 1 else 'gaps'}; {text}"


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


_EXTRA: Final = "is not in the schema; it is dropped"
"""What a drift WARNING says of a field the schema does not declare."""
_MISSING: Final = "is missing; it reads as null"
"""What a drift WARNING says of a vendor-required field a payload lacked."""
_UNCONVERTIBLE: Final = "held a value that does not fit its type; it reads as null"
"""What a drift WARNING says of a field that held a value its type cannot hold."""
_ROUNDED: Final = (
    "held a whole number beyond 2**53, which its double column holds only rounded; it reads rounded"
)
"""What a drift WARNING says of a `long` field whose widening to a `double` rounded a value."""


def _rows(count: int) -> str:
    """Return `"row"` for one and `"rows"` for any other count, for a `SchemaDrift` entry.

    Args:
        count: The number of rows.

    Returns:
        The noun.
    """
    return "row" if count == 1 else "rows"


def _drift_context(drift: DriftReport, field: str, *, where: str, symbol: str | None) -> str:
    """Return where a drifted field was seen, for its WARNING.

    Args:
        drift: The payload's drift, with its samples.
        field: The drifted wire key.
        where: Where the payload came from.
        symbol: The payload's symbol when its sample has none, or None.

    Returns:
        `where`, then `; symbol <symbol>` when one is known and `; first value <value>` for an
        extra field's or an odd value's sample, such as
        `"wss://h/stocks channel T; symbol AAPL; first value 1"`.
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
    regenerated. The wire decoders, and the timestamp routine after them, report each
    payload's differences as a `DriftReport`: the fields it had that the schema does not
    declare (dropped), the declared fields it lacked (null), the fields that held a value that
    does not fit their type (null), the `long` fields whose widening into a `double` column
    rounded a whole number beyond 2**53 (served rounded), and the rows dropped because a key
    could not be read. Every transport routes those reports here:

    - `report` logs one WARNING per (dataset, transport, field) for the life of the session,
      and one WARNING per dataset the first time a row of it is dropped, with the running count
      of dropped rows at DEBUG after that, at most once a minute. A static verb (`fetch()`,
      `history()`) reports drift only this way.
    - `report_statuses` logs one WARNING per (REST path, status) for a REST envelope status
      outside the documented ones; the page's rows are kept.
    - `record` adds a report to the drift of one stream or connection, its owner, and returns
      the owner's new `SchemaDrift` text when it changed, for the caller to write into the
      owner's diagnostics row: its drifted fields, and how many of its rows were dropped. So
      a dropped row stays visible after its one WARNING. `forget` drops an owner once it has
      left its registry.

    Drift is never a failure: one odd value or row never costs the rest of a payload. Only a
    payload whose every row was dropped fails where it is found, before it reaches the
    registry, since that is the sign of a change in Massive's format.

    `SessionCore` creates one per session, and any library thread calls it. Thread-safe: one
    leaf lock guards its sets and counts, held for set operations and for building a text,
    never for logging, a Deephaven call, or a wait. `record` and `forget` may therefore be
    called with the session's `registry_lock` held. `report` and `report_statuses` must not
    be: they log, and a log handler may do I/O.

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

            drift = DriftReport(extra=frozenset({"halted"}), unconvertible=frozenset({"x"}))
            registry = DriftRegistry()
            where = "GET /v3/trades/{stockTicker}"
            registry.report("stocks.trades", Transport.REST, drift, where=where)  # two WARNINGs
            registry.report("stocks.trades", Transport.REST, drift, where=where)  # no log
            registry.record(7, "REST", drift)  # "REST:halted+, REST:x?"
            registry.record(7, "REST", drift)  # unchanged: None
            registry.record(7, "REST", DriftReport(dropped_rows=2))
            # "REST:2 rows dropped, REST:halted+, REST:x?"
    """

    __slots__ = (
        "_dropped",
        "_dropped_logged",
        "_lock",
        "_owner_dropped",
        "_owners",
        "_statuses",
        "_warned",
    )

    def __init__(self) -> None:
        """Start with no drift seen."""
        self._lock = threading.Lock()
        self._warned: set[tuple[str, Transport, str]] = set()
        self._statuses: set[tuple[str, str]] = set()
        self._owners: dict[Hashable, frozenset[str]] = {}
        # Per owner, the rows dropped so far on each of its prefixes (transports or channels).
        self._owner_dropped: dict[Hashable, dict[str, int]] = {}
        self._dropped: dict[str, int] = {}
        # Per dataset, when its dropped rows were last logged (monotonic seconds), and the
        # running count that line showed.
        self._dropped_logged: dict[str, tuple[float, int]] = {}

    def report(
        self,
        dataset: str,
        transport: Transport,
        drift: DriftReport,
        *,
        where: str,
        symbol: str | None = None,
    ) -> None:
        """Log a WARNING for each field of `drift` not yet reported, and count dropped rows.

        Each field's WARNING names the dataset, the transport, and the field, then where the
        payload came from, the symbol of the first row that showed the field, and for an extra
        field or a value that does not fit that row's value (`DriftReport.samples`), so the
        log says enough to reproduce it: `... (schema drift) at wss://delayed.massive.com/stocks
        channel T; symbol AAPL; first value 182.33`. A field is reported once per session and
        dataset; the first sample is the one a fix needs. The first rows of a dataset dropped
        because a key could not be read log one WARNING with the reason of the first of them;
        later drops log the running count of the dataset's dropped rows at DEBUG, at most once
        every `DROPPED_COUNT_INTERVAL` seconds per dataset, with the rows dropped since the
        last such line: drops between are counted, not logged. Ignores `drift.statuses`; see
        `report_statuses`.

        Args:
            dataset: The dataset name, such as `"stocks.trades"`.
            transport: The transport the payload arrived on.
            drift: The payload's drift, as a wire decoder returned it, with the drift of its
                timestamps added.
            where: Where the payload came from: the REST request (`"GET /v3/trades/AAPL"`),
                the WebSocket URL and channel, or the flat file.
            symbol: The symbol the payload is about when its sample cannot say (a REST
                request for one symbol), or None.
        """
        changes = [(field, _EXTRA) for field in drift.extra]
        changes += [(field, _MISSING) for field in drift.missing]
        changes += [(field, _UNCONVERTIBLE) for field in drift.unconvertible]
        changes += [(field, _ROUNDED) for field in drift.rounded]
        now = time.monotonic()
        with self._lock:
            new = [(f, kind) for f, kind in changes if (dataset, transport, f) not in self._warned]
            self._warned.update((dataset, transport, f) for f, _ in new)
            before = self._dropped.get(dataset, 0)
            total = before + drift.dropped_rows
            logged_at, logged_total = self._dropped_logged.get(dataset, (now, 0))
            due = bool(drift.dropped_rows) and (
                not before or now - logged_at >= DROPPED_COUNT_INTERVAL
            )
            if drift.dropped_rows:
                self._dropped[dataset] = total
            if due:
                self._dropped_logged[dataset] = (now, total)
        for field, kind in sorted(new):
            context = _drift_context(drift, field, where=where, symbol=symbol)
            logger.warning(
                "%s: %s field %r %s (schema drift) at %s",
                dataset,
                transport.value,
                field,
                kind,
                context,
            )
        if drift.dropped_rows and not before:
            logger.warning(
                "%s: dropped %d %s row(s) whose key cannot be read, the first at %s: %s; such "
                "rows are dropped and counted: the running count is logged at DEBUG and shown "
                "in the SchemaDrift of each live table that lost them",
                dataset,
                drift.dropped_rows,
                transport.value,
                where,
                drift.dropped_reason,
            )
        elif due:
            logger.debug(
                "%s: dropped %d more row(s) whose key cannot be read in the last %.0f s (%d so "
                "far); the latest %s at %s: %s",
                dataset,
                total - logged_total,
                now - logged_at,
                total,
                transport.value,
                where,
                drift.dropped_reason,
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

        Each extra field becomes the entry `<prefix>:<field>+`, each missing one
        `<prefix>:<field>-`, each field that held a value that does not fit
        `<prefix>:<field>?`, and each `long` field whose widening to a `double` rounded a value
        `<prefix>:<field>~`. An owner's entries only accumulate: drift seen once stays shown.
        Dropped rows add one entry per prefix with the owner's running count,
        `<prefix>:<n> rows dropped`, which sorts before the prefix's fields, so a long text cut
        at `SCHEMA_DRIFT_SHOWN` keeps it.

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
        entries |= {f"{prefix}:{field}?" for field in drift.unconvertible}
        entries |= {f"{prefix}:{field}~" for field in drift.rounded}
        with self._lock:
            seen = self._owners.get(owner, frozenset())
            if entries <= seen and not drift.dropped_rows:
                return None
            seen |= entries
            self._owners[owner] = seen
            dropped = self._owner_dropped.setdefault(owner, {})
            if drift.dropped_rows:
                dropped[prefix] = dropped.get(prefix, 0) + drift.dropped_rows
            counts = {f"{name}:{count} {_rows(count)} dropped" for name, count in dropped.items()}
            # Built under the lock, so the text is the owner's entries at this moment.
            text = ", ".join(sorted(seen | counts))
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
            self._owner_dropped.pop(owner, None)


RateLimitPull: TypeAlias = "Callable[[], tuple[Sequence[RateLimitRow], float | None]]"
"""Returns the `m.rate_limits()` rows and the seconds until they change with time alone."""

_RATE_LIMITS_CHANGED: Final = object()
"""The queue item that asks the writer to pull fresh `m.rate_limits()` rows."""

_Item: TypeAlias = "ConnectionRow | StreamRow | SymbolReadRow | threading.Event | object"


class _KeyedTable:
    """One diagnostics table: a blink table's publisher and the latest row per key of it.

    The writer adds each batch of rows to the blink table through its publisher, and the table
    users see is `last_by` of the blink table on the keys: it keeps the latest row of each key,
    copied out of the blink rows, so a later row of a key replaces the earlier one as a modify
    (also when two adds of one key land in one update cycle), and a new key is appended after
    the keys seen before it. Rows are never deleted, so the table keeps one row per key ever
    written. It is not an input table: nobody can edit it in the web UI, and no row can be
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
        blink, self.publisher = blink_publisher(
            name,
            col_defs(schema),
            on_shutdown=functools.partial(_log_shut_down, name),
            release_later=release_later,
        )
        self.table: Final = blink.last_by(list(keys))

    def add(self, rows: Sequence[ConnectionRow | StreamRow | RateLimitRow | SymbolReadRow]) -> None:
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
    """The session's four diagnostics tables and the single thread that writes them.

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
    never blocking) asks the writer to pull them again, at most once per batch.

    `release()` lets the writer add the rows already queued, release the scope, and exit, and
    waits for it unless told not to (a caller that holds the update graph's lock must not: the
    writer releases the scope under that lock); `stop()` does the same without waiting and
    without taking any lock, for use from a finalizer. A writer that dies on something it
    cannot carry on after calls `on_death` first (the session fails its live tables there).
    """

    __slots__ = (
        "_connections",
        "_exec_ctx",
        "_rate_due",
        "_rate_limits",
        "_rate_pending",
        "_rate_pull",
        "_release_lock",
        "_released",
        "_scope",
        "_streams",
        "_symbol_reads",
        "_writer",
    )

    def __init__(
        self,
        exec_ctx: ExecutionContext,
        name: str,
        *,
        release_later: Callable[[LivenessScope], None],
        on_death: Callable[[BaseException], None] | None = None,
    ) -> None:
        """Create the four tables inside a new session-owned scope and start the writer.

        Each blink table is created under the update graph's shared lock (`blink_publisher`),
        so this waits for a running update cycle to end; it must not run on an update-graph
        thread or under a library lock. If creating a table or starting the writer fails, the
        scope (with any table already created in it) is released and the exception propagates.

        Args:
            exec_ctx: The session's execution context, entered by the writer thread.
            name: The writer thread's name, for thread dumps, such as
                `massive #1 diagnostics`; each table's publisher adds the table's name.
            release_later: Releases a scope on the session's reaper, under the update graph's
                shared lock (`SessionCore.release_later`), for the blink tables' publishers.
            on_death: Runs if the writer dies on something it cannot carry on after
                (`QueueWorker`).
        """
        self._exec_ctx = exec_ctx
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
            self._writer: QueueWorker[_Item] = QueueWorker(
                exec_ctx,
                name,
                handle=self._apply,
                on_exit=functools.partial(release_scope, exec_ctx, self._scope),
                timeout=self._rate_wait,
                on_timeout=self._rate_due_now,
                on_death=on_death,
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
    def thread(self) -> threading.Thread:
        """The writer thread."""
        return self._writer.thread

    def write(self, row: ConnectionRow | StreamRow | SymbolReadRow) -> None:
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

    def stop(self) -> None:
        """Ask the writer to add the queued rows, release the tables, and exit; no wait.

        Takes no lock, so the session's finalizer can call it from garbage collection. Later
        rows and rate-limit notices are not queued, as after `release()`.
        """
        self._released = True  # one attribute store; `write` and the rest read it under a lock
        self._writer.stop()

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

    def _apply(self, items: list[_Item]) -> None:
        """Add one batch: the latest row per key, one `add` per table, then mark the waits.

        The rate-limit rows are pulled when a marker is in the batch, or when their
        time-driven change has come, so a queue that never goes quiet cannot hold them back.

        Args:
            items: Rows, `wait_until_shown` events, and rate-limit markers, in queue order.
        """
        streams: dict[int, StreamRow] = {}
        connections: dict[tuple[str, str, str], ConnectionRow] = {}
        symbol_reads: dict[tuple[int, str], SymbolReadRow] = {}
        added: list[threading.Event] = []
        # The rows' time-driven change is due: a queue too busy for the timed wait to expire
        # must not starve that pull.
        due = self._rate_due
        pull = due is not None and time.monotonic() >= due
        for item in items:
            if isinstance(item, threading.Event):
                added.append(item)
            elif isinstance(item, StreamRow):
                streams[item.stream_id] = item
            elif isinstance(item, ConnectionRow):
                connections[item.transport, item.asset_class, item.family] = item
            elif isinstance(item, SymbolReadRow):
                symbol_reads[item.stream_id, item.symbol] = item
            else:
                pull = True
        try:
            for table, rows in (
                (self._streams, lambda: list(streams.values())),
                (self._connections, lambda: list(connections.values())),
                (self._rate_limits, self._pull_rate_limits if pull else list),
                (self._symbol_reads, lambda: list(symbol_reads.values())),
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
            for event in added:
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
