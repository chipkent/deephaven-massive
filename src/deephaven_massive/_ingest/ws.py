"""The WebSocket transport: connections over `WsClient`, shared by sessions, and their streams.

`WsTransport` is the session's `StreamTransport`, over the session's one `WsConnections`. A
stream verb (`blink()`, `append_only()`, `ring()`, `last_by()`) that finds no live stream for
its `(dataset, channel, symbols)` asks it to open one: the transport decides the host of the
class and family (`RestTransport.resolve_host`, once per session), finds or creates the
connection to `wss://<host>/<cluster>`, creates the stream's tables
(`_tables/stream.py`), and adds the stream's topics to the connection. The wire itself (the
socket, authentication, frame decoding, reconnecting with backoff) belongs to the connection's
`_massive.WsClient`; the subscription protocol to `Subscriptions` (`_ingest/subscriptions.py`);
everything else the library decides belongs here:

- **Refusals.** A refused topic fails exactly the streams that want it, with
  `EntitlementError`; nothing tears down the connection. A refusal that names no topic
  applies to the whole step in flight, unless it is the server's real-time verdict on a
  `socket.` connection whose recency was unknown: then the decision is recorded with the REST
  transport (`RestTransport.record_verdict`), the connection's streams move to the `delayed.`
  host, and they keep running.
- **Rows, rejects, and drift.** Rows go to the connection's flush thread (`Flusher`), which
  publishes them. A rejected row fails the streams that would have received it, its raw wire
  symbol mapped to the canonical one (`_datasets/symbols.py`); a row with no symbol fails only
  the channel's `dm.ALL` streams and is logged at ERROR. Drift is recorded on the connection's
  `SchemaDrift` here and handed to the flush thread for the streams' rows.
- **The options cap.** An `/options` connection carries at most `OPTIONS_CAP` explicit topics.
  A static symbol list that would cross it raises `ValueError` when the verb is called; a
  reactive table's excess topics are deferred, in sorted order, and admitted as room frees.
- **Sharing.** Sessions in one process with the same API key and equal connection settings
  (`CONNECTION_SETTINGS`, and the same Deephaven update graph) share one `ConnectionGroup`, so
  their streams ride the same connections: individual plans allow one connection per cluster,
  and two sessions with connections of their own would knock each other off. A connection is
  leased per stream, and remembers which sessions use it, so each session's `m.connections()`
  shows it, each stream reports to its own session, and a session's close takes only its own
  streams away. The options cap, refusals, fatal failures, and the real-time verdict are the
  connection's, so they act on every session's streams on it.
- **Lifetime.** A connection with no stream attached for `IDLE_GRACE` seconds closes itself
  and leaves the registry. A fatal failure (a rejected key, a handshake error retrying cannot
  fix) fails every stream on it, a stream opening at that moment included. `m.close()` removes
  the session's streams from their connections, closes every connection no other session
  leases, and waits a bounded time for its threads. While a connection waits for a slot
  (`WAITING_SLOT`), each of its streams says so in its `Error`.

**Threads and locks.** Each connection's subscription state is confined to its client's loop
thread; other threads reach it only through `loop.call_soon_threadsafe` with plain data
(`Connection.post`). The registries that map URLs to connections and streams to connections,
each connection's leases, sessions, failure, and options-cap bookkeeping, each session's
registry state here, and the group's record of `NO_ACK` topics are guarded by the group's lock,
a leaf lock like the sessions' `registry_lock` (the two are never held together): no Deephaven
call, network call, or wait happens under it (creating a client loads TLS settings, so it
happens outside). The loop thread makes no Java call: every failure goes to a flush thread, or,
for a stream whose connection failed while it opened, to the opening caller. A stream's
blink-table shutdown callback takes the locks only to update the registries and then posts the
release of the stream's topics to its connection's loop; it never sends and never calls Java.
Nothing here holds a session's core strongly: the transport, the group, the connections, and
the streams' sinks refer to it weakly, so a session with no stream left, dropped without
`close()`, is freed by reference counting and its closer runs.
"""

from __future__ import annotations

import functools
import hashlib
import itertools
import logging
import threading
import time
import weakref
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Protocol, cast

from deephaven_massive import _massive
from deephaven_massive._datasets.request import HostDecision
from deephaven_massive._datasets.symbols import SymbolTable, canonical_from_wire
from deephaven_massive._enums import AssetClass, ChannelFamily, Recency
from deephaven_massive._errors import EntitlementError, MassiveError, from_wire
from deephaven_massive._ingest.futures_calendar import FuturesCalendar, RowFetch
from deephaven_massive._ingest.subscriptions import (
    Sender,
    Subscriptions,
    Timer,
    channel_of,
    is_wildcard,
    stream_topics,
    wildcard,
)
from deephaven_massive._massive import Cancel, Transport, WireAuthError, WsState
from deephaven_massive._tables.diagnostics import (
    ConnectionRow,
    ConnectionState,
    DriftRegistry,
    StreamUpdate,
    describe_symbols,
)
from deephaven_massive._tables.stream import ChannelPlan, Flusher, StreamSink, open_stream_tables
from deephaven_massive._tables.watchlist import read_symbols
from deephaven_massive._types import _All

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Callable, Collection, Hashable, Iterable, Sequence

    from deephaven.execution_context import ExecutionContext

    from deephaven_massive._config import MassiveConfig
    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._massive import DriftReport, Reject, Source, Status, WireError
    from deephaven_massive._tables.core import SessionCore
    from deephaven_massive._tables.liveness import StreamHandle
    from deephaven_massive._tables.stream import FuturesSessionDates

__all__ = [
    "CONNECTION_SETTINGS",
    "IDLE_GRACE",
    "JOIN_TIMEOUT",
    "OPTIONS_CAP",
    "ClientFactory",
    "Connection",
    "ConnectionGroup",
    "WsConnections",
    "WsTransport",
]

logger = logging.getLogger(__name__)

IDLE_GRACE: Final = 30.0
"""Seconds a connection with no stream attached stays open before it closes itself."""

OPTIONS_CAP: Final = 1000
"""The most explicit topics one `/options` connection carries. Massive allows 1,000 option
contracts per connection; whether it counts contracts or topics is not documented, so the
library counts topics (`T.X` and `Q.X` are two), the stricter reading. Wildcards count zero."""

JOIN_TIMEOUT: Final = 5.0
"""Seconds `m.close()` waits for each connection's threads before it logs a WARNING."""

_MESSAGE_ROW_NS: Final = 1_000_000_000
"""How often, at most, a connection's row is rewritten only to advance `LastMessageAt`."""

CONNECTION_SETTINGS: Final = (
    "api_key",
    "ws_hosts",
    "ack_timeout",
    "reconnect_backoff",
    "max_reconnect_backoff",
    "flush_interval",
    "rest_base_url",
)
"""The `MassiveConfig` fields that shape a group's WebSocket connections. A connection carries
one value of each, so only sessions whose values are all equal share connections
(`ConnectionGroup`). `rest_base_url` is one because the group's futures schedule calendar reads
through any of its sessions' REST clients, which must all read the same API."""

_group_numbers: Final = itertools.count(1)
"""Numbers the connection groups of the process, for their threads' names."""

_BACKLOG_ROW_DELAY: Final = 1.0
"""Seconds after a connection's row showed buffered rows that it is written again."""


class _Client(Sender, Protocol):
    """What a connection needs of its client: `WsClient`'s interface."""

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """The client's event loop."""
        ...

    def start(self) -> None:
        """Start the client's thread."""
        ...

    def close(self) -> None:
        """Ask the client to stop, from any thread, without waiting."""
        ...

    def join(self, timeout: float) -> bool:
        """Wait a bounded time for the client's thread to end."""
        ...

    def set_required(
        self, channel: str, keys: frozenset[str], *, symbol: str | None = None
    ) -> None:
        """Declare a channel's required key columns and its symbol column."""
        ...


class ClientFactory(Protocol):
    """Creates the client of one connection; `Connection` passes its callbacks."""

    def __call__(
        self,
        url: str,
        *,
        sources: Sequence[Source],
        on_open: Callable[[int], None],
        on_status: Callable[[int, Status], None],
        on_data: Callable[[int, str, list[dict[str, object]], list[Reject], DriftReport], None],
        on_state: Callable[[WsState, float | WireError | None], None],
    ) -> _Client:
        """Return a new, unstarted client of `url` with the given callbacks.

        Args:
            url: The endpoint, `wss://<host>/<cluster>`.
            sources: The WebSocket sources of the cluster.
            on_open: The connection's `on_open`.
            on_status: The connection's `on_status`.
            on_data: The connection's `on_data`.
            on_state: The connection's `on_state`.
        """
        ...


@dataclass(slots=True)
class _Stream:
    """One stream as its connection's loop thread knows it. Loop-confined once attached.

    Attributes:
        stream_id: The stream's id.
        channel: Its channel.
        plan: How its dataset reads the channel.
        sink: The stream as the flush thread publishes to it; it carries the stream's session.
        symbols: The canonical symbols it wants; None for `dm.ALL`.
        topics: Its topics now.
        static: True for `dm.ALL` and a symbol list, False for a reactive symbol table, whose
            topics the options cap may defer.
    """

    stream_id: int
    channel: str
    plan: ChannelPlan
    sink: StreamSink
    symbols: frozenset[str] | None
    topics: frozenset[str]
    static: bool


class Connection:
    """A connection to one `wss://<host>/<cluster>`, its flush thread, and its streams.

    Created by a `WsConnections` for its `ConnectionGroup` (outside every lock: creating the
    client loads TLS settings) and registered under the group's lock; started by its creator,
    or after a delay when it replaces a connection the server's real-time verdict moved. The
    sessions of the group share it: `users` counts each session's leases, and each stream
    carries its own session. Its loop-thread state (the `Subscriptions`, the streams, the
    diagnostics row) is touched only by the client's loop thread, which runs the callbacks below
    and the work other threads `post`. `families`, `leases`, `users`, `retired`, `failure`,
    `awaiting`, and the options-cap fields are guarded by the group's lock. `close()` and
    `join()` may be called from any thread.

    It writes its diagnostics row (and one per further family it serves) into the
    `m.connections()` of each session in `users`, under the group's lock and only while it is
    that session's current connection for the key, so a connection that left the registry
    never overwrites the row of the one that replaced it. A session stays in `users`, also with
    no lease left, until it closes, and through the close of a connection no other session
    leases, so it sees the connection's last state, `CLOSED` included.
    """

    def __init__(
        self,
        group: ConnectionGroup,
        *,
        exec_ctx: ExecutionContext,
        url: str,
        asset_class: AssetClass,
        family: ChannelFamily,
        decision: HostDecision,
        sessions: FuturesSessionDates | None,
    ) -> None:
        """Create the connection, its client, and its flush thread; nothing starts yet.

        Args:
            group: The group that owns it (its settings, registry, and the verdict).
            exec_ctx: The execution context of the session creating it, which its flush
                thread enters; the group's sessions share one update graph.
            url: The endpoint, `wss://<host>/<cluster>`.
            asset_class: The cluster's class.
            family: The family of the stream that created it.
            decision: The host decision behind `url`.
            sessions: For a futures connection, the group's schedule calendar, which corrects
                its rows' session dates; None otherwise.
        """
        self.url: Final = url
        self.asset_class: Final = asset_class
        self.decision: Final = decision
        self.path: Final = f"/{asset_class.value}"
        self._key: Final = (Transport.WS.value, asset_class.value, family.value)
        self.exec_ctx: Final = exec_ctx
        self._group = group
        self._lock: Final = group.lock
        # Guarded by the group's lock.
        self.families: set[ChannelFamily] = {family}
        self.leases = 0
        self.users: dict[WsConnections, int] = {}
        self.retired = False
        self.failure: MassiveError | None = None
        self.awaiting: set[int] = set()
        self.cap_admitted: frozenset[str] = frozenset()
        self.cap_pending: dict[int, frozenset[str]] = {}
        # Start and close bookkeeping, under its own leaf lock.
        self._start_lock = threading.Lock()
        self._started = False
        self._closing = False
        self._timer: threading.Timer | None = None
        self.sessions: Final = sessions
        self.flusher: Final = Flusher(
            exec_ctx,
            f"massive-flush {decision.host}{self.path}",
            interval=group.flush_interval,
            sessions=sessions,
        )
        cluster = f"{asset_class.value}/"
        sources = [s for k, s in _massive.sources(Transport.WS).items() if k.startswith(cluster)]
        self._client = group.factory(
            url,
            sources=sources,
            on_open=self._on_open,
            on_status=self._on_status,
            # Rows of every generation are delivered alike, so the generation is not needed.
            on_data=lambda _generation, channel, rows, rejects, drift: self._on_data(
                channel, rows, rejects=rejects, drift=drift
            ),
            on_state=self._on_state,
        )
        self.subscriptions: Final = Subscriptions(
            self._client,
            call_later=self._client.loop.call_later,
            ack_timeout=group.ack_timeout,
            cap=OPTIONS_CAP if asset_class is AssetClass.OPTIONS else None,
            on_refused=self._refused,
            on_step_refused=self._step_refused,
            first_no_ack=functools.partial(group.first_no_ack, self.path),
            on_change=self._changed,
        )
        # Loop-confined from here on.
        self._streams: dict[int, _Stream] = {}
        self._early: dict[int, list[tuple[Callable[..., None], tuple[object, ...]]]] = {}
        self._required: dict[str, frozenset[str]] = {}
        self._idle: Timer | None = None
        self._successor: Connection | None = None
        self._deferred_shown: dict[int, int] = {}
        self._state = ConnectionState.CONNECTING
        self._reconnects = 0
        self._last_message_at: int | None = None
        self._message_row_at = 0
        self._next_reconnect_at: int | None = None
        self._last_error: str | None = None
        self._drift: str | None = None
        self._drift_seen: Final = DriftRegistry()  # the connection's own SchemaDrift text
        self._stalled = False
        self._ended = False
        self._moving = False
        self._backlog_timer: Timer | None = None
        self._change_scheduled = False

    @property
    def closing(self) -> bool:
        """True once `close()` was called."""
        with self._start_lock:
            return self._closing

    def take(self, owner: WsConnections) -> None:
        """Add one lease for a stream of a session; the group's lock is held.

        Args:
            owner: The stream's session.
        """
        self.leases += 1
        self.users[owner] = self.users.get(owner, 0) + 1

    def give(self, owner: WsConnections) -> None:
        """Return one lease of a session; the group's lock is held. The session stays a user.

        Args:
            owner: The stream's session.
        """
        self.leases -= 1
        self.users[owner] -= 1

    @property
    def key(self) -> tuple[str, str, str]:
        """The connection's owner key in the drift registry: transport, class, and family.

        Fixed when the connection is created, from the family of the stream that created it,
        so a stream of another family that joins later (an FMV stream on a `business.`
        connection opened for trades) never moves the connection's drift to a new owner.
        """
        return self._key

    # --- lifecycle (any thread) ----------------------------------------------------------------

    def start(self, delay: float = 0.0) -> None:
        """Start the flush thread and the client, now or after `delay` seconds.

        Does nothing once `close()` was called, or once started. The delayed start runs on a
        daemon timer thread, which `close()` cancels.

        Args:
            delay: Seconds to wait first; the backoff when the connection replaces one the
                server's real-time verdict moved.
        """
        with self._start_lock:
            if self._closing or self._started or self._timer is not None:
                return
            if delay <= 0:
                self._start_locked()
                return
            self._timer = threading.Timer(delay, self._start_later)
            self._timer.daemon = True
            self._timer.start()

    def _start_later(self) -> None:
        """The delayed start, on the timer thread."""
        with self._start_lock:
            if not self._closing and not self._started:
                self._start_locked()

    def _start_locked(self) -> None:
        """Start both threads; the start lock is held."""
        self._started = True
        self.flusher.start()
        self._client.start()

    def close(self, *, final_flush: bool = False) -> None:
        """Close the client and stop the flush thread. Any thread; never waits; idempotent.

        A client that never started is started after its close request, so its thread ends at
        once and closes its loop, as `WsClient` requires.

        Args:
            final_flush: True to have the flush thread apply what it has queued (failures
                included) before it ends; False when the session closes, which has already
                failed its tables.
        """
        with self._start_lock:
            self._closing = True
            timer, self._timer = self._timer, None
            self._client.close()
            if not self._started:
                self._started = True
                self._client.start()
        if timer is not None:
            timer.cancel()
        self.flusher.stop(final=final_flush)

    def join(self, timeout: float) -> bool:
        """Wait a bounded time for the client's and the flush thread's ends.

        Args:
            timeout: The longest wait for each thread, in seconds.

        Returns:
            Whether both have ended.
        """
        client = self._client.join(timeout)
        return self.flusher.join(timeout) and client

    def post(self, callback: Callable[..., None], *args: object) -> bool:
        """Run `callback(*args)` on the connection's loop thread. Any thread; never blocks.

        Args:
            callback: A loop-thread method of this connection.
            *args: Plain data.

        Returns:
            False when the loop has closed (the connection ended), so the callback never runs;
            the case is logged at INFO.
        """
        try:
            self._client.loop.call_soon_threadsafe(callback, *args)
        except RuntimeError:
            logger.info(
                "%s has closed; dropped %s", self.url, getattr(callback, "__name__", callback)
            )
            return False
        return True

    # --- loop thread: streams ------------------------------------------------------------------

    def add_stream(self, stream: _Stream) -> None:
        """Take a new stream's topics: declare its channel's keys, then reconcile.

        Ignored once the client has ended: nothing would ever release the stream there.

        Args:
            stream: The stream.
        """
        if self._ended:
            return
        self._register_stream(stream)
        self._after_change(opened=(stream.stream_id,))

    def _register_stream(self, stream: _Stream) -> None:
        """Record a stream and add its topics, declaring its channel's required keys first.

        `set_required` runs before the channel's first subscribe, and again when a stream of
        another dataset on the same channel widens its keys. The keys of all datasets on a
        channel are required together, so such a dataset's key also rejects the rows the
        other datasets read; every declared dataset reads its own channel today.

        Args:
            stream: The stream.
        """
        self._streams[stream.stream_id] = stream
        if self._stalled:
            self._update_streams({stream.stream_id: StreamUpdate(error=self._stall_text())})
        keys = self._required.get(stream.channel, frozenset()) | stream.plan.required
        if self._required.get(stream.channel) != keys:
            self._required[stream.channel] = keys
            self._client.set_required(stream.channel, keys, symbol=stream.plan.symbol_field)
        self.subscriptions.add(stream.topics, static=stream.static)
        if self._idle is not None:
            self._idle.cancel()
            self._idle = None

    def release_stream(self, stream_id: int) -> None:
        """Release a shut-down stream's topics and routing; forward it if the stream moved.

        Args:
            stream_id: The stream whose blink table shut down.
        """
        stream = self._streams.pop(stream_id, None)
        if stream is None:
            self._unknown(stream_id, self.release_stream, ())
            return
        self.flusher.routes.remove(stream_id)
        self._deferred_shown.pop(stream_id, None)
        self.subscriptions.remove(stream.topics, static=stream.static)
        self._after_change()

    def change_symbols(
        self, stream_id: int, added: frozenset[str], removed: frozenset[str]
    ) -> None:
        """Apply a reactive symbol table's change to its stream's topics and its row.

        Args:
            stream_id: The stream.
            added: Canonical symbols the table now holds.
            removed: Canonical symbols it no longer holds.
        """
        stream = self._streams.get(stream_id)
        if stream is None:
            self._unknown(stream_id, self.change_symbols, (added, removed))
            return
        symbols = (cast("frozenset[str]", stream.symbols) | added) - removed
        # The stream's session resolved every added crypto pair before posting the change.
        with self._lock:
            pairs = dict(self._group.pairs)
        topics = stream_topics(self.asset_class, stream.channel, symbols, pairs=pairs)
        self.subscriptions.add(topics - stream.topics)
        self.subscriptions.remove(stream.topics - topics)
        stream.symbols, stream.topics = symbols, topics
        stream.sink.set_symbols(symbols)
        count, text = describe_symbols(tuple(sorted(symbols)))
        self._update_streams({stream_id: StreamUpdate(symbol_count=count, symbols=text)})
        self._after_change()

    def _unknown(
        self, stream_id: int, callback: Callable[..., None], args: tuple[object, ...]
    ) -> None:
        """Handle a call for a stream this connection does not hold.

        A stream that moved away is forwarded to the connection that took it; one that is
        moving here but not adopted yet keeps the call until `adopt` applies it; any other
        (a stream whose open failed before it was attached) is ignored.

        Args:
            stream_id: The stream.
            callback: The call that found no stream, a loop-thread method of this connection.
            args: Its arguments after the stream id.
        """
        if self._successor is not None:
            name = getattr(callback, "__name__", "")
            self._successor.post(getattr(self._successor, name), stream_id, *args)
            return
        with self._lock:
            awaited = stream_id in self.awaiting
        if awaited:
            self._early.setdefault(stream_id, []).append((callback, args))
        else:
            self._after_change()  # a stream that never arrived may have been the last lease

    def check_idle(self) -> None:
        """Arm the idle close if the connection has no stream left.

        Also forgets the calls kept for streams it no longer awaits (a move whose hand-over
        never came).
        """
        self._drop_unawaited()
        self._after_change()

    def _drop_unawaited(self) -> None:
        """Forget the calls kept for streams this connection no longer awaits."""
        with self._lock:
            awaited = set(self.awaiting)
        for stream_id in [s for s in self._early if s not in awaited]:
            del self._early[stream_id]

    def _after_change(self, *, opened: Collection[int] = ()) -> None:
        """After a change of streams or topics: reconcile, publish the cap, maybe go idle.

        Args:
            opened: The streams just added (or adopted), whose topics now count against the
                options cap through the admitted set instead of as pending.
        """
        if self._ended:  # the client has ended: nothing to reconcile or to close
            return
        self.subscriptions.reconcile()
        self._show_deferred(opened=opened)
        if not self._streams and self._idle is None:
            self._idle = self._client.loop.call_later(IDLE_GRACE, self._idle_close)

    def _idle_close(self) -> None:
        """Close the connection if it has had no stream for the grace period."""
        self._idle = None
        if self._streams or not self._group.retire(self, idle=True):
            return
        logger.info("closing %s: no stream for %.0f s", self.url, IDLE_GRACE)
        self.close()

    def hand_over(self, successor: Connection) -> None:
        """Move every stream to `successor` and close. Loop thread.

        Called when the server's real-time verdict moves the connection to another host. Under
        the group's lock, the streams are routed on `successor` too (or, if it has already
        failed, they fail here); their topics reach its loop with `adopt`; a release or symbol
        change that still arrives here is forwarded. The streams stay routed on this
        connection as well, until its flush thread ends: its last flush applies the failures it
        has queued and publishes the rows it has buffered (a stream's publish lock keeps the two
        flush threads from interleaving on one stream), and then the thread clears its routes,
        so this connection holds none of the moved streams once it has ended.

        Only the streams that still hold a lease go there (the move pointed each lease at
        `successor`). A stream whose lease is gone (its blink table shut down, or its session
        closed, after or before the move) stays behind: it stops being routed, `successor`
        stops awaiting it, and its topics are never subscribed there, so a closed session's
        stream cannot keep the successor open.

        Args:
            successor: The connection that takes over.
        """
        streams = list(self._streams.values())
        self._streams.clear()
        self._successor = successor
        with self._lock:
            leased = self._group.streams
            moving = [s for s in streams if s.stream_id in leased]
            gone = {s.stream_id for s in streams} - {s.stream_id for s in moving}
            for stream_id in gone:
                successor.awaiting.discard(stream_id)
                successor.cap_pending.pop(stream_id, None)
            failure = successor.failure
            if failure is None:
                for stream in moving:
                    successor.flusher.routes.add(stream.channel, stream.sink)
        for stream_id in gone:
            self.flusher.routes.remove(stream_id)
        if failure is not None:
            self.flusher.fail([s.stream_id for s in moving], failure)
        else:
            successor.post(successor.adopt, moving)
        self.subscriptions.ended()
        self.close(final_flush=True)

    def adopt(self, streams: list[_Stream]) -> None:
        """Take over the streams of a connection the verdict moved, and the calls they missed.

        Args:
            streams: The streams, with their topics.
        """
        with self._lock:
            self.awaiting.difference_update(s.stream_id for s in streams)
        if self._ended:  # it failed or closed first; its failure already failed the routes
            return
        for stream in streams:
            self._register_stream(stream)
        update = StreamUpdate(host=self.decision.host, path=self.path)
        self._update_streams({s.stream_id: update for s in streams})
        for stream in streams:
            for callback, args in self._early.pop(stream.stream_id, ()):
                callback(stream.stream_id, *args)
        self._drop_unawaited()
        self._after_change(opened=[s.stream_id for s in streams])

    # --- loop thread: client callbacks ---------------------------------------------------------

    def _on_open(self, generation: int) -> None:
        """A new authenticated socket: re-send the whole desired set.

        Args:
            generation: The new generation.
        """
        if generation > 1:
            self._reconnects += 1
        self._state = ConnectionState.CONNECTED
        self._next_reconnect_at = None
        if self._stalled:
            self._stalled = False
            self._unstall()
        self.subscriptions.opened(generation)

    def _on_status(self, generation: int, status: Status) -> None:
        """Apply a status object of the current generation; an older one's is stale.

        Args:
            generation: The generation it arrived in.
            status: The decoded status.
        """
        if generation == self.subscriptions.generation:
            self.subscriptions.status(status)

    def _on_data(
        self,
        channel: str,
        rows: list[dict[str, object]],
        *,
        rejects: list[Reject],
        drift: DriftReport,
    ) -> None:
        """Hand one channel's rows to the flush thread; fail the rejects' streams; record drift.

        Args:
            channel: The channel.
            rows: The valid rows.
            rejects: The rows that failed validation.
            drift: The frame's drift for the channel.
        """
        if rows:
            self.flusher.rows(channel, rows)
            self._arm_backlog_row()  # the row write below may be throttled
        for reject in rejects:
            self._reject(channel, reject)
        if drift.extra or drift.missing:
            self._record_drift(channel, drift)
        now = time.time_ns()
        self._last_message_at = now
        if now - self._message_row_at >= _MESSAGE_ROW_NS:
            self._write_row()

    def _on_state(self, state: WsState, detail: float | WireError | None) -> None:
        """Show the client's state; on a fatal failure, fail every stream.

        Args:
            state: The client's new state.
            detail: The next attempt's time (seconds since the epoch) for `RECONNECTING` and
                `WAITING_SLOT`, the `WireError` for `FAILED`, None otherwise.
        """
        self._next_reconnect_at = None  # shown only while a reconnect waits for its time
        if state in {WsState.RECONNECTING, WsState.WAITING_SLOT}:
            self.subscriptions.ended()
            self._next_reconnect_at = int(cast("float", detail) * 1e9)
            if state is WsState.WAITING_SLOT and not self._stalled:
                self._stalled = True
                text = self._stall_text()
                self._update_streams({s: StreamUpdate(error=text) for s in self._streams})
        elif state is WsState.FAILED:
            self.subscriptions.ended()
            self._failed(cast("WireError", detail))
        elif state is WsState.CLOSED:
            self.subscriptions.ended()
        self._state = ConnectionState(state.value)
        self._write_row()
        if state in {WsState.CLOSED, WsState.FAILED}:
            # The client's last callback: the loop runs nothing that would release these, so
            # it lets them go now. A call posted later is dropped, or ignored (`_ended`).
            self._ended = True
            self._streams.clear()
            self._early.clear()
            self._deferred_shown.clear()

    def _failed(self, error: WireError) -> None:
        """Fail every stream after a fatal failure, and leave the registry.

        The failure is recorded under the group's lock together with the routed streams it
        fails, so a stream attached at that moment is either among them or sees the failure
        and fails itself (`WsConnections` does it on the opening caller's thread).

        Args:
            error: The client's error.
        """
        if isinstance(error, WireAuthError):
            public = from_wire(error)
        else:
            public = MassiveError(
                f"the WebSocket connection to {self.decision.host}{self.path} failed: "
                f"{error.message}",
                status=error.status,
            )
        self._last_error = str(public)
        logger.error("%s", public)
        with self._lock:  # one section: no verb leases the connection once it has failed
            self.failure = public
            routed = [sink.stream_id for sink in self.flusher.routes.sinks()]
            self._group.retire(self, idle=False)
        self.flusher.fail(routed, public)
        self.flusher.stop(final=True)  # the failures above are applied before it ends

    # --- loop thread: failures and drift -------------------------------------------------------

    def _reject(self, channel: str, reject: Reject) -> None:
        """Fail the streams that would have received a rejected row.

        Args:
            channel: The row's channel.
            reject: The rejected row, with its raw wire symbol.
        """
        symbol = (
            None if reject.symbol is None else canonical_from_wire(self.asset_class, reject.symbol)
        )
        if symbol is None:
            logger.error(
                "%s%s: a %s row has no valid symbol; failing the channel's ALL streams: %s",
                self.decision.host,
                self.path,
                channel,
                reject.error.message,
            )
        ids = [
            s.stream_id
            for s in self._streams.values()
            if s.channel == channel
            and (s.symbols is None or (symbol is not None and symbol in s.symbols))
        ]
        if ids:
            self.flusher.fail(ids, from_wire(reject.error))

    def _refused(self, topics: tuple[str, ...], message: str) -> None:
        """Fail the streams of refused topics with `EntitlementError`.

        Args:
            topics: The refused topics.
            message: The server's message, verbatim.
        """
        refused = set(topics)
        hint = f"MassiveConfig.ws_hosts[AssetClass.{self.asset_class.name}] chooses the host"
        if self.asset_class is AssetClass.INDICES and any(channel_of(t) == "V" for t in refused):
            hint += " (index values may need business.massive.com)"
        error = EntitlementError(
            f"{self.decision.host}{self.path} refused {', '.join(sorted(refused))}: {message}; "
            f"the plan may not include this data on this host; {hint}"
        )
        self._last_error = str(error)
        ids = [s.stream_id for s in self._streams.values() if s.topics & refused]
        self.flusher.fail(ids, error)

    def _step_refused(self, status: Status) -> bool:
        """Decide whether a refusal of a whole step is the server's real-time verdict.

        The verdict can arrive more than once for one step: a step of more than 1,000 topics
        goes out in several subscribe frames, and the server refuses each of them. The first
        verdict starts the move; each later one finds the connection already moving and is
        handled by that move too, so it never fails the streams being moved.

        Args:
            status: A refusal naming no topic, of a step none of whose topics was resolved.

        Returns:
            True when the connection is moving to the `delayed.` host (the streams keep
            running); False when the refusal fails the step's streams.
        """
        decision = self.decision
        with self._lock:
            tick = ChannelFamily.TICK in self.families
        if (
            not status.realtime_verdict
            or decision.recency is not Recency.UNKNOWN
            or not tick
            or not decision.host.startswith("socket.")
        ):
            return False
        if not self._moving:
            self._moving = self._group.reroute(self, status.message)
        return self._moving

    def _record_drift(self, channel: str, drift: DriftReport) -> None:
        """Add a frame's drift to the connection's row and hand it to the flush thread.

        Args:
            channel: The channel.
            drift: Its drift in the frame.
        """
        text = self._drift_seen.record(self.key, channel, drift)
        if text is not None:
            self._drift = text
            self._write_row()
        self.flusher.drift(channel, drift)

    def _stall_text(self) -> str:
        """Return the `Error` of a stream whose connection waits for a slot.

        Returns:
            The text, naming the host and the cluster.
        """
        return (
            f"waiting for a WebSocket slot on {self.decision.host}{self.path}: another "
            "connection with this API key, perhaps from another process, holds it; the plan "
            "allows one connection per cluster"
        )

    def _unstall(self) -> None:
        """Clear the streams' wait for a slot, showing an options-cap warning again if any."""
        updates: dict[int, StreamUpdate] = {}
        for stream_id in self._streams:
            if self._deferred_shown.get(stream_id, 0):
                updates[stream_id] = StreamUpdate(error=self._cap_text())
            else:
                updates[stream_id] = StreamUpdate(clear_error=True)
        self._update_streams(updates)

    def _cap_text(self) -> str:
        """Return the `Error` of an options stream whose contracts the cap defers.

        Returns:
            The text, with the topics wanted and deferred now.
        """
        deferred = len(self.subscriptions.deferred)
        return f"over capacity: {self.subscriptions.explicit}/{OPTIONS_CAP}; {deferred} deferred"

    # --- loop thread: diagnostics --------------------------------------------------------------

    def _changed(self) -> None:
        """Schedule one publication of the subscription state for this turn of the loop.

        `Subscriptions` reports a change after every acknowledgement, refusal, and `NO_ACK`,
        and one frame from Massive can carry a status for each of thousands of topics.
        Publishing reads every topic, so doing it per status would cost O(topics²) per step
        and stall the loop for seconds on a large subscription. Instead the first change of
        a loop turn schedules `_publish_change` with `call_soon`, and later changes in the
        same turn ride on it: the row is still rewritten after each frame's statuses.
        """
        if self._change_scheduled:
            return
        self._change_scheduled = True
        self._client.loop.call_soon(self._publish_change)

    def _publish_change(self) -> None:
        """Publish the options-cap state and rewrite the row for the changes of a loop turn."""
        self._change_scheduled = False
        self._show_deferred()
        self._write_row()

    def _show_deferred(self, *, opened: Collection[int] = ()) -> None:
        """Publish the admitted options contracts and show each stream's deferred count.

        The admitted set is published, and an opening stream's pending topics dropped, in one
        section of the group's lock, so a verb's cap check never sees the stream's topics in
        neither.

        Args:
            opened: The streams whose pending topics are now counted through the admitted set.
        """
        if self.asset_class is not AssetClass.OPTIONS:
            return
        with self._lock:
            self.cap_admitted = self.subscriptions.admitted
            for stream_id in opened:
                self.cap_pending.pop(stream_id, None)
        deferred = self.subscriptions.deferred
        updates: dict[int, StreamUpdate] = {}
        for stream in self._streams.values():
            count = len(stream.topics & deferred)
            if count == self._deferred_shown.get(stream.stream_id, 0):
                continue
            self._deferred_shown[stream.stream_id] = count
            if count:
                text = self._cap_text()
                logger.warning("%s stream %d: %s", stream.plan.dataset, stream.stream_id, text)
                update = StreamUpdate(deferred=count, error=text)
            else:
                logger.warning(
                    "%s stream %d: no contract is deferred any more",
                    stream.plan.dataset,
                    stream.stream_id,
                )
                update = StreamUpdate(deferred=0, clear_error=True)
            # While the connection waits for a slot, that wait stays the stream's `Error`.
            updates[stream.stream_id] = StreamUpdate(deferred=count) if self._stalled else update
        if updates:
            self._update_streams(updates)

    def _update_streams(self, updates: dict[int, StreamUpdate]) -> None:
        """Apply changes to streams' `m.streams()` rows, each in its own session.

        Args:
            updates: The change of each stream, by id; every id is one of the connection's
                streams.
        """
        by_core: dict[SessionCore, dict[int, StreamUpdate]] = {}
        for stream_id, update in updates.items():
            core = self._streams[stream_id].sink.core()
            if core is not None:  # a freed session shows no row
                by_core.setdefault(core, {})[stream_id] = update
        for core, changes in by_core.items():
            core.update_streams(changes)

    def _arm_backlog_row(self) -> None:
        """Schedule a rewrite of the row `_BACKLOG_ROW_DELAY` seconds from now, unless one is.

        Called when rows are buffered and when a written row shows buffered rows. The
        rewrite shows the backlog as it is then, and schedules the next one while it is not
        zero, so `BufferedRows` shows rows that arrived while row writes were throttled and
        falls back to zero once the flush thread catches up.
        """
        if self._backlog_timer is None:
            self._backlog_timer = self._client.loop.call_later(
                _BACKLOG_ROW_DELAY, self._backlog_row
            )

    def _backlog_row(self) -> None:
        """Rewrite the row that showed buffered rows, to show the backlog as it is now."""
        self._backlog_timer = None
        self._write_row()

    def _write_row(self) -> None:
        """Write the connection's diagnostics rows into each user session that shows it.

        A session shows the row of a (class, family) only while this is its current
        connection for the key; a session that has been freed meanwhile is skipped. A row
        that shows buffered rows is written again `_BACKLOG_ROW_DELAY` seconds later, and so
        on until it shows none, so `BufferedRows` falls back to zero once the flush thread
        catches up, even when no message arrives to rewrite the row.
        """
        topics, acked, pending, no_ack = self.subscriptions.counts()
        backlog = self.flusher.backlog
        if backlog:
            self._arm_backlog_row()
        now = time.time_ns()
        self._message_row_at = now
        with self._lock:
            users = [(owner, owner.core()) for owner in self.users]
            for family in sorted(self.families):
                row = ConnectionRow(
                    transport=Transport.WS,
                    asset_class=self.asset_class,
                    family=family,
                    host=self.decision.host,
                    path=self.path,
                    recency=self.decision.recency,
                    evidence=self.decision.evidence,
                    state=self._state,
                    topics=topics,
                    acked_topics=acked,
                    pending_topics=pending,
                    no_ack_topics=no_ack,
                    buffered_rows=backlog,
                    reconnects=self._reconnects,
                    last_message_at=self._last_message_at,
                    next_reconnect_at=self._next_reconnect_at,
                    updated_at=now,
                    last_error=self._last_error,
                    schema_drift=self._drift,
                )
                for owner, core in users:
                    if core is not None and owner.owns_row(self, family):
                        core.diagnostics.write(row)


class _WsClientFactory:
    """The factory of real `WsClient`s, with the session's key and backoffs.

    Holds the API key, which goes only into each client's authentication frame and is never
    logged; its `repr` does not show it.
    """

    __slots__ = ("_api_key", "_backoff", "_max_backoff")

    def __init__(self, api_key: str, *, backoff: float, max_backoff: float) -> None:
        """Keep the settings every client gets.

        Args:
            api_key: The session's key.
            backoff: `MassiveConfig.reconnect_backoff`.
            max_backoff: `MassiveConfig.max_reconnect_backoff`.
        """
        self._api_key = api_key
        self._backoff = backoff
        self._max_backoff = max_backoff

    def __call__(
        self,
        url: str,
        *,
        sources: Sequence[Source],
        on_open: Callable[[int], None],
        on_status: Callable[[int, Status], None],
        on_data: Callable[[int, str, list[dict[str, object]], list[Reject], DriftReport], None],
        on_state: Callable[[WsState, float | WireError | None], None],
    ) -> _Client:
        """Return a new, unstarted `WsClient` of `url`.

        Args:
            url: The endpoint.
            sources: The WebSocket sources of the cluster.
            on_open: The connection's `on_open`.
            on_status: The connection's `on_status`.
            on_data: The connection's `on_data`.
            on_state: The connection's `on_state`.

        Returns:
            The client.
        """
        return _massive.WsClient(
            url,
            api_key=self._api_key,
            sources=sources,
            on_open=on_open,
            on_status=on_status,
            on_data=on_data,
            on_state=on_state,
            reconnect_backoff=self._backoff,
            max_reconnect_backoff=self._max_backoff,
        )


def _weakly(owner_ref: weakref.ref[WsConnections], method: str, *args: object) -> None:
    """Call a session's `WsConnections` method through a weak reference, if it still exists.

    The blink table's shutdown callback and a watchlist's change callback are held from Java
    until the JVM collects the publisher or the listener. Holding the session's connections
    strongly there would keep a finished session alive until the next JVM collection. While a
    stream runs, its session (and so its `WsConnections`) is alive anyway. `WsConnections`
    holds its core only weakly too, so this is defense in depth: it keeps the transport, not
    only the session, from waiting for the JVM.

    Args:
        owner_ref: The session's `WsConnections`, weakly.
        method: The method's name, `"_shut_down"` or `"_symbols_changed"`.
        *args: Its arguments.
    """
    owner = owner_ref()
    if owner is not None:
        getattr(owner, method)(*args)


def _wanted(
    asset_class: AssetClass,
    channel: str,
    symbols: NormalizedSymbols,
    *,
    pairs: Mapping[str, tuple[str, str]],
) -> tuple[frozenset[str] | None, frozenset[str]]:
    """Return what a new stream wants: its symbols and its topics at the start.

    Args:
        asset_class: The stream's class.
        channel: Its channel.
        symbols: Its normalized symbols.
        pairs: The base and quote of each crypto symbol, from Massive's crypto ticker list.

    Returns:
        `(symbols, topics)`: for `dm.ALL`, None and the channel's wildcard; for a symbol
        list, the symbols and their topics; for a reactive table, two empty sets, which its
        watch fills once it starts.
    """
    if isinstance(symbols, _All):
        return None, frozenset({wildcard(channel)})
    if isinstance(symbols, SymbolTable):
        return frozenset(), frozenset()
    return frozenset(symbols), stream_topics(asset_class, channel, symbols, pairs=pairs)


class WsTransport:
    """The `StreamTransport` a verb receives: the session's core and its `WsConnections`.

    `SessionClients.streams()` returns a new one for each call, holding the core itself for the
    length of the verb that uses it, and the session's one `WsConnections`, which holds the
    core only weakly because it lives as long as the session. Thread-safe: it has no state of
    its own.

    Example:
        ::

            transport = WsTransport(core, connections)
            handle = transport.open_stream(spec, request, "T", stream_id=7)
    """

    __slots__ = ("_connections", "_core")

    def __init__(self, core: SessionCore, connections: WsConnections) -> None:
        """Bind the session's core to its connections.

        Args:
            core: The session's core.
            connections: The session's connections.
        """
        self._core = core
        self._connections = connections

    def open_stream(
        self, spec: DatasetSpec, request: Request, channel: str, *, stream_id: int
    ) -> StreamHandle:
        """Open a stream on the session's connections (`WsConnections.open_stream`).

        Args:
            spec: A WebSocket dataset.
            request: The normalized arguments.
            channel: The channel.
            stream_id: The id the session minted.

        Returns:
            The stream's handle.
        """
        return self._connections.open_stream(
            self._core, spec, request, channel=channel, stream_id=stream_id
        )


_groups_lock = threading.Lock()
_groups: dict[Hashable, ConnectionGroup] = {}
# The connection groups of the process, by sharing key (`ConnectionGroup.attach`). Its lock is
# taken before a group's lock, never under it.


def _sharing_value(name: str, value: object) -> Hashable:
    """Return a connection setting as it appears in a group's sharing key.

    The key is process-wide and public (`ConnectionGroup.key`), so the API key appears as its
    SHA-256 digest, never itself: no repr or traceback of the key can show the secret. A mapping
    (`ws_hosts`) becomes a frozenset of its items.

    Args:
        name: One of `CONNECTION_SETTINGS`.
        value: The session's value of it.

    Returns:
        A hashable value, equal for equal settings.
    """
    if name == "api_key":
        return hashlib.sha256(cast("str", value).encode()).digest()
    if isinstance(value, Mapping):
        return frozenset(value.items())
    return value


class ConnectionGroup:
    """The WebSocket connections shared by the sessions of one API key and equal settings.

    Individual Massive plans allow one WebSocket connection per cluster per account, so sessions
    of one process that use the same key share their connections instead of knocking each other
    off. Sessions join a group only when every setting that shapes a connection is equal
    (`CONNECTION_SETTINGS`), they run on the same Deephaven update graph (a connection's flush
    thread enters one execution context for all its streams), and they use the same client
    factory (tests pass their own): a connection carries one value of each, so a session never
    silently gets another session's settings. A session whose settings differ gets a group of
    its own; its construction warns about it (`_session.py`).

    Each session's `WsConnections` attaches to its group when the session opens its first
    stream (`attach`) and detaches when the session closes or is freed (`detach`); the group
    leaves the process's registry with its last session. The group owns what belongs to the
    connections rather than to a session: the connections by URL, the connection and the
    session of each stream, the record of `NO_ACK` topics, the crypto pairs its sessions
    looked up, and the server's real-time verdict (`reroute`), which moves a connection with
    every session's streams and is kept by (class, family) for every session's later streams.
    It holds no session core. Its lock, a leaf lock (`lock`), guards all of it and each
    connection's leases, users, failure, and options-cap bookkeeping; no Deephaven call,
    network call, or wait happens under it, and a session's `registry_lock` is never taken
    with it.

    Example:
        ::

            group = ConnectionGroup.attach(
                connections, config=core.config, exec_ctx=core.exec_ctx, client_factory=None
            )
            group.detach(connections)  # the last session: the group leaves the registry
    """

    def __init__(
        self,
        key: Hashable,
        *,
        factory: ClientFactory,
        ack_timeout: float,
        flush_interval: float,
        reconnect_backoff: float,
        pinned: frozenset[AssetClass],
    ) -> None:
        """Create an empty group; `attach` creates them.

        Args:
            key: Its key in the process's registry: the sharing key of `attach`.
            factory: Creates each connection's client.
            ack_timeout: `MassiveConfig.ack_timeout` of its sessions.
            flush_interval: `MassiveConfig.flush_interval` of its sessions.
            reconnect_backoff: `MassiveConfig.reconnect_backoff` of its sessions.
            pinned: The classes whose host its sessions pin with `MassiveConfig.ws_hosts`.
        """
        self.key: Final = key
        self.label: Final = f"massive group #{next(_group_numbers)}"
        self.lock: Final = threading.RLock()
        self.factory: Final = factory
        self.ack_timeout: Final = ack_timeout
        self.flush_interval: Final = flush_interval
        self.reconnect_backoff = reconnect_backoff
        self._pinned = pinned
        # Guarded by lock.
        self.connections: dict[str, Connection] = {}
        self.streams: dict[int, Connection] = {}
        self.owners: dict[int, WsConnections] = {}
        self.sessions: set[WsConnections] = set()
        self._no_acked: set[tuple[str, str]] = set()
        self._calendar: FuturesCalendar | None = None
        # The base and quote of each crypto symbol a session looked up: vendor facts, the same
        # for every session of the key, which the connections' topics are built from.
        self.pairs: dict[str, tuple[str, str]] = {}
        # The server's real-time verdicts by (class, family): the key's entitlement, which every
        # session of the group shares, so a session that never leased the moved connection
        # uses the delayed host too (`WsConnections._decide`).
        self.verdicts: dict[tuple[AssetClass, ChannelFamily], HostDecision] = {}

    @classmethod
    def attach(
        cls,
        owner: WsConnections,
        *,
        config: MassiveConfig,
        exec_ctx: ExecutionContext,
        client_factory: ClientFactory | None,
    ) -> ConnectionGroup:
        """Join a session to the group of its key and settings, creating the group if needed.

        Args:
            owner: The session's `WsConnections`.
            config: The session's configuration.
            exec_ctx: The session's execution context, whose update graph is part of the key.
            client_factory: The session's client factory; None for the default `WsClient`,
                which the key's settings describe completely.

        Returns:
            The group.
        """
        settings = tuple(
            _sharing_value(name, getattr(config, name)) for name in CONNECTION_SETTINGS
        )
        graph = str(exec_ctx.update_graph.j_update_graph.getName())
        key = (*settings, graph, client_factory)
        with _groups_lock:
            group = _groups.get(key)
            if group is None:
                factory = client_factory or _WsClientFactory(
                    config.api_key,
                    backoff=config.reconnect_backoff,
                    max_backoff=config.max_reconnect_backoff,
                )
                group = _groups[key] = cls(
                    key,
                    factory=factory,
                    ack_timeout=config.ack_timeout,
                    flush_interval=config.flush_interval,
                    reconnect_backoff=config.reconnect_backoff,
                    pinned=frozenset(config.ws_hosts),
                )
            with group.lock:
                group.sessions.add(owner)
        return group

    def detach(self, owner: WsConnections) -> None:
        """Take a closed session out of the group; the last one takes the group out of the registry.

        The last session also closes the group's futures schedule calendar, outside the locks
        (its close waits a bounded time for its worker).

        Args:
            owner: The session's `WsConnections`, after its `close`.
        """
        calendar = None
        with _groups_lock, self.lock:
            self.sessions.discard(owner)
            if not self.sessions:
                del _groups[self.key]
                calendar, self._calendar = self._calendar, None
        if calendar is not None:
            calendar.close()

    def futures_calendar(self, *, contracts_path: str, schedules_path: str) -> FuturesCalendar:
        """Return the group's futures schedule calendar, creating it on first use.

        The calendar dates the streamed futures rows of every session on the group's futures
        connections, so it belongs to the group, not to the session that first needed it: it
        lives until the group's last session leaves, and reads Massive's contracts and
        schedules through the REST client of any session still open (`_fetch_rows`); they
        are the same vendor facts for every session of the key. The calendar starts its worker
        thread when it is created, outside the lock; a calendar a racing caller installed
        first wins, and the other is closed at once. Never waits on the network.

        Args:
            contracts_path: The `futures.contracts` dataset's REST path.
            schedules_path: The `futures.schedules` dataset's REST path.

        Returns:
            The calendar.

        Raises:
            MassiveError: If every session has left the group meanwhile.
        """
        with self.lock:
            found = self._calendar
        if found is not None:
            return found
        candidate = FuturesCalendar(
            self._fetch_rows,
            contracts_path=contracts_path,
            schedules_path=schedules_path,
            label=self.label,
        )
        with self.lock:
            found = self._calendar
            if found is None and self.sessions:
                found = self._calendar = candidate
        if found is not candidate:
            candidate.close()
        if found is None:
            raise MassiveError("every session of the WebSocket connection group has closed")
        return found

    def _fetch_rows(
        self, template: str, query: Mapping[str, object], *, bucket: str, cancel: Cancel
    ) -> list[dict[str, object]]:
        """Read every row of a REST path through an open session of the group.

        The futures schedule calendar's worker calls it. A session that closes while it reads
        fails that read, which the calendar treats as a failed lookup; the next read goes
        through another session.

        Args:
            template: The REST path template; it takes no path parameter.
            query: Its query.
            bucket: The rate governor bucket the requests are charged to.
            cancel: The lookup's token: its lane, and the end of its waits once set.

        Returns:
            The rows.

        Raises:
            MassiveError: If no session of the group is open, or the read fails.
        """
        with self.lock:
            open_ = [o for o in self.sessions if not o.closed and o.core() is not None]
        if not open_:
            raise MassiveError("every session of the WebSocket connection group has closed")
        return open_[0].fetch_rows(template, query, bucket=bucket, cancel=cancel)

    def first_no_ack(self, path: str, topic: str) -> bool:
        """Record a topic's `NO_ACK`; tell whether it is the topic's first on the cluster.

        Kept here, not per connection, so a topic reconnects at most once for the group, also
        across an idle close or a verdict's move. Takes the lock briefly.

        Args:
            path: The cluster path of the connection, such as `/stocks`.
            topic: The topic.

        Returns:
            True for the first `NO_ACK` of the topic on the cluster.
        """
        with self.lock:
            first = (path, topic) not in self._no_acked
            self._no_acked.add((path, topic))
        return first

    def retire(self, connection: Connection, *, idle: bool) -> bool:
        """Take a connection out of the registry, so the next stream opens a new one.

        Args:
            connection: The connection, from its loop thread.
            idle: True for the idle close, which happens only while no stream holds a lease.

        Returns:
            True when the connection was retired; False for an idle close that a stream
            opening meanwhile has cancelled.
        """
        with self.lock:
            if idle and connection.leases:
                return False
            if self.connections.get(connection.url) is connection:
                del self.connections[connection.url]
            connection.retired = True
        return True

    def reroute(self, connection: Connection, message: str) -> bool:
        """Accept the server's real-time verdict on a connection: move it to the `delayed.` host.

        Called on the connection's loop thread. Never for a host pinned by
        `MassiveConfig.ws_hosts`, never for a connection that is closing, and once per
        connection; the decision is recorded with the REST transport of every session on the
        connection, so their later streams of the class and family resolve to the delayed host
        and a verdict cannot repeat for them. The move itself runs on a short-lived thread,
        because creating the new connection's client loads TLS settings, which the loop thread
        must not wait for (`_move`).

        Args:
            connection: The connection the verdict arrived on.
            message: The server's message.

        Returns:
            True when the connection is moving; False when the verdict fails the step's
            streams instead.
        """
        if connection.asset_class in self._pinned:
            return False
        with self.lock:
            if connection.closing or connection.retired:
                return False
            connection.retired = True  # one verdict per connection
        decision = HostDecision(
            "delayed." + connection.decision.host.removeprefix("socket."),
            Recency.DELAYED,
            f"server verdict: {message}",
        )
        logger.warning(
            "%s refused real-time data (%s); moving its streams to %s in %.0f s",
            connection.url,
            message,
            decision.host,
            self.reconnect_backoff,
        )
        mover = threading.Thread(
            target=self._move,
            args=(connection, decision),
            name=f"massive-move {connection.url}",
            daemon=True,
        )
        mover.start()
        return True

    def _move(self, connection: Connection, decision: HostDecision) -> None:
        """Record the verdict and move a connection's streams to the delayed host's connection.

        Runs on the thread `reroute` starts. Every session on the connection records the
        verdict, also one that leased the connection while the verdict was being recorded or
        the successor built (both happen outside the lock): it is recorded once the leases
        have moved. The successor is the delayed host's connection, created here unless one
        exists; under the lock, the old connection leaves the registry, every lease moves with
        its session, each session shows the successor's row, and the successor awaits the
        moved streams, so a release or watchlist change that reaches it before `adopt` waits
        there. The successor starts after `reconnect_backoff`; the old connection's loop then
        hands the streams over.

        Args:
            connection: The connection the verdict arrived on.
            decision: The delayed host's decision.
        """
        asset_class = connection.asset_class
        with self.lock:
            self.verdicts[asset_class, ChannelFamily.TICK] = decision
            users = self._open_users(connection)
        self._record_verdict(connection, users, decision)
        url = f"wss://{decision.host}{connection.path}"
        candidate = Connection(
            self,
            exec_ctx=connection.exec_ctx,
            url=url,
            asset_class=asset_class,
            family=ChannelFamily.TICK,
            decision=decision,
            sessions=connection.sessions,
        )
        successor: Connection | None = None
        moved: list[int] = []
        late: list[WsConnections] = []
        with self.lock:
            # Not if the connection is closing or failed, or left the registry meanwhile: its
            # last session closed, or it failed (its streams fail with it).
            if (
                not connection.closing
                and connection.failure is None
                and self.connections.get(connection.url) is connection
            ):
                # A stream of the class may already have opened the delayed host's connection.
                successor = self.connections.setdefault(url, candidate)
                successor.families.add(ChannelFamily.TICK)
                del self.connections[connection.url]
                for owner in connection.users:
                    successor.users.setdefault(owner, 0)
                    owner.show_row(asset_class, ChannelFamily.TICK, successor)
                for stream_id, current in list(self.streams.items()):
                    if current is connection:
                        owner = self.owners[stream_id]
                        self.streams[stream_id] = successor
                        successor.awaiting.add(stream_id)
                        if asset_class is AssetClass.OPTIONS:
                            # Counted against the successor's cap until it adopts the stream:
                            # the old connection's admitted topics include the stream's.
                            successor.cap_pending[stream_id] = connection.cap_pending.pop(
                                stream_id, connection.cap_admitted
                            )
                        successor.take(owner)
                        connection.give(owner)
                        moved.append(stream_id)
                late = [owner for owner in self._open_users(connection) if owner not in users]
        self._record_verdict(connection, late, decision)
        if successor is not candidate:
            candidate.close()
        if successor is None:  # the connection closed or failed meanwhile
            connection.close()
            return
        successor.start(delay=self.reconnect_backoff)  # does nothing if it already started
        if not connection.post(connection.hand_over, successor):
            # The old loop ended first: its streams were failed there and never arrive.
            with self.lock:
                successor.awaiting.difference_update(moved)
                for stream_id in moved:
                    successor.cap_pending.pop(stream_id, None)
            successor.post(successor.check_idle)

    @staticmethod
    def _open_users(connection: Connection) -> list[WsConnections]:
        """Return the sessions on a connection that are neither closed nor freed; lock held.

        Args:
            connection: The connection.

        Returns:
            Those sessions.
        """
        return [
            owner
            for owner in connection.users
            if not owner.closed and owner.core() is not None  # a freed one is closing
        ]

    @staticmethod
    def _record_verdict(
        connection: Connection, owners: list[WsConnections], decision: HostDecision
    ) -> None:
        """Record the verdict with each of some sessions' REST transports; no lock held.

        Args:
            connection: The connection the verdict arrived on.
            owners: The sessions.
            decision: The delayed host's decision.
        """
        key = (connection.asset_class, ChannelFamily.TICK)
        for owner in owners:
            try:
                owner.record_verdict(*key, decision)
            except Exception:  # the session's next stream of the class records it again
                logger.exception("could not record the real-time verdict for %s", connection.url)
                continue
            with owner.group.lock:
                owner.verdicts_recorded.add(key)


class WsConnections:
    """One session's side of its `ConnectionGroup`: its streams, its rows, and its close.

    Created once per session by `SessionClients.streams()`, which registers `close` with the
    session and hands it to each `WsTransport` it returns. It joins the group of the session's
    key and settings at once (`ConnectionGroup.attach`) and leaves it in `close`. It lives as
    long as the session's clients, so it holds the session core only weakly: the core holds it
    (through its `SessionClients`), and its `close` is a session closer, which must not keep the
    core alive. `open_stream` runs on the caller thread, which passes the core; the blink
    tables' shutdown callbacks and the symbol watches call in from any thread. Its state (the
    session's streams, the connection whose row it shows for each class and family, the
    closed flag, and the channel plans) is guarded by the group's lock.

    Its futures connections use their group's schedule calendar (`FuturesCalendar`,
    `ConnectionGroup.futures_calendar`), which dates streamed futures rows by their trading
    session; a futures stream's explicit symbols, and those a reactive table adds, are looked up
    as soon as the stream opens.

    Example:
        ::

            connections = WsConnections(
                core, resolve_host=rest.resolve_host, record_verdict=rest.record_verdict
            )
            handle = connections.open_stream(core, spec, request, channel="T", stream_id=7)
    """

    def __init__(
        self,
        core: SessionCore,
        *,
        resolve_host: Callable[[AssetClass, ChannelFamily], HostDecision],
        record_verdict: Callable[[AssetClass, ChannelFamily, HostDecision], None],
        fetch_rows: RowFetch,
        crypto_pair: Callable[[str], tuple[str, str]],
        client_factory: ClientFactory | None = None,
    ) -> None:
        """Join the session's connection group; no connection is opened.

        Args:
            core: The session's core, held weakly.
            resolve_host: Decides the host of a (class, family), once per session; the REST
                transport's `resolve_host`, which may make one REST request.
            record_verdict: Replaces the cached decision of a (class, family) with the one the
                server's real-time verdict implies; the REST transport's `record_verdict`.
            fetch_rows: Returns every row of a REST path for a query; the REST transport's
                `fetch_rows`, through which the group's futures schedule calendar reads while
                the session is open.
            crypto_pair: Returns the base and quote of a crypto ticker from Massive's crypto
                ticker list, which the session reads once; the REST
                transport's `crypto_pair`, which may make one REST request.
            client_factory: Creates each connection's client; by default a `WsClient` with the
                session's key and backoffs. Sessions share connections only with the same
                factory.
        """
        self._core = weakref.ref(core)
        self._label = core.label
        self._resolve = resolve_host
        self.record_verdict = record_verdict
        self.fetch_rows = fetch_rows
        self._crypto_pair = crypto_pair
        # The futures schedule calendar reads the paths of the futures reference datasets.
        by_name = core.catalog.by_name
        self._calendar_paths: Final = (
            cast("str", by_name["futures.contracts"].fetch_path),
            cast("str", by_name["futures.schedules"].fetch_path),
        )
        self.group: Final = ConnectionGroup.attach(
            self, config=core.config, exec_ctx=core.exec_ctx, client_factory=client_factory
        )
        self._lock: Final = self.group.lock
        # Guarded by the group's lock.
        self._mine: set[int] = set()
        self._row_owners: dict[tuple[AssetClass, ChannelFamily], Connection] = {}
        self._plans: dict[tuple[DatasetSpec, str], ChannelPlan] = {}
        # The group's verdicts this session has recorded with its REST transport.
        self.verdicts_recorded: set[tuple[AssetClass, ChannelFamily]] = set()
        # One worker, started on the first crypto watchlist change: a pair is looked up off
        # the update-graph thread, and no change of a stream overtakes another.
        self._lookups: Final = ThreadPoolExecutor(1, f"{core.label} crypto pairs")
        self._closed = False

    @property
    def closed(self) -> bool:
        """True once `close()` has run; read with the group's lock held."""
        return self._closed

    def core(self) -> SessionCore | None:
        """Return the session's core, or None once it has been freed.

        Returns:
            The core.
        """
        return self._core()

    # --- opening -------------------------------------------------------------------------------

    def open_stream(
        self,
        core: SessionCore,
        spec: DatasetSpec,
        request: Request,
        *,
        channel: str,
        stream_id: int,
    ) -> StreamHandle:
        """Open a stream: decide its host, lease its connection, create its tables, subscribe.

        Runs on the caller thread inside the session's execution context, with no lock held
        while it resolves the host (one REST request at the class's first stream) or creates
        tables. The stream's topics reach the connection's loop thread after its tables exist,
        and a reactive symbol table's watch starts after that, so no change is lost. If
        anything raises, even `KeyboardInterrupt`, the stream's scope (when created) is
        released and its lease and topics returned.

        Args:
            core: The session's core, which the calling verb holds.
            spec: A WebSocket dataset.
            request: The normalized arguments; its symbols are the stream's.
            channel: The channel.
            stream_id: The id the session minted.

        Returns:
            The stream's handle.

        Static options symbols that would put more than `OPTIONS_CAP` explicit topics on the
        `/options` connection (counting every session's topics on it) raise the `ValueError`
        of `_check_cap`, and a closed session the `MassiveError` of `_check_open`; host
        resolution raises the `EntitlementError` or `MassiveError` of `resolve_host`; a crypto
        ticker Massive's crypto ticker list does not split raises the `MassiveError` of
        `resolve_pairs`.
        """
        asset_class = cast("AssetClass", spec.asset_class)
        plan = self._plan(core, spec, channel)
        symbols = request.symbol_key()
        if asset_class is AssetClass.CRYPTO:
            if isinstance(symbols, SymbolTable):
                self.resolve_pairs(read_symbols(spec, symbols.table))
            elif isinstance(symbols, tuple):
                self.resolve_pairs(symbols)
        wanted, topics = _wanted(asset_class, channel, symbols, pairs=self.pairs())
        decision = self._decide(asset_class, spec.family)
        connection, created, decision = self._lease(
            core, spec, decision, stream_id=stream_id, topics=topics
        )
        try:
            with self._lock:
                verdict = self.group.verdicts.get((asset_class, spec.family))
            if verdict is decision:  # the group's verdict decided the host
                self._take_verdict(asset_class, spec.family, verdict)
            if asset_class is AssetClass.FUTURES and wanted:
                self.futures_calendar().watch(wanted)
            if created:
                connection.start()
            opened = open_stream_tables(
                plan,
                core.dataset_schema(spec),
                symbols,
                core=core,
                stream_id=stream_id,
                host=decision.host,
                path=connection.path,
                # Held from Java until the JVM collects the publisher or the listener: weakly,
                # so a finished session is freed without waiting for a JVM collection.
                on_shutdown=functools.partial(_weakly, weakref.ref(self), "_shut_down", stream_id),
                on_symbols=functools.partial(
                    _weakly, weakref.ref(self), "_symbols_changed", stream_id
                ),
            )
        except BaseException:
            self._abandon(connection, stream_id)
            raise
        try:
            static = not isinstance(symbols, SymbolTable)
            stream = _Stream(stream_id, channel, plan, opened.sink, wanted, topics, static)
            self._attach(stream)
            if opened.watch is not None:
                opened.watch.start()
        except BaseException:
            opened.handle.scope.release()
            self._abandon(connection, stream_id)
            raise
        return opened.handle

    def _decide(self, asset_class: AssetClass, family: ChannelFamily) -> HostDecision:
        """Return the host of a (class, family): the group's verdict, or the session's decision.

        A real-time verdict any session of the group received settles the host for all of
        them, without a request: the session records it with its REST transport the first
        time it leases a connection by it (`_take_verdict`), so its later decisions agree.
        Otherwise the session's own decision stands (`resolve_host`, which may make one REST
        request).

        Args:
            asset_class: The class.
            family: The channel family.

        Returns:
            The host decision.
        """
        with self._lock:
            verdict = self.group.verdicts.get((asset_class, family))
        if verdict is None:
            return self._resolve(asset_class, family)
        return verdict

    def _take_verdict(
        self, asset_class: AssetClass, family: ChannelFamily, verdict: HostDecision
    ) -> None:
        """Record a group's verdict with the session's REST transport, the first time only.

        It counts as recorded only once `record_verdict` has returned, so one that raised is
        tried again by the next stream of the class.

        Args:
            asset_class: The class.
            family: The channel family.
            verdict: The group's verdict.
        """
        key = (asset_class, family)
        with self._lock:
            done = key in self.verdicts_recorded
        if done:
            return
        self.record_verdict(asset_class, family, verdict)
        with self._lock:
            self.verdicts_recorded.add(key)

    def _plan(self, core: SessionCore, spec: DatasetSpec, channel: str) -> ChannelPlan:
        """Return the cached plan of a (dataset, channel), building it on first use.

        Args:
            core: The session's core, for the catalog.
            spec: The dataset.
            channel: The channel.

        Returns:
            The plan.
        """
        with self._lock:
            plan = self._plans.get((spec, channel))
        if plan is None:
            built = ChannelPlan.build(
                core.catalog, spec, channel, unknown_symbols=core.config.unknown_symbols
            )
            with self._lock:
                plan = self._plans.setdefault((spec, channel), built)
        return plan

    def _lease(
        self,
        core: SessionCore,
        spec: DatasetSpec,
        decision: HostDecision,
        *,
        stream_id: int,
        topics: frozenset[str],
    ) -> tuple[Connection, bool, HostDecision]:
        """Find or create the group's connection of a stream and lease it for the stream.

        A new connection is created outside the lock (its client loads TLS settings) and
        registered under it. The registry is read again under the lock, and as often as it
        takes: a connection found before may have been retired meanwhile, and one registered by
        another thread (of this session or another) meanwhile wins, the new one then closing
        unused. The group's verdicts are read again there too: a verdict that arrived after
        the host was decided replaces the decision, so the stream never leases the connection
        the verdict moved away from. The session becomes one of the connection's users and
        shows its row.

        Args:
            core: The session's core, whose execution context a new connection's flush thread
                enters.
            spec: The stream's dataset.
            decision: Its host decision.
            stream_id: The stream.
            topics: Its static topics, for the options cap.

        Returns:
            `(connection, created, decision)`: created is True when the caller must start it,
            and the decision is the one the connection serves (the group's verdict, if one
            arrived meanwhile).

        Static topics that would cross the options cap raise the `ValueError` of `_check_cap`,
        and a closed session the `MassiveError` of `_check_open`.
        """
        group = self.group
        asset_class = cast("AssetClass", spec.asset_class)
        url = f"wss://{decision.host}/{asset_class.value}"
        candidate: Connection | None = None
        stale: Connection | None = None
        try:
            while True:
                with self._lock:
                    self._check_open()
                    verdict = group.verdicts.get((asset_class, spec.family))
                    if verdict is not None and verdict is not decision:
                        decision = verdict
                        url = f"wss://{decision.host}/{asset_class.value}"
                        stale, candidate = candidate, None  # built for the old host
                    connection = group.connections.get(url)
                    self._check_cap(spec, connection, topics)
                    created = connection is None and candidate is not None
                    if created:
                        connection = group.connections[url] = cast("Connection", candidate)
                        candidate = None
                    if connection is not None:
                        connection.families.add(spec.family)
                        self._row_owners[asset_class, spec.family] = connection
                        if asset_class is AssetClass.OPTIONS:
                            connection.cap_pending[stream_id] = topics
                        connection.take(self)
                        group.streams[stream_id] = connection
                        group.owners[stream_id] = self
                        self._mine.add(stream_id)
                        break
                if stale is not None:
                    stale.close()
                    stale = None
                futures = asset_class is AssetClass.FUTURES
                candidate = Connection(
                    group,
                    exec_ctx=core.exec_ctx,
                    url=url,
                    asset_class=asset_class,
                    family=spec.family,
                    decision=decision,
                    sessions=self.futures_calendar() if futures else None,
                )
        finally:
            for unused in (candidate, stale):  # lost the race, refused, or outdated
                if unused is not None:
                    unused.close()
        return connection, created, decision

    def _check_open(self) -> None:
        """Refuse a stream once the session's side is closed; the group's lock is held.

        Raises:
            MassiveError: If `close()` has run.
        """
        if self._closed:
            raise MassiveError(f"{self._label} is closed")

    @staticmethod
    def _check_cap(
        spec: DatasetSpec, connection: Connection | None, topics: frozenset[str]
    ) -> None:
        """Refuse static options topics that would cross the cap; the group's lock is held.

        In use are the topics the connection admitted and those of streams still opening, of
        every session on the connection. The cap counts topics, one per contract and channel,
        so `T.X` and `Q.X` are two.

        Args:
            spec: The stream's dataset.
            connection: Its connection, or None if none exists yet.
            topics: Its static topics.

        Raises:
            ValueError: If the new explicit topics and those in use exceed `OPTIONS_CAP`.
        """
        if spec.asset_class is not AssetClass.OPTIONS:
            return
        in_use: set[str] = set()
        if connection is not None:
            in_use.update(connection.cap_admitted)
            for pending in connection.cap_pending.values():
                in_use.update(pending)
        new = {t for t in topics if not is_wildcard(t)} - in_use
        if len(new) + len(in_use) > OPTIONS_CAP:
            raise ValueError(
                f"{spec.name}: {len(new)} new option topics and {len(in_use)} in use exceed "
                f"the {OPTIONS_CAP} topics one /options connection carries; a topic is one "
                "contract on one channel, so each channel of a contract counts once, and the "
                "connection's topics include those of every session sharing it"
            )

    def _attach(self, stream: _Stream) -> None:
        """Route a new stream's rows and hand the stream to its connection's loop thread.

        The stream goes to the connection that holds its lease now (the verdict may have moved
        it). Routing and the check for the connection's failure happen in one section of the
        group's lock, the one the connection's `_failed` takes, so a connection that failed
        meanwhile never leaves the stream running: the stream fails here, on the caller thread.
        A stream whose lease is gone (the session closed) is left as it is.

        Args:
            stream: The stream, with its topics.
        """
        with self._lock:
            connection = self.group.streams.get(stream.stream_id)
            failure = None if connection is None else connection.failure
            if connection is not None and failure is None:
                connection.flusher.routes.add(stream.channel, stream.sink)
                connection.post(connection.add_stream, stream)
        if failure is not None:
            stream.sink.fail(failure)

    def _abandon(self, connection: Connection, stream_id: int) -> None:
        """Return the lease of a stream whose open failed, and let its connection go idle.

        Idempotent: the shutdown callback may already have returned the lease. The lease is
        returned wherever it is now: the verdict may have moved it to a successor while the
        stream opened.

        Args:
            connection: The connection the stream leased.
            stream_id: The stream.
        """
        with self._lock:
            current = self._forget(stream_id)
            touched = {connection} if current is None else {connection, current}
            for each in touched:
                each.cap_pending.pop(stream_id, None)
                each.awaiting.discard(stream_id)
        for each in touched:
            each.flusher.routes.remove(stream_id)
            each.post(each.check_idle)

    def _forget(self, stream_id: int) -> Connection | None:
        """Drop a stream's lease from the group; the group's lock is held. Idempotent.

        Args:
            stream_id: The stream; stream ids are unique in the process.

        Returns:
            The connection that held the lease, or None if it was already returned.
        """
        group = self.group
        current = group.streams.get(stream_id)
        if current is None:
            return None
        del group.streams[stream_id]
        del group.owners[stream_id]
        self._mine.discard(stream_id)
        current.give(self)
        return current

    # --- callbacks from any thread -------------------------------------------------------------

    def _shut_down(self, stream_id: int) -> None:
        """The blink table's `on_shutdown_callback`: unregister the stream, release its topics.

        Runs on whichever thread dropped the table's last reference (the reaper, an
        update-graph thread, a gRPC thread, or Deephaven's liveness-cleanup thread). It does
        two things only: the registry updates (`SessionCore.stream_shut_down`, then the
        stream's lease, under the group's lock) and one `call_soon_threadsafe` to the
        connection's loop, which releases the topics there. A closed loop (the connection ended)
        is logged at INFO. It never raises. The session is alive: the stream's record keeps it
        in the live set until `stream_shut_down` has run, and while the stream opens the opening
        caller holds it.

        Args:
            stream_id: The stream.
        """
        try:
            cast("SessionCore", self._core()).stream_shut_down(stream_id)
            with self._lock:
                connection = self._forget(stream_id)
            if connection is not None:
                # A loop that has ended holds no stream any more (`Connection._on_state`).
                connection.post(connection.release_stream, stream_id)
        except Exception:
            logger.exception("%s: the shutdown of stream %d failed", self._label, stream_id)

    def _symbols_changed(
        self, stream_id: int, added: frozenset[str], removed: frozenset[str]
    ) -> None:
        """A reactive symbol table changed: post the change to the stream's connection.

        Runs on an update-graph thread (or the caller thread for the initial replay), with the
        update-graph lock held: it takes the group's lock only to find the connection, and
        never blocks.

        Args:
            stream_id: The stream.
            added: Canonical symbols the table now holds.
            removed: Canonical symbols it no longer holds.
        """
        with self._lock:
            connection = self.group.streams.get(stream_id)
        if connection is None:
            return
        if connection.asset_class is AssetClass.FUTURES and added:
            self.futures_calendar().watch(added)
        if connection.asset_class is not AssetClass.CRYPTO:
            connection.post(connection.change_symbols, stream_id, added, removed)
            return
        try:
            self._lookups.submit(self._change_pairs, stream_id, added, removed)
        except RuntimeError:
            return  # close() shut the worker down meanwhile: the stream is gone with it

    def _change_pairs(self, stream_id: int, added: frozenset[str], removed: frozenset[str]) -> None:
        """Resolve a crypto stream's added pairs, then post the change; fail it if one fails.

        Runs on the lookup worker, which makes no Deephaven call: a failed lookup goes to the
        flush thread (`Flusher.fail`), and the change to the connection's loop thread.

        Args:
            stream_id: The stream.
            added: Canonical symbols the table now holds.
            removed: Canonical symbols it no longer holds.
        """
        try:
            self.resolve_pairs(added)
        except MassiveError as error:
            failure: MassiveError | None = error
        except Exception as error:  # a defect: logged with its trace, and it fails the stream
            logger.exception("%s: looking up crypto pairs failed", self._label)
            failure = MassiveError(f"looking up the crypto pairs {sorted(added)} failed: {error!r}")
        else:
            failure = None
        with self._lock:
            connection = self.group.streams.get(stream_id)
        if connection is None:
            return
        if failure is not None:
            connection.flusher.fail([stream_id], failure)
        else:
            connection.post(connection.change_symbols, stream_id, added, removed)

    def resolve_pairs(self, symbols: Iterable[str]) -> None:
        """Look up, once per group, the base and quote of each crypto symbol.

        A joined ticker does not say where its quote starts (`X:DOTUSD` could be `DOT-USD` or
        `DO-TUSD`), so each takes its base and quote from Massive's crypto ticker list, which
        the session reads once; a topic is never built from a guess. May block the calling thread
        for that read, with no lock held, so it runs on the caller thread before a stream
        starts, or on the lookup worker.

        Args:
            symbols: Canonical crypto symbols.

        A failed lookup, or a ticker Massive does not list, raises the `MassiveError` (or its
        `AuthenticationError` or `EntitlementError`) of `crypto_pair`, its message prefixed
        with `cannot stream '<ticker>'`.
        """
        for symbol in sorted(symbols):
            with self._lock:
                known = symbol in self.group.pairs
            if known:
                continue
            try:
                pair = self._crypto_pair(symbol)
            except MassiveError as error:
                raise type(error)(
                    f"cannot stream {symbol!r}: {error}",
                    status=error.status,
                    request_id=error.request_id,
                ) from error
            with self._lock:
                self.group.pairs[symbol] = pair

    def pairs(self) -> Mapping[str, tuple[str, str]]:
        """Return a copy of the crypto splits looked up so far, for building topics.

        Returns:
            The base and quote of each crypto symbol looked up, by symbol.
        """
        with self._lock:
            return dict(self.group.pairs)

    # --- rows (connections call these) ----------------------------------------------------------

    def futures_calendar(self) -> FuturesCalendar:
        """Return the group's futures schedule calendar, creating it on first use.

        A closed session raises the `MassiveError` of `_check_open`.

        Returns:
            The calendar every futures connection's flusher corrects its rows' dates with
            (`ConnectionGroup.futures_calendar`).
        """
        with self._lock:
            self._check_open()
        contracts, schedules = self._calendar_paths
        return self.group.futures_calendar(contracts_path=contracts, schedules_path=schedules)

    def owns_row(self, connection: Connection, family: ChannelFamily) -> bool:
        """Tell whether the session shows a connection's row for its class and a family.

        Called with the group's lock held.

        Args:
            connection: The connection.
            family: One of its families.

        Returns:
            True while it is the latest connection the session leased for the key.
        """
        return self._row_owners.get((connection.asset_class, family)) is connection

    def show_row(
        self, asset_class: AssetClass, family: ChannelFamily, connection: Connection
    ) -> None:
        """Make a connection the one whose row the session shows for a key; lock held.

        Args:
            asset_class: The class.
            family: The family.
            connection: The connection, such as the successor of a moved one.
        """
        self._row_owners[asset_class, family] = connection

    # --- closing -------------------------------------------------------------------------------

    def close(self) -> None:
        """Take the session's streams off their connections, close the unused ones; idempotent.

        Runs as a session closer: on the thread that closes the session, or on the reaper for a
        session freed unclosed (which has no stream left). Under the group's lock, every lease
        of the session is returned, and the session leaves the users of every connection
        another session still leases; a connection that no other session leases is taken out
        of the registry, and the session stays its user, so its final `CLOSED` row reaches the
        session's diagnostics (written before they are released). Then, outside
        the lock, the session's streams stop being routed at once (the session has already
        failed their tables) and their topics are released on the connections that stay; the
        unused connections are closed (which interrupts a backoff wait at once) and each is
        joined for at most `JOIN_TIMEOUT` seconds, with a WARNING for one that has not ended.
        A connection whose loop has ended keeps no stream (`Connection._on_state`).
        Last, the session leaves the group.
        """
        with self._lock:
            if self._closed:
                return
            self._closed = True
            mine, closing = self._leave()
        self._lookups.shutdown(wait=False, cancel_futures=True)
        for stream_id, connection in mine:
            connection.flusher.routes.remove(stream_id)
            if connection not in closing:  # an ended loop holds no stream (`_on_state`)
                connection.post(connection.release_stream, stream_id)
        for connection in closing:
            connection.close()
        for connection in closing:
            if not connection.join(JOIN_TIMEOUT):
                logger.warning(
                    "%s: the connection to %s has not ended %.0f s after close()",
                    self._label,
                    connection.url,
                    JOIN_TIMEOUT,
                )
        self.group.detach(self)

    def _leave(self) -> tuple[list[tuple[int, Connection]], list[Connection]]:
        """Return the session's leases and leave its connections; the group's lock is held.

        Returns:
            The session's streams with the connection each leased, and the connections that no
            other session leases, now out of the registry and to be closed. The session leaves
            the users of every other connection, and stays a user of those it closes.
        """
        group = self.group
        mine = [(s, cast("Connection", self._forget(s))) for s in sorted(self._mine)]
        for stream_id, connection in mine:
            connection.cap_pending.pop(stream_id, None)
        closing = []
        for connection in {*group.connections.values(), *(c for _, c in mine)}:
            if self not in connection.users:
                continue
            if connection.leases:  # another session leases it: it stays theirs
                del connection.users[self]
                continue
            # No session leases it: it closes. The session stays one of its users, so its last
            # row, `CLOSED`, still reaches the session's `m.connections()`.
            closing.append(connection)
            if group.connections.get(connection.url) is connection:
                del group.connections[connection.url]
            connection.retired = True
        return mine, closing
