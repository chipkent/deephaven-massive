"""One WebSocket connection over `WsClient`, and the group that shares connections between sessions.

A `Connection` is one `wss://<host>/<cluster>` URL: its `_massive.WsClient` (the socket,
authentication, frame decoding, reconnecting with backoff), its `Subscriptions` (the
subscription protocol), its flush thread (`Flusher`), and the streams it carries
(`ConnectionStream`). It owns everything that happens on one connection:

- **Refusals and failures.** A refused topic fails exactly the streams that want it; a
  refusal of a whole step fails the step, unless it is the server's real-time verdict on a
  `socket.` connection whose recency was unknown, which moves the connection's streams to the
  `delayed.` host (`ConnectionGroup.reroute`, `Connection.hand_over`). A fatal failure (a
  rejected key, a handshake error retrying cannot fix) fails every stream on it.
- **Rows, rejects, drift, and its row.** Rows go to the flush thread; a rejected row fails the
  streams that would have received it; drift is recorded on the connection's `SchemaDrift`. A
  row with no symbol fails only the channel's `dm.ALL` streams and is logged at ERROR as a
  defect of the library, once per spelling. The connection logs one WARNING per outage, and one
  when it goes silent during its market's trading hours (`NO_FRAMES_WARNING`), each with its
  recovery at INFO. The connection's `m.connections()` row is rewritten as its state changes, at
  most once a second while messages flow, and its traffic is logged as a summary every
  `SUMMARY_INTERVAL` seconds.
- **The options cap.** An `/options` connection carries at most `OPTIONS_CAP` explicit topics.
- **Lifetime.** A connection with no stream attached for `IDLE_GRACE` seconds closes itself.

`ConnectionGroup` is what sessions share: sessions in one process with the same API key and
equal connection settings (`CONNECTION_SETTINGS`, and the same Deephaven update graph) attach
to one group, so their streams ride the same connections, and a connection is leased per
stream. The group also holds the shared futures calendar and the record of `NO_ACK` topics.
Each session's own registry of streams and leases, and its `StreamTransport`, are `ws.py`'s
(`WsConnections`, `WsTransport`); a `ClientFactory` builds each connection's client.

**Threads and locks.** A connection's subscription state is confined to its client's loop
thread; other threads reach it only through `Connection.post`. The group's lock is a leaf lock,
like the sessions' `registry_lock` (the two are never held together), and guards the registries
of connections, leases, sessions, failures, and options-cap bookkeeping: no Deephaven call,
network call, or wait happens under it. The loop thread makes no Java call. Nothing here holds
a session's core strongly.

The module is one subject although it runs past 1,000 lines: a connection and its group change
the same leases under the group's one lock, and a move to another host acts on both at once.
"""

from __future__ import annotations

import functools
import hashlib
import itertools
import logging
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Final, Protocol, cast

from deephaven_massive import _massive
from deephaven_massive._datasets.trading_hours import in_trading_hours
from deephaven_massive._datasets.wire_symbols import canonical_from_wire
from deephaven_massive._enums import AssetClass, ChannelFamily, Recency, subscription_of
from deephaven_massive._errors import (
    LIBRARY_BUG,
    EntitlementError,
    ErrorCause,
    MassiveError,
    from_wire,
    wire_cause,
)
from deephaven_massive._ingest.futures_calendar import FuturesCalendar
from deephaven_massive._ingest.subscriptions import (
    Sender,
    Subscriptions,
    Timer,
    channel_of,
    stream_topics,
)
from deephaven_massive._log_limits import BoundedLog, bounded_list, log_failure
from deephaven_massive._massive import Cancel, Transport, WireAuthError, WsState
from deephaven_massive._tables.diagnostics import (
    ConnectionRow,
    ConnectionState,
    DriftRegistry,
    StreamUpdate,
    describe_symbols,
)
from deephaven_massive._tables.stream import ChannelPlan, Flusher, StreamSink
from deephaven_massive._tables.transports import HostDecision

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Callable, Collection, Hashable, Iterable, Sequence

    from deephaven.execution_context import ExecutionContext

    from deephaven_massive._config import MassiveConfig
    from deephaven_massive._ingest.ws import WsConnections
    from deephaven_massive._massive import DriftReport, Reject, Source, Status, WireError
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._tables.stream import FuturesSessionDates

__all__ = [
    "CONNECTION_SETTINGS",
    "IDLE_GRACE",
    "NO_FRAMES_WARNING",
    "OPTIONS_CAP",
    "SUMMARY_INTERVAL",
    "ClientFactory",
    "Connection",
    "ConnectionGroup",
    "ConnectionStream",
]

logger = logging.getLogger(__name__)


IDLE_GRACE: Final = 30.0
"""Seconds a connection with no stream attached stays open before it closes itself."""

OPTIONS_CAP: Final = 1000
"""The most explicit topics one `/options` connection carries. Massive allows 1,000 option
contracts per connection; whether it counts contracts or topics is not documented, so the
library counts topics (`T.X` and `Q.X` are two), the stricter reading. Wildcards count zero."""

_MESSAGE_ROW_NS: Final = 1_000_000_000
"""How often, at most, a connection's row is rewritten only to advance `LastMessageAt`."""

SUMMARY_INTERVAL: Final = 60.0
"""Seconds between a connection's DEBUG summaries of its traffic, so a stall shows in the log."""

NO_FRAMES_WARNING: Final = 300.0
"""Seconds a connection with acknowledged subscriptions may go without a frame during its asset
class's regular trading hours before it logs a WARNING that its live tables may be stale. Five
minutes, since a connection carrying only a few quiet symbols can go a minute or two without a
trade; the check runs with each `SUMMARY_INTERVAL` summary."""

_UNREADABLE_ROW_LIMIT: Final = 100
"""The most distinct rows with no valid symbol logged at ERROR per process."""

_unreadable_rows: Final = BoundedLog(logger, limit=_UNREADABLE_ROW_LIMIT, level=logging.ERROR)

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
class _Traffic:
    """A connection's traffic in the current summary window. Loop-confined.

    `Connection._summarize` logs it every `SUMMARY_INTERVAL` seconds and starts the next
    window. `last_data`, `acknowledged`, and `published` outlive the window: a channel that
    went quiet shows how long ago it last had rows, and the next summary names the topics
    added and removed since this one and the rows each stream got in between.

    Attributes:
        batches: The data batches received in the window, one per channel of a frame.
        rows: The valid rows received in the window, per channel.
        rejects: The rows rejected in the window.
        last_data: When each channel last had rows, in nanoseconds since the epoch.
        acknowledged: The acknowledged topics at the last summary.
        published: Each stream's `StreamSink.rows_published` at the last summary, by stream id.
    """

    batches: int = 0
    rows: dict[str, int] = field(default_factory=dict)
    rejects: int = 0
    last_data: dict[str, int] = field(default_factory=dict)
    acknowledged: frozenset[str] = frozenset()
    published: dict[int, int] = field(default_factory=dict)

    def received(self, channel: str, rows: int, rejects: int, *, now: int) -> None:
        """Count one batch of a channel.

        Args:
            channel: The batch's channel.
            rows: Its valid rows.
            rejects: Its rejected rows.
            now: When it arrived, in nanoseconds since the epoch.
        """
        self.batches += 1
        if rows:
            self.rows[channel] = self.rows.get(channel, 0) + rows
            self.last_data[channel] = now
        self.rejects += rejects

    def next_window(self) -> None:
        """Start the next window: clear the counts and keep `last_data`."""
        self.batches = 0
        self.rows = {}
        self.rejects = 0


@dataclass(slots=True)
class ConnectionStream:
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


@dataclass(slots=True)
class _Episodes:
    """A connection's two silence episodes, each logged once as a WARNING. Loop-confined.

    Attributes:
        lost_at: When the socket was lost (seconds since the epoch), or None while connected.
        lost_warned: Whether the outage's WARNING was logged (only when the connection had
            live tables), so its reconnect logs the recovery.
        last_frame_at: When the last frame of any kind arrived or the socket opened
            (nanoseconds since the epoch); data has the connection's own `_last_message_at`.
        quiet_at: When the no-frames WARNING was logged (nanoseconds since the epoch), or
            None outside that episode.
    """

    lost_at: float | None = None
    lost_warned: bool = False
    last_frame_at: int = 0
    quiet_at: int | None = None


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
        # The row last written for each family, for `show_closed`.
        self._rows: dict[ChannelFamily, ConnectionRow] = {}
        # Start and close bookkeeping, under its own leaf lock.
        self._start_lock = threading.Lock()
        self._started = False
        self._closing = False
        self._timer: threading.Timer | None = None
        self._start_name = f"{group.label} start {decision.host}{self.path}"
        self.sessions: Final = sessions
        self.flusher: Final = Flusher(
            exec_ctx,
            f"{group.label} flush {decision.host}{self.path}",
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
        self._streams: dict[int, ConnectionStream] = {}
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
        self._summary: Timer | None = None
        self._traffic = _Traffic()
        self._drift_seen: Final = DriftRegistry()  # the connection's own SchemaDrift text
        self._stalled = self._ended = self._moving = False
        self._episodes = _Episodes()
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
            self._timer.name = self._start_name
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

    def show_closed(self, owner: WsConnections) -> None:
        """Show a closing connection `CLOSED` to a session that does not wait for its end.

        A session that closes on a thread holding the update graph's lock (a console command)
        does not join the connection, so the client's own last row, `CLOSED`, could reach the
        session's diagnostics only after they are released, and be dropped: a held
        `m.connections()` would keep showing the connection as it last was. So, under the
        group's lock, which every row write holds, the session gets the last row written for
        each family it shows, marked `CLOSED` with no topics and no buffered rows, as a
        connection that has ended holds none (a `FAILED` row keeps its state), and then leaves
        the connection's users, so no later row of the client replaces it. Reads only the
        immutable rows the client thread stored, never its loop-confined state. Any thread.

        Args:
            owner: The closing session's `WsConnections`.
        """
        with self._lock:
            now = time.time_ns()
            core = owner.core()
            for family, row in self._rows.items():
                if core is not None and owner.owns_row(self, family):
                    final = row
                    if row.state is not ConnectionState.FAILED:
                        final = replace(
                            row,
                            state=ConnectionState.CLOSED,
                            topics=0,
                            acked_topics=0,
                            pending_topics=0,
                            no_ack_topics=0,
                            buffered_rows=0,
                            next_reconnect_at=None,
                            updated_at=now,
                        )
                    core.diagnostics.write(final)
            self.users.pop(owner, None)

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
            the case is logged at DEBUG.
        """
        try:
            self._client.loop.call_soon_threadsafe(callback, *args)
        except RuntimeError:
            logger.debug(
                "%s has closed; dropped %s", self.url, getattr(callback, "__name__", callback)
            )
            return False
        return True

    # --- loop thread: streams ------------------------------------------------------------------

    def add_stream(self, stream: ConnectionStream) -> None:
        """Take a new stream's topics: declare its channel's keys, then reconcile.

        Ignored once the client has ended: nothing would ever release the stream there.

        Args:
            stream: The stream.
        """
        if self._ended:
            return
        self._register_stream(stream)
        self._after_change(opened=(stream.stream_id,))

    def _register_stream(self, stream: ConnectionStream) -> None:
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
        logger.debug(
            "stream %d shut down: its route is removed and %d topics released on %s",
            stream_id,
            len(stream.topics),
            self.url,
        )
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
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug(
                "stream %d: its symbol table added %s and removed %s",
                stream_id,
                sorted(added),
                sorted(removed),
            )
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
        moving here but not adopted yet keeps the call until `adopt` applies it. Any other (a
        stream whose open failed before it was attached, or one that was moving here when it
        shut down) is dropped, with the calls kept for it, and the connection may go idle.

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
            # A stream that never arrived may have been the last lease, and calls kept for it
            # while it was still awaited are no longer needed.
            logger.debug(
                "%s: stream %d is neither held nor awaited here; %s is dropped",
                self.url,
                stream_id,
                getattr(callback, "__name__", callback),
            )
            self.check_idle()

    def stop_awaiting(self, stream_ids: Iterable[int]) -> None:
        """Stop awaiting streams and stop counting them against the options cap; lock held.

        Called with the group's lock held, for streams that will never be adopted here: their
        lease was returned, or the old connection's loop ended before it could hand them over.
        The caller posts `check_idle` (or a release follows) so the calls kept for them go too.

        Args:
            stream_ids: The streams.
        """
        for stream_id in stream_ids:
            self.awaiting.discard(stream_id)
            self.cap_pending.pop(stream_id, None)

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
        closed, after or before the move) stays behind: it stops being routed here, and its
        topics are never subscribed on `successor`, which stopped awaiting it when the lease
        was returned (`WsConnections._forget`), so a closed session's stream cannot keep the
        successor open. A lease returned after this choice is caught by `adopt`. The
        connection closes even if the hand-over raises, since a connection out of the registry
        has no other close.

        Args:
            successor: The connection that takes over.
        """
        try:
            streams = list(self._streams.values())
            self._streams.clear()
            self._successor = successor
            with self._lock:
                leased = self._group.streams
                moving = [s for s in streams if s.stream_id in leased]
                gone = {s.stream_id for s in streams} - {s.stream_id for s in moving}
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
        finally:  # the connection ends even if the hand-over raised: nothing else closes it
            self.close(final_flush=True)

    def adopt(self, streams: list[ConnectionStream]) -> None:
        """Take over the streams of a connection the verdict moved, and the calls they missed.

        Only the streams whose lease this connection still holds are taken. A stream whose
        lease was returned after the hand-over chose it (its blink table shut down) is left
        out and its route removed: its release may have reached this loop first and been
        dropped, so registering it would keep its topics subscribed with nothing to release
        them.

        Args:
            streams: The streams, with their topics.
        """
        with self._lock:
            self.awaiting.difference_update(s.stream_id for s in streams)
            leased = self._group.streams
            gone = [s.stream_id for s in streams if leased.get(s.stream_id) is not self]
        if gone:
            logger.debug(
                "%s: streams %s shut down during their hand-over; they are not adopted",
                self.url,
                gone,
            )
            for stream_id in gone:
                self.flusher.routes.remove(stream_id)
            streams = [s for s in streams if s.stream_id not in gone]
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
        episodes = self._episodes
        if episodes.lost_at is not None:
            if episodes.lost_warned:
                logger.info(
                    "%s: reconnected after %.0f s; its live tables receive data again",
                    self.url,
                    time.time() - episodes.lost_at,
                )
            episodes.lost_at = None
            episodes.lost_warned = False
        episodes.last_frame_at = time.time_ns()
        if self._summary is None:
            self._summary = self._client.loop.call_later(SUMMARY_INTERVAL, self._summarize)
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
        self._episodes.last_frame_at = time.time_ns()
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
        now = time.time_ns()
        self._traffic.received(channel, len(rows), len(rejects), now=now)
        if rows:
            self.flusher.rows(channel, rows)
            self._arm_backlog_row()  # the row write below may be throttled
        for reject in rejects:
            self._reject(channel, reject)
        if drift.extra or drift.missing:
            self._record_drift(channel, drift)
        self._last_message_at = now
        if now - self._message_row_at >= _MESSAGE_ROW_NS:
            self._write_row()

    def _summarize(self) -> None:
        """Log the last window's traffic at DEBUG, then start the next window.

        Runs on the loop thread every `SUMMARY_INTERVAL` seconds from the first generation on,
        whether or not DEBUG is on (the line is built only when it is): the data batches and
        rows per channel received, the rejected rows, the rows each stream published and the
        streams that published none, the acknowledged topics (their count, the first
        `LIST_LIMIT`, and those added and removed since the last summary), and how long ago
        each channel last had data, so a stream that went quiet shows in the log even though
        nothing failed. Every list is bounded (`bounded_list`), so a connection of thousands
        of topics or streams logs one short line. It also checks for a silent connection
        (`_check_frames`).
        """
        self._summary = self._client.loop.call_later(SUMMARY_INTERVAL, self._summarize)
        self._check_frames(time.time_ns())
        traffic = self._traffic
        acknowledged = frozenset(self.subscriptions.acknowledged)
        published = {sid: stream.sink.rows_published for sid, stream in self._streams.items()}
        if logger.isEnabledFor(logging.DEBUG):
            now = time.time_ns()
            topics, acked, pending, no_ack = self.subscriptions.counts()
            rows = ", ".join(f"{c}={n}" for c, n in sorted(traffic.rows.items()))
            ages = ", ".join(
                f"{c} {(now - at) / 1e9:.0f} s ago" for c, at in sorted(traffic.last_data.items())
            )
            # A stream attached during the window counts from zero: its sink is new.
            got = {sid: n - traffic.published.get(sid, 0) for sid, n in sorted(published.items())}
            logger.debug(
                "%s: last %.0f s: %d data batches, rows %s, %d rejected; rows per stream %s, "
                "streams with no rows %s; %d of %d topics acknowledged %s, added %s, removed %s, "
                "%d pending, %d with no acknowledgement; last data %s; state %s, %d reconnects",
                self.url,
                SUMMARY_INTERVAL,
                traffic.batches,
                rows or "none",
                traffic.rejects,
                bounded_list(f"{sid}={n}" for sid, n in got.items() if n),
                bounded_list(sid for sid, n in got.items() if not n),
                acked,
                topics,
                bounded_list(sorted(acknowledged)),
                bounded_list(sorted(acknowledged - traffic.acknowledged)),
                bounded_list(sorted(traffic.acknowledged - acknowledged)),
                pending,
                no_ack,
                ages or "never",
                self._state.value,
                self._reconnects,
            )
        traffic.acknowledged = acknowledged
        traffic.published = published
        traffic.next_window()

    def _check_frames(self, now: int) -> None:
        """Warn once per episode when the connection goes silent while its market trades.

        A connection that holds acknowledged subscriptions and has received no frame of any
        kind for `NO_FRAMES_WARNING` seconds, during its asset class's regular trading hours
        (`in_trading_hours`), logs one WARNING that its live tables may be stale; the first
        frame after it logs the recovery at INFO. A connection that is not connected is the
        outage WARNING's (`_on_state`), not this one's.

        Args:
            now: The current time, in nanoseconds since the epoch.
        """
        episodes = self._episodes
        last = max(episodes.last_frame_at, self._last_message_at or 0)
        if episodes.quiet_at is not None:
            if last > episodes.quiet_at:
                logger.info("%s: frames arrive again", self.url)
                episodes.quiet_at = None
            return
        silent = (now - last) / 1e9
        if (
            self._state is not ConnectionState.CONNECTED
            or not self.subscriptions.acknowledged
            or silent < NO_FRAMES_WARNING
            or not in_trading_hours(self.asset_class, datetime.fromtimestamp(now / 1e9, UTC))
        ):
            return
        episodes.quiet_at = now
        logger.warning(
            "%s: no frame for %.0f s during %s trading hours, though %d topics are "
            "acknowledged, so its live tables may be stale; a market holiday or symbols that "
            "are not trading explain it, and m.connections() shows the connection's state",
            self.url,
            silent,
            self.asset_class.value,
            len(self.subscriptions.acknowledged),
        )

    def _on_state(self, state: WsState, detail: float | WireError | None) -> None:
        """Show the client's state; on a fatal failure, fail every stream.

        The first `RECONNECTING` or `WAITING_SLOT` of an outage logs one WARNING when the
        connection carries live tables, since they get no data until it reconnects; each
        later attempt of the same outage logs nothing here, and the reconnect logs the
        recovery at INFO (`_on_open`).

        Args:
            state: The client's new state.
            detail: The next attempt's time (seconds since the epoch) for `RECONNECTING` and
                `WAITING_SLOT`, the `WireError` for `FAILED`, None otherwise.
        """
        self._next_reconnect_at = None  # shown only while a reconnect waits for its time
        if state in {WsState.RECONNECTING, WsState.WAITING_SLOT}:
            self.subscriptions.ended()
            self._next_reconnect_at = int(cast("float", detail) * 1e9)
            episodes = self._episodes
            if episodes.lost_at is None:
                episodes.lost_at = time.time()
                episodes.lost_warned = bool(self._streams)
                if episodes.lost_warned:
                    logger.warning(
                        "%s: the connection is down (%s); its %d live tables get no data until "
                        "it reconnects, next attempt in %.0f s; m.connections() shows its state",
                        self.url,
                        "the account has no free connection, max_connections"
                        if state is WsState.WAITING_SLOT
                        else "reconnecting",
                        len(self._streams),
                        cast("float", detail) - episodes.lost_at,
                    )
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
                cause=wire_cause(error),  # an internal error of the client is a defect
            )
        self._last_error = str(public)
        log_failure(logger, public, "%s")
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
        symbol = None
        why = "it has none"
        if reject.symbol is not None:
            symbol = canonical_from_wire(self.asset_class, reject.symbol)
            why = f"{reject.symbol!r} is not a spelling the library reads"
        if symbol is None:
            _unreadable_rows.log(
                f"{self.path}\0{channel}\0{reject.symbol}",
                "%s%s: a %s row has no valid symbol (%s); failing the channel's ALL streams: "
                "%s. %s",
                self.decision.host,
                self.path,
                channel,
                why,
                reject.error.message,
                LIBRARY_BUG,
            )
        ids = [
            s.stream_id
            for s in self._streams.values()
            if s.channel == channel
            and (s.symbols is None or (symbol is not None and symbol in s.symbols))
        ]
        logger.debug(
            "%s channel %s: a rejected row fails streams %s: %s; the row: %s",
            self.url,
            channel,
            ids,
            reject.error.message,
            reject.raw,
        )
        if ids:  # a row the library cannot read is its own defect, never the user's
            self.flusher.fail(ids, from_wire(reject.error, cause=ErrorCause.LIBRARY_DEFECT))

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
            f"the WebSocket server {self.decision.host}{self.path} refused the subscription to "
            f"{', '.join(sorted(refused))}; Massive said: {message}; the "
            f"{subscription_of(self.asset_class)} subscription's plan may not include this data "
            f"on this host; {hint}"
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
        self.flusher.drift(channel, drift, where=f"{self.url} channel {channel}")

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
                logger.info(
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
                self._rows[family] = row
                for owner, core in users:
                    if core is not None and owner.owns_row(self, family):
                        core.diagnostics.write(row)


class _WsClientFactory:
    """The factory of real `WsClient`s, with the session's key, backoffs, and thread prefix.

    Holds the API key, which goes only into each client's authentication frame and is never
    logged; its `repr` does not show it.
    """

    __slots__ = ("_api_key", "_backoff", "_max_backoff", "_thread_prefix")

    def __init__(
        self, api_key: str, *, backoff: float, max_backoff: float, thread_prefix: str
    ) -> None:
        """Keep the settings every client gets.

        Args:
            api_key: The session's key.
            backoff: `MassiveConfig.reconnect_backoff`.
            max_backoff: `MassiveConfig.max_reconnect_backoff`.
            thread_prefix: The connection group's label (`"massive group #1"`), which starts
                each client thread's name.
        """
        self._api_key = api_key
        self._backoff = backoff
        self._max_backoff = max_backoff
        self._thread_prefix = thread_prefix

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
            thread_prefix=self._thread_prefix,
        )


_groups_lock = threading.Lock()

_groups: dict[Hashable, ConnectionGroup] = {}


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
        label: str,
        factory: ClientFactory,
        ack_timeout: float,
        flush_interval: float,
        reconnect_backoff: float,
        pinned: frozenset[AssetClass],
    ) -> None:
        """Create an empty group; `attach` creates them.

        Args:
            key: Its key in the process's registry: the sharing key of `attach`.
            label: Its label, `"massive group #<n>"`, which starts the name of every thread its
                connections run (their clients, flush threads, delayed starts, and moves), since
                they serve several sessions.
            factory: Creates each connection's client.
            ack_timeout: `MassiveConfig.ack_timeout` of its sessions.
            flush_interval: `MassiveConfig.flush_interval` of its sessions.
            reconnect_backoff: `MassiveConfig.reconnect_backoff` of its sessions.
            pinned: The classes whose host its sessions pin with `MassiveConfig.ws_hosts`.
        """
        self.key: Final = key
        self.label: Final = label
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
                label = f"massive group #{next(_group_numbers)}"
                factory = client_factory or _WsClientFactory(
                    config.api_key,
                    backoff=config.reconnect_backoff,
                    max_backoff=config.max_reconnect_backoff,
                    thread_prefix=label,
                )
                group = _groups[key] = cls(
                    key,
                    label=label,
                    factory=factory,
                    ack_timeout=config.ack_timeout,
                    flush_interval=config.flush_interval,
                    reconnect_backoff=config.reconnect_backoff,
                    pinned=frozenset(config.ws_hosts),
                )
            with group.lock:
                group.sessions.add(owner)
        return group

    def detach(self, owner: WsConnections, *, wait: bool = True) -> None:
        """Take a closed session out of the group; the last one takes the group out of the registry.

        The last session also closes the group's futures schedule calendar, outside the locks.

        Args:
            owner: The session's `WsConnections`, after its `close`.
            wait: True to let the calendar's close wait a bounded time for its worker; False
                when the closing thread holds the update graph's lock, which must not wait.
        """
        calendar = None
        with _groups_lock, self.lock:
            self.sessions.discard(owner)
            if not self.sessions:
                del _groups[self.key]
                calendar, self._calendar = self._calendar, None
        if calendar is not None:
            calendar.close(wait=wait)

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
            name=f"{self.label} move {connection.decision.host}{connection.path}",
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
                successor.stop_awaiting(moved)
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
                logger.exception(
                    "could not record the real-time verdict for %s. %s", connection.url, LIBRARY_BUG
                )
                continue
            with owner.group.lock:
                owner.verdicts_recorded.add(key)
