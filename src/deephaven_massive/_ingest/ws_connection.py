"""One WebSocket connection over `WsClient`, and the group that shares connections between sessions.

A `Connection` is one `wss://<host>/<cluster>` URL: its `_massive.WsClient` (the socket,
authentication, frame decoding, reconnecting with backoff), its `Subscriptions` (the
subscription protocol), its flush thread (`Flusher`), and the streams it carries
(`ConnectionStream`). It owns everything that happens on one connection:

- **Refusals and failures.** A refused topic fails exactly the streams that want it, and a
  refusal of a whole step fails the step's streams, with an `EntitlementError` that quotes
  Massive and names the fix for the connection's feed (`_refused`); a connection never moves
  to another host. A fatal failure (a rejected key, a handshake error retrying cannot fix)
  fails every stream on it.
- **Rows, drift, and its row.** Rows go to the flush thread, with the client's marks of the
  rows it could not read; a field Massive's documentation lacks is handed to it too, and it
  reports both as the streams' problems; a withheld row fails no stream unless its cause is
  `RAISE`. Each stream's outage, quiet feed, options-cap deferral, and unacknowledged topics
  are spans of its problems in `m.problems()` (`_report_spans`). The connection logs one
  WARNING per outage, and one when it goes silent during its market's trading hours
  (`NO_FRAMES_WARNING`), each with its recovery at INFO. The connection's `m.connections()`
  row is rewritten as its state changes, at
  most once a second while messages flow, and its traffic is logged as a summary every
  `SUMMARY_INTERVAL` seconds.
- **Outages and silence.** A live table never shows stale data as if it were current. While
  the connection is down, every stream on it says so in its `m.streams()` `Error`, and an
  outage that lasts `MassiveConfig.max_outage` fails them all, since their rows from the
  outage's start on are missing. A silent connection's streams say so in their `Error` too,
  but keep running: quiet symbols and market holidays are silent as well.
- **A taken connection.** Massive keeps the newest connection of a key on a cluster, and closes
  the older with a `max_connections` status (`WsState.TAKEN`). Each stream follows its
  session's `MassiveConfig.connection_taken`: a `FAIL` session's streams fail at once, and the
  connection reconnects, taking the connection back, only while a stream of a `RECONNECT`
  session remains; otherwise it closes (`_taken`). The setting is no connection setting, so
  sessions that differ only in it still share connections and never take them from each other.
- **The options cap.** An `/options` connection carries at most `OPTIONS_CAP` explicit topics.
- **Lifetime.** A connection with no stream attached for `IDLE_GRACE` seconds closes itself.

`ConnectionGroup` is what sessions share: sessions in one process with the same API key and
equal connection settings (`CONNECTION_SETTINGS`, and the same Deephaven update graph) attach
to one group, so their streams ride the same connections, and a connection is leased per
stream. The group also holds the shared futures calendar.
Each session's own registry of streams and leases, and its `StreamTransport`, are `ws.py`'s
(`WsConnections`); a `ClientFactory` builds each connection's client.

**Threads and locks.** A connection's subscription state is confined to its client's loop
thread; other threads reach it only through `Connection.post`. The group's lock is a leaf lock,
like the sessions' `registry_lock` (the two are never held together), and guards the registries
of connections, leases, sessions, failures, and options-cap bookkeeping: no Deephaven call,
network call, or wait happens under it. The loop thread makes no Java call. Nothing here holds
a session's core strongly.

The module is one subject although it runs past 1,000 lines: a connection and its group change
the same leases, sessions, and options-cap bookkeeping under the group's one lock.
"""

from __future__ import annotations

import hashlib
import itertools
import logging
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Final, Protocol, TypeAlias, assert_never

from deephaven_massive import _massive
from deephaven_massive._config import DELAYED_PREFIX, FMV_HOST, REALTIME_HOST, delayed_fix
from deephaven_massive._datasets.trading_hours import in_trading_hours
from deephaven_massive._enums import AssetClass, ChannelFamily, ConnectionTaken, subscription_of
from deephaven_massive._errors import (
    NEW_SESSION,
    AuthenticationError,
    EntitlementError,
    ErrorSource,
    MassiveError,
    SessionClosedError,
    from_wire,
    required,
    required_instance,
    wire_source,
)
from deephaven_massive._ingest.futures_calendar import FuturesCalendar
from deephaven_massive._ingest.host_decision import refused_snapshot, unlabelled
from deephaven_massive._ingest.recency_labels import shows_real_time
from deephaven_massive._ingest.subscriptions import (
    Sender,
    Subscriptions,
    Timer,
    channel_of,
    stream_topics,
    wildcard,
)
from deephaven_massive._log_limits import bounded_list, log_failure
from deephaven_massive._massive import (
    Cancel,
    DriftReport,
    RowFault,
    Transport,
    WireAuthError,
    WireError,
    WsState,
)
from deephaven_massive._tables.diagnostics import (
    ConnectionRow,
    ConnectionState,
    StreamUpdate,
    describe_symbols,
    utc_text,
)
from deephaven_massive._tables.problem_events import ProblemCause, SpanTracker
from deephaven_massive._tables.stream import ChannelPlan, Flusher, StreamSink

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Callable, Collection, Hashable, Iterable, Sequence

    from deephaven.execution_context import ExecutionContext

    from deephaven_massive._config import MassiveConfig
    from deephaven_massive._ingest.futures_calendar import RowFetch
    from deephaven_massive._ingest.ws import WsConnections
    from deephaven_massive._massive import RowMarks, Source, Status
    from deephaven_massive._tables.problem_events import Problem
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._tables.stream import FuturesSessionDates
    from deephaven_massive._tables.transports import HostDecision

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
    "GroupSession",
]

logger = logging.getLogger(__name__)


IDLE_GRACE: Final = 30.0
"""Seconds a connection with no stream attached stays open before it closes itself."""

OPTIONS_CAP: Final = 1000
"""The most explicit topics one `/options` connection carries. Massive allows 1,000 option
contracts per connection; whether it counts contracts or topics is not verified, so the
library counts topics (`T.X` and `Q.X` are two), the stricter reading, until a live check
shows otherwise. Wildcards count zero."""

_NOTE_TOPICS: Final = 5
"""The most unacknowledged topics a stream's `Error` names; it counts the rest."""

_MESSAGE_ROW_NS: Final = 1_000_000_000
"""How often, at most, a connection's row is rewritten only to advance `LastMessageAt`."""

SUMMARY_INTERVAL: Final = 60.0
"""Seconds between a connection's DEBUG summaries of its traffic, so a stall shows in the log."""

NO_FRAMES_WARNING: Final = 300.0
"""Seconds a connection with acknowledged subscriptions may go without a frame during its asset
class's regular trading hours before it logs a WARNING that its live tables may be stale. Five
minutes, since a connection carrying only a few quiet symbols can go a minute or two without a
trade; the check runs with each `SUMMARY_INTERVAL` summary."""

KEEPALIVE_WINDOW: Final = 40.0
"""Seconds a socket that died silently may still look open: the `websockets` keepalive's
default `ping_interval` (20 s) plus its `ping_timeout` (20 s), which `WsClient` keeps. A
connection's pings are no frames, so after a quiet spell an outage's timer counts from the
drop less this window, not from the last frame, which may be hours old."""

CONNECTION_SETTINGS: Final = (
    "api_key",
    "websocket_hosts",
    "subscription_confirmation_timeout",
    "reconnect_backoff",
    "max_reconnect_backoff",
    "max_outage",
    "flush_interval",
    "rest_base_url",
)
"""The `MassiveConfig` fields that shape a group's WebSocket connections. A connection carries
one value of each, so only sessions whose values are all equal share connections
(`ConnectionGroup`). `rest_base_url` is one because the group's futures schedule calendar reads
through any of its sessions' REST clients, which must all read the same API."""

_FMV_CHANNEL: Final = "FMV"
"""The fair-market-value channel, every class's one channel of the FMV family."""

_group_numbers: Final = itertools.count(1)
"""Numbers the connection groups of the process, for their threads' names."""

_TAKEN_HELP: Final = (
    "https://github.com/chipkent/deephaven-massive/blob/main/docs/troubleshooting.md"
    "#live-table-messages"
)
"""The troubleshooting section the error of a taken connection points to."""

_Current: TypeAlias = "Mapping[str, tuple[str | None, str | None, str, int]]"
"""The spans of one cause that hold now for a stream, as `SpanTracker.update` takes them."""

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


class GroupSession(Protocol):
    """A session of a connection group, as the group reads through it: a `WsConnections`."""

    @property
    def closed(self) -> bool:
        """True once the session's `close` has run."""
        ...

    def core(self) -> SessionCore | None:
        """Return the session's core, or None once it has been freed.

        Returns:
            The core.
        """
        ...

    @property
    def fetch_rows(self) -> RowFetch:
        """Returns every row of a REST path for a query, through the session's REST transport."""
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
        on_data: Callable[[int, str, list[tuple[object, ...]], RowMarks, DriftReport], None],
        on_state: Callable[[WsState, float | WireError | None], None],
        reconnect_deadline: Callable[[], float | None],
    ) -> _Client:
        """Return a new, unstarted client of `url` with the given callbacks.

        Args:
            url: The endpoint, `wss://<host>/<cluster>`.
            sources: The WebSocket sources of the cluster.
            on_open: The connection's `on_open`.
            on_status: The connection's `on_status`.
            on_data: The connection's `on_data`.
            on_state: The connection's `on_state`.
            reconnect_deadline: The connection's `_reconnect_deadline`, which shortens a
                reconnect wait that would let a held-down stream fail untried.
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
        dropped: The rows the client dropped in the window because a key could not be read.
        last_data: When each channel last had rows, in nanoseconds since the epoch.
        acknowledged: The acknowledged topics at the last summary.
        published: Each stream's `StreamSink.rows_published` at the last summary, by stream id.
    """

    batches: int = 0
    rows: dict[str, int] = field(default_factory=dict)
    dropped: int = 0
    last_data: dict[str, int] = field(default_factory=dict)
    acknowledged: frozenset[str] = frozenset()
    published: dict[int, int] = field(default_factory=dict)

    def received(self, channel: str, rows: int, dropped: int, *, now: int) -> None:
        """Count one batch of a channel.

        Args:
            channel: The batch's channel.
            rows: Its valid rows.
            dropped: Its rows dropped because a key could not be read.
            now: When it arrived, in nanoseconds since the epoch.
        """
        self.batches += 1
        if rows:
            self.rows[channel] = self.rows.get(channel, 0) + rows
            self.last_data[channel] = now
        self.dropped += dropped

    def next_window(self) -> None:
        """Start the next window: clear the counts and keep `last_data`."""
        self.batches = 0
        self.rows = {}
        self.dropped = 0


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
        refusal_note: What the error of a refused subscription adds for this stream: for a
            real-time-only dataset, its documented feed and its page
            (`host_decision.real_time_note`); `""` for any other.
    """

    stream_id: int
    channel: str
    plan: ChannelPlan
    sink: StreamSink
    symbols: frozenset[str] | None
    topics: frozenset[str]
    static: bool
    refusal_note: str = ""


@dataclass(frozen=True, slots=True)
class _TopicStates:
    """The topic sets an outage's recovery check reads, taken once per check.

    Attributes:
        generation: The current socket's generation.
        acknowledged: The topics Massive acknowledged on it.
        answered: Those and the topics in `NO_ACK`.
        deferred: The options topics the cap keeps off the wire.
    """

    generation: int
    acknowledged: frozenset[str]
    answered: frozenset[str]
    deferred: frozenset[str]


@dataclass(slots=True)
class _Episodes:
    """A connection's two silence episodes, each logged once as a WARNING. Loop-confined.

    Each also shows in its streams' `Error` while it lasts, and an outage that lasts
    `MassiveConfig.max_outage` for a stream fails it (`down`).

    Attributes:
        slot_warned: Whether the outage has logged its WARNING for waiting on a free
            connection slot, which its first `WAITING_SLOT` logs even when the outage began as
            a reconnect.
        down_since: When the outage began: the last frame before it (seconds since the
            epoch), or None outside one. It lasts until every stream has recovered on a
            reconnected socket, so a reconnect that drops again before that keeps it, and the
            outage warns once.
        down: The streams the outage holds down, each with the loop timer that fails it at
            `max_outage`, by id.
        gap_since: When each down stream's rows may have started to go missing (seconds since
            the epoch): the outage's start, or the stream's open when it opened during the
            outage: its `outage` span's start.
        data_since_open: Whether a data frame arrived since the current socket opened.
        down_note: The detail of a down stream's `outage` span during the outage under way,
            or None.
        last_frame_at: When the last frame of any kind arrived or the socket opened
            (nanoseconds since the epoch); data has the connection's own `_last_message_at`.
        quiet_at: When the no-frames WARNING was logged (nanoseconds since the epoch), or
            None outside that episode.
        quiet_note: The detail of every stream's `quiet_feed` span during that episode, or
            None.
        failure: The error the outage fails its streams with, made at its first failure, so
            the outage logs it once however many streams it fails; None until then.
        expired: The streams an outage has failed, which no later outage holds down again.
    """

    slot_warned: bool = False
    down_since: float | None = None
    down: dict[int, asyncio.TimerHandle] = field(default_factory=dict)
    gap_since: dict[int, float] = field(default_factory=dict)
    data_since_open: bool = False
    down_note: str | None = None
    last_frame_at: int = 0
    quiet_at: int | None = None
    quiet_note: str | None = None
    failure: MassiveError | None = None
    expired: set[int] = field(default_factory=set)


class Connection:
    """A connection to one `wss://<host>/<cluster>`, its flush thread, and its streams.

    Created by a `WsConnections` for its `ConnectionGroup` (outside every lock: creating the
    client loads TLS settings) and registered under the group's lock; started by its creator.
    The sessions of the group share it: `users` counts each session's leases, and each stream
    carries its own session. Its loop-thread state (the `Subscriptions`, the streams, the
    diagnostics row) is touched only by the client's loop thread, which runs the callbacks below
    and the work other threads `post`. `families`, `leases`, `users`, `failure`,
    `pending_failures`, and the options-cap fields are guarded by the group's lock. `close()` and
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
            group: The group that owns it (its settings and registry).
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
        self.exec_ctx: Final = exec_ctx
        self._group = group
        self._lock: Final = group.lock
        # Guarded by the group's lock.
        self.families: set[ChannelFamily] = {family}
        self.leases = 0
        self.users: dict[WsConnections, int] = {}
        self.failure: MassiveError | None = None
        self.cap_admitted: frozenset[str] = frozenset()
        self.cap_pending: dict[int, frozenset[str]] = {}
        # The failure of each stream of a `FAIL` session that was still opening when the
        # connection was taken, which it fails with when it attaches.
        self.pending_failures: dict[int, MassiveError] = {}
        # The row last written for each family, for `show_closed`.
        self._rows: dict[ChannelFamily, ConnectionRow] = {}
        # Start and close bookkeeping, under its own leaf lock.
        self._start_lock = threading.Lock()
        self._started = False
        self._closing = False
        self.sessions: Final = sessions
        self.flusher: Final = Flusher(
            exec_ctx,
            f"{group.label} flush {decision.host}{self.path}",
            interval=group.flush_interval,
            sessions=sessions,
            # Rows of a reconnected socket may recover a stream the outage holds down.
            on_new_generation=lambda: self.post(self._check_recovered),
        )
        cluster = f"{asset_class.value}/"
        sources = [s for k, s in _massive.sources(Transport.WS).items() if k.startswith(cluster)]
        self._client = group.factory(
            url,
            sources=sources,
            on_open=self._on_open,
            on_status=self._on_status,
            on_data=lambda generation, channel, rows, marks, drift: self._on_data(
                channel, rows, generation=generation, marks=marks, drift=drift
            ),
            on_state=self._on_state,
            reconnect_deadline=self._reconnect_deadline,
        )
        self.subscriptions: Final = Subscriptions(
            self._client,
            call_later=self._client.loop.call_later,
            subscription_confirmation_timeout=group.subscription_confirmation_timeout,
            cap=OPTIONS_CAP if asset_class is AssetClass.OPTIONS else None,
            on_refused=self._refused,
            on_change=self._changed,
        )
        # Loop-confined from here on.
        self._streams: dict[int, ConnectionStream] = {}
        self._required: dict[str, frozenset[str]] = {}
        self._idle: Timer | None = None
        # Each stream's deferred options topics and topics Massive has not acknowledged, as its
        # spans last showed them; a stream with neither has no entry.
        self._notes_shown: dict[int, tuple[int, frozenset[str]]] = {}
        self._state = ConnectionState.CONNECTING
        self._reconnects = 0
        self._last_message_at: int | None = None
        self._message_row_at = 0
        self._next_reconnect_at: int | None = None
        self._last_error: str | None = None
        self._summary: Timer | None = None
        self._traffic = _Traffic()
        # Each stream's span problems the connection reports (an outage, a quiet feed, the
        # options cap, unacknowledged topics), by cause; loop-confined.
        self._spans: dict[int, dict[ProblemCause, SpanTracker]] = {}
        self._ended = False
        self._episodes = _Episodes()
        # When another connection with the key took this one, until the next socket opens.
        self._taken_at: float | None = None
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

    # --- lifecycle (any thread) ----------------------------------------------------------------

    def start(self) -> None:
        """Start the flush thread and the client. Does nothing once closed or started."""
        with self._start_lock:
            if self._closing or self._started:
                return
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
            self._client.close()
            if not self._started:
                self._started = True
                self._client.start()
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

    def post_later(self, delay: float, callback: Callable[..., None], *args: object) -> bool:
        """Run `callback(*args)` on the loop thread after `delay` seconds. Any thread; never blocks.

        The loop keeps the timer, so it ends with the connection: a callback still waiting then
        never runs.

        Args:
            delay: Seconds to wait.
            callback: A callable that never blocks and makes no Java call.
            *args: Plain data.

        Returns:
            False when the loop has closed, so the callback never runs (`post`).
        """
        return self.post(self._start_timer, delay, callback, *args)

    def _start_timer(self, delay: float, callback: Callable[..., None], *args: object) -> None:
        """Start a timer of `post_later`; runs on the loop thread.

        Args:
            delay: Seconds to wait.
            callback: What the timer runs.
            *args: Its arguments.
        """
        self._client.loop.call_later(delay, callback, *args)

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
        channel are required together, so such a dataset's key also drops the rows the
        other datasets read; every declared dataset reads its own channel today.

        Args:
            stream: The stream.
        """
        self._streams[stream.stream_id] = stream
        if self._episodes.down_since is not None:  # it opens during an outage: held down
            self._hold_down(stream.stream_id, self._group.max_outage, since=time.time())
        if self._episodes.down_since is not None or self._episodes.quiet_note is not None:
            self._show_outage_notes([stream.stream_id])
        keys = self._required.get(stream.channel, frozenset()) | stream.plan.required
        if self._required.get(stream.channel) != keys:
            self._required[stream.channel] = keys
            self._client.set_required(stream.channel, keys, symbol=stream.plan.symbol_field)
        self.subscriptions.add(stream.topics, static=stream.static)
        if self._idle is not None:
            self._idle.cancel()
            self._idle = None

    def release_stream(self, stream_id: int) -> None:
        """Release a shut-down stream's topics and routing.

        Args:
            stream_id: The stream whose blink table shut down.
        """
        stream = self._streams.pop(stream_id, None)
        if stream is None:
            self._unknown(stream_id, self.release_stream, ())
            return
        self.flusher.routes.remove(stream_id)
        self._notes_shown.pop(stream_id, None)
        self._end_spans(stream, self._spans.pop(stream_id, {}))
        self._release_down(stream_id)
        self._episodes.expired.discard(stream_id)
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
        """Apply a change of a stream's symbols to its topics and its row.

        A reactive symbol table's changes come here, and so do a crypto stream's pairs once
        they are split (`WsConnections._change_pairs`).

        Args:
            stream_id: The stream.
            added: Canonical symbols the table now holds.
            removed: Canonical symbols it no longer holds.
        """
        stream = self._streams.get(stream_id)
        if stream is None:
            self._unknown(stream_id, self.change_symbols, (added, removed))
            return
        symbols = (required(stream.symbols, f"stream {stream_id}'s symbols") | added) - removed
        # The stream's session resolved every added crypto pair before posting the change.
        with self._lock:
            pairs = dict(self._group.pairs)
        topics = stream_topics(self.asset_class, stream.channel, symbols, pairs=pairs)
        # Counted as the stream's other topics are, so its release removes what was added.
        self.subscriptions.add(topics - stream.topics, static=stream.static)
        self.subscriptions.remove(stream.topics - topics, static=stream.static)
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
        self, stream_id: int, callback: Callable[..., None], _args: tuple[object, ...]
    ) -> None:
        """Drop a call for a stream this connection does not hold, and maybe go idle.

        Such a stream's open failed before it was attached, so it may have been the last
        lease.

        Args:
            stream_id: The stream.
            callback: The call that found no stream, for the log.
            _args: Its arguments after the stream id, unused.
        """
        logger.debug(
            "%s: stream %d is not held here; %s is dropped",
            self.url,
            stream_id,
            getattr(callback, "__name__", callback),
        )
        self.check_idle()

    def check_idle(self) -> None:
        """Arm the idle close if the connection has no stream left."""
        self._after_change()

    def _after_change(self, *, opened: Collection[int] = ()) -> None:
        """After a change of streams or topics: reconcile, publish the cap, maybe go idle.

        Args:
            opened: The streams just added, whose topics now count against the
                options cap through the admitted set instead of as pending.
        """
        if self._ended:  # the client has ended: nothing to reconcile or to close
            return
        if self._taken_at is not None and self._settle_taken(None):
            return  # the last stream of a RECONNECT session left during the reconnect wait
        self.subscriptions.reconcile()
        self._show_notes(opened=opened)
        if not self._streams and self._idle is None:
            self._idle = self._client.loop.call_later(IDLE_GRACE, self._idle_close)

    def _idle_close(self) -> None:
        """Close the connection if it has had no stream for the grace period."""
        self._idle = None
        if self._streams or not self._group.retire(self, idle=True):
            return
        logger.info("closing %s: no stream for %.0f s", self.url, IDLE_GRACE)
        self.close()

    # --- loop thread: client callbacks ---------------------------------------------------------

    def _on_open(self, generation: int) -> None:
        """A new authenticated socket: re-send the whole desired set.

        Args:
            generation: The new generation.
        """
        if generation > 1:
            self._reconnects += 1
        episodes = self._episodes
        if episodes.down_since is not None and self._streams:  # the recovery of its WARNING
            logger.info(
                "%s: reconnected after %.0f s; re-subscribing",
                self.url,
                time.time() - episodes.down_since,
            )
        episodes.last_frame_at = time.time_ns()
        if self._summary is None:
            self._summary = self._client.loop.call_later(SUMMARY_INTERVAL, self._summarize)
        self._state = ConnectionState.CONNECTED
        self._next_reconnect_at = None
        self._taken_at = None
        episodes.data_since_open = False
        self.subscriptions.opened(generation)
        self._check_recovered()  # with nothing to confirm, the outage ends now

    def _on_status(self, generation: int, status: Status) -> None:
        """Apply a status object of the current generation; an older one's is stale.

        Args:
            generation: The generation it arrived in.
            status: The decoded status.
        """
        self._episodes.last_frame_at = time.time_ns()
        self._heard()
        if generation == self.subscriptions.generation:
            self.subscriptions.status(status)

    def _on_data(
        self,
        channel: str,
        rows: list[tuple[object, ...]],
        *,
        generation: int,
        marks: RowMarks,
        drift: DriftReport,
    ) -> None:
        """Hand one channel's rows to the flush thread, and record the frame's drift.

        Every row goes to the flush thread with the client's marks, which withholds each row
        the library cannot read whole (`ChannelPlan.convert`). A row whose key could not be
        read counts in the window's traffic as dropped, not as data heard.

        Args:
            channel: The channel.
            rows: The decoded rows, each a tuple of the source's fields.
            generation: The generation of the socket the frame arrived on, which the streams
                its rows reach record, so only rows of a reconnected socket recover a stream
                (`_recovered`).
            marks: The rows the client could not read whole, by index into `rows`.
            drift: The frame's drift for the channel.
        """
        now = time.time_ns()
        self._heard()
        if self._episodes.down_since is not None:  # data flows again: the outage may be over
            self._episodes.data_since_open = True
            self._check_recovered()
        unread = marks.get(RowFault.UNREADABLE_KEYS)
        dropped = 0 if unread is None else len(unread.rows)
        self._traffic.received(channel, len(rows) - dropped, dropped, now=now)
        # A channel's data in a frame holds at least one row.
        self.flusher.rows(channel, rows, marks, generation=generation)
        self._arm_backlog_row()  # the row write below may be throttled
        if drift.extra:  # a field Massive's documentation lacks; the rest is the rows' marks
            self.flusher.drift(channel, drift, where=f"{self.url} channel {channel}")
        self._last_message_at = now
        if now - self._message_row_at >= _MESSAGE_ROW_NS:
            self._write_row()

    def _summarize(self) -> None:
        """Log the last window's traffic at DEBUG, then start the next window.

        Runs on the loop thread every `SUMMARY_INTERVAL` seconds from the first generation on,
        whether or not DEBUG is on (the line is built only when it is): the data batches and
        rows per channel received, the rows dropped for a key, the rows each stream published
        and the streams that published none, the acknowledged topics (their count, the first
        `LIST_LIMIT`, and those added and removed since the last summary), and how long ago
        each channel last had data, so a stream that went quiet shows in the log even though
        nothing failed. Every list is bounded (`bounded_list`), so a connection of thousands
        of topics or streams logs one short line. It also checks for a silent connection
        (`_check_frames`).
        """
        self._summary = self._client.loop.call_later(SUMMARY_INTERVAL, self._summarize)
        self._check_frames(time.time_ns())
        self._check_recovered()  # rows flushed since the last data frame may recover a stream
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
            # A stream opened during the window counts from zero, as its sink is new.
            got = {sid: n - traffic.published.get(sid, 0) for sid, n in sorted(published.items())}
            logger.debug(
                "%s: last %.0f s: %d data batches, rows %s, %d dropped; rows per stream %s, "
                "streams with no rows %s; %d of %d topics acknowledged %s, added %s, removed %s, "
                "%d pending, %d with no acknowledgement; last data %s; state %s, %d reconnects",
                self.url,
                SUMMARY_INTERVAL,
                traffic.batches,
                rows or "none",
                traffic.dropped,
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
        (`in_trading_hours`), logs one WARNING that its live tables may be stale and shows it
        in every stream's `Error` (`_set_quiet_note`); the streams keep running, since quiet symbols
        and market holidays are silent too. The first frame after it clears the note and logs
        the recovery at INFO (`_heard`). A connection that is not connected is the outage's
        (`_on_state`), not this one's.

        Args:
            now: The current time, in nanoseconds since the epoch.
        """
        episodes = self._episodes
        if episodes.quiet_at is not None:
            return
        last = max(episodes.last_frame_at, self._last_message_at or 0)
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
        self._set_quiet_note(
            f"no data since {utc_text(last / 1e9)} during {self.asset_class.value} trading "
            "hours; quiet symbols or a market holiday may explain it"
        )

    def _heard(self) -> None:
        """End a silence episode on a frame's arrival: log the recovery and clear the note."""
        if self._episodes.quiet_at is None:
            return
        self._episodes.quiet_at = None
        logger.info("%s: frames arrive again", self.url)
        self._set_quiet_note(None)

    def _set_quiet_note(self, note: str | None) -> None:
        """Show the silence's note on every stream, or clear it.

        Args:
            note: The text, or None to clear it.
        """
        self._episodes.quiet_note = note
        self._show_outage_notes(list(self._streams))

    def _warn_down(self, *, waiting_slot: bool, at: float) -> None:
        """Warn once of a lost socket's outage, and once of a wait for a slot.

        The first `RECONNECTING` or `WAITING_SLOT` of an outage logs one WARNING when the
        connection carries live tables, since they get no data until it reconnects. The first
        `WAITING_SLOT` of the outage also warns when the outage began as a reconnect, since
        the account then has no free connection, which only the user can change. Each later
        attempt logs nothing here.

        Args:
            waiting_slot: True for `WAITING_SLOT`, False for `RECONNECTING`.
            at: The next attempt's time, seconds since the epoch.
        """
        episodes = self._episodes
        warn = episodes.down_since is None and bool(self._streams)  # the outage's first attempt
        if waiting_slot and not episodes.slot_warned and self._streams:
            episodes.slot_warned = True
            warn = True
        if not warn:
            return
        logger.warning(
            "%s: the connection is down (%s); its %d live tables get no data until it "
            "reconnects, next attempt in %.0f s; m.connections() shows its state",
            self.url,
            "the account has no free connection, max_connections"
            if waiting_slot
            else "reconnecting",
            len(self._streams),
            at - time.time(),
        )

    def _on_state(self, state: WsState, detail: float | WireError | None) -> None:
        """Show the client's state; on a fatal failure, fail every stream.

        A `RECONNECTING` or `WAITING_SLOT` warns of the outage once (`_warn_down`), and the
        reconnect logs it at INFO (`_on_open`), the outage's end once every stream receives
        data again (`_check_recovered`). Each also starts or continues the outage (`_outage`).
        A `TAKEN` applies each stream's `connection_taken` (`_taken`).

        Args:
            state: The client's new state.
            detail: The next attempt's time (seconds since the epoch) for `RECONNECTING`,
                `WAITING_SLOT`, and `TAKEN`, the `WireError` for `FAILED`, None otherwise.
        """
        self._next_reconnect_at = None  # shown only while a reconnect waits for its time
        if state is WsState.TAKEN:
            self.subscriptions.ended()
            self._taken(required_instance(detail, float, "a TAKEN state's next attempt"))
            return
        if state is WsState.RECONNECTING or state is WsState.WAITING_SLOT:
            self.subscriptions.ended()
            at = required_instance(detail, float, f"a {state.value} state's next attempt")
            self._next_reconnect_at = int(at * 1e9)
            self._warn_down(waiting_slot=state is WsState.WAITING_SLOT, at=at)
            self._outage(detail=self._slot_text() if state is WsState.WAITING_SLOT else None)
        elif state is WsState.FAILED:
            self.subscriptions.ended()
            self._failed(required_instance(detail, WireError, "a FAILED state's error"))
        elif state is WsState.CLOSED:
            self.subscriptions.ended()
        elif state is WsState.CONNECTING:
            pass  # only the row shows it: the socket is not yet open
        else:
            assert_never(state)  # a new state needs its rule here
        self._state = ConnectionState(state.value)
        self._write_row()
        if state in {WsState.CLOSED, WsState.FAILED}:
            # The client's last callback: the loop runs nothing that would release these, so
            # it lets them go now. A call posted later is dropped, or ignored (`_ended`).
            self._ended = True
            self._cancel_outage_timers()
            self._streams.clear()
            self._notes_shown.clear()
            self._spans.clear()  # each stream failed or closed, which ends its spans

    def _taken(self, next_at: float) -> None:
        """Apply `connection_taken` after another connection with the key took this one.

        The connection remembers when it was taken until its next socket opens, so a change
        of its streams during the reconnect wait applies the setting again (`_settle_taken`).

        Args:
            next_at: When the client's next attempt is due, seconds since the epoch.
        """
        self._taken_at = time.time()
        self._settle_taken(next_at)

    def _settle_taken(self, next_at: float | None) -> bool:
        """Fail the streams of `FAIL` sessions, and close unless a `RECONNECT` stream remains.

        Under the group's lock, which every lease and route holds, the streams leased on the
        connection are split by their session's setting. While one of a `RECONNECT` session
        remains, the streams of `FAIL` sessions fail: a routed one through the flush thread,
        and one still opening when it attaches (`pending_failures`, which
        `WsConnections._attach` reads). The rest ride an outage as for a reconnect: the row
        shows `RECONNECTING`, and their `Error` says when the connection was taken and when the
        client takes it back. Otherwise every routed stream fails, the connection leaves the
        registry with the failure recorded, so a stream attaching at that moment fails too, and
        it closes: the client reports `CLOSED` instead of reconnecting, which would take the
        connection back from the other program.

        Args:
            next_at: When the client's next attempt is due, seconds since the epoch, when the
                client has just reported `TAKEN`; None when the connection's streams changed
                during the wait, which changes nothing while a `RECONNECT` stream remains: a
                stream that opens during the wait was never taken, so it rides the reconnect.

        Returns:
            True when the connection closed.
        """
        at = required(self._taken_at, "the time the connection was taken")
        group = self._group
        error = MassiveError(
            f"another connection with this API key took the WebSocket connection to "
            f"{self.decision.host}{self.path} at {utc_text(at)}, so this table stopped; stop "
            "the other program or session that uses the key and call the method again, or set "
            "MassiveConfig(connection_taken=dm.ConnectionTaken.RECONNECT) to take the "
            f"connection back; see {_TAKEN_HELP}",
            source=ErrorSource.MASSIVE,
        )
        with self._lock:
            leased = [sid for sid, connection in group.streams.items() if connection is self]
            keepers = {
                sid
                for sid in leased
                if group.owners[sid].connection_taken is ConnectionTaken.RECONNECT
            }
            routed = {sink.stream_id for sink in self.flusher.routes.sinks()}
            if keepers:
                failing = [sid for sid in leased if sid not in keepers]
                if next_at is not None:  # only a stream leased before the eviction was taken
                    opening = {sid: error for sid in failing if sid not in routed}
                    self.pending_failures.update(opening)
                failing = [sid for sid in failing if sid in routed]
            else:  # one section: no verb leases the connection once it has failed
                failing = sorted(routed)
                self.failure = error
                group.retire(self, idle=False)
        if keepers:
            if next_at is not None:
                self._ride_taken(at, next_at, keepers=len(keepers), failing=failing, error=error)
            return False
        logger.warning(
            "%s: another connection with this API key took the connection at %s "
            "(max_connections); no live table of a session with connection_taken=RECONNECT "
            "remains, so its %d live tables fail and the connection closes",
            self.url,
            utc_text(at),
            len(leased),  # the opening ones too, which fail as they attach
        )
        self._taken_at = None  # the close is decided: a later change must not decide it again
        self._last_error = str(error)
        self.flusher.fail(failing, error)
        self.close(final_flush=True)  # the client reports CLOSED; the failures go first
        return True

    def _ride_taken(
        self, at: float, next_at: float, *, keepers: int, failing: list[int], error: MassiveError
    ) -> None:
        """Fail the routed streams of `FAIL` sessions and hold the rest down until the reconnect.

        Args:
            at: When the connection was taken, seconds since the epoch.
            next_at: When the client's next attempt is due, seconds since the epoch.
            keepers: How many streams of `RECONNECT` sessions remain.
            failing: The routed streams of `FAIL` sessions.
            error: The error they fail with.
        """
        logger.warning(
            "%s: another connection with this API key took the connection at %s "
            "(max_connections); %d live tables of sessions with connection_taken=RECONNECT "
            "remain, so it reconnects at %s and takes the connection back; %d live tables of "
            "sessions with connection_taken=FAIL fail",
            self.url,
            utc_text(at),
            keepers,
            utc_text(next_at),
            len(failing),
        )
        self._last_error = (
            f"another connection with this API key took the connection at {utc_text(at)}"
        )
        self._episodes.expired.update(failing)  # no outage holds a failing stream down
        if failing:
            self.flusher.fail(failing, error)
        self._next_reconnect_at = int(next_at * 1e9)
        self._outage(
            detail=f"another connection with this API key took it at {utc_text(at)}; "
            f"reconnecting at {utc_text(next_at)} takes it back (connection_taken=RECONNECT)"
        )
        self._state = ConnectionState.RECONNECTING
        self._write_row()

    def _failed(self, error: WireError) -> None:
        """Fail every stream after a fatal failure, and leave the registry.

        The failure is recorded under the group's lock together with the routed streams it
        fails, so a stream attached at that moment is either among them or sees the failure
        and fails itself (`WsConnections` does it on the opening caller's thread).

        Args:
            error: The client's error.
        """
        public: MassiveError
        if isinstance(error, WireAuthError):
            # Massive answers `auth_failed` for a bad key, and may answer it for a key whose
            # plan has no streams of the class too; its text, which the message keeps, tells.
            wire = from_wire(error)
            public = AuthenticationError(
                f"{wire}; this can also mean the {subscription_of(self.asset_class)} plan "
                f"includes no WebSocket streams for {self.asset_class.value}, not only a bad key",
                status=wire.status,
                request_id=wire.request_id,
                source=wire.source,
            )
        else:
            public = MassiveError(
                f"the WebSocket connection to {self.decision.host}{self.path} failed: "
                f"{error.message}",
                status=error.status,
                source=wire_source(error),  # an internal error of the client is a defect
            )
        self._last_error = str(public)
        log_failure(logger, public, "%s")
        with self._lock:  # one section: no verb leases the connection once it has failed
            self.failure = public
            routed = {sink.stream_id for sink in self.flusher.routes.sinks()}
            self._group.retire(self, idle=False)
        self.flusher.fail(routed, public)
        self.flusher.stop(final=True)  # the failures above are applied before it ends

    # --- loop thread: failures and drift -------------------------------------------------------

    def _refused(self, topics: tuple[str, ...], message: str) -> None:
        """Fail the streams of refused topics with `EntitlementError`, naming the feed's fix.

        A real-time-only dataset's streams add its documented feed and its page
        (`ConnectionStream.refusal_note`), and only theirs.

        Args:
            topics: The refused topics.
            message: The server's message, verbatim.
        """
        refused = set(topics)
        text = (
            f"the WebSocket server {self.decision.host}{self.path} refused the subscription to "
            f"{', '.join(sorted(refused))}; Massive said: {message}; "
            f"{self._refusal_hint(refused)}"
        )
        self._last_error = text
        # One error per note, so a note reaches only the streams that carry it.
        by_note: dict[str, list[int]] = {}
        for stream in self._streams.values():
            if stream.topics & refused:
                by_note.setdefault(stream.refusal_note, []).append(stream.stream_id)
        for note, ids in by_note.items():
            self.flusher.fail(ids, EntitlementError(f"{text}; {note}" if note else text))

    def _refusal_hint(self, refused: set[str]) -> str:
        """Return what a refusal's message says to do, by the connection's feed and its evidence.

        Each hint quotes what the host decision saw and names the setting that changes the
        outcome; none says what a plan includes, which is Massive's to answer.

        Args:
            refused: The refused topics.

        Returns:
            On a pinned host, that `MassiveConfig.websocket_hosts` chooses it; on the delayed
            feed, that the subscription is listed in `delayed_subscriptions` and that removing it
            uses the real-time feed; on the real-time feed of a key whose snapshot Massive
            labelled real-time, the label, and that the key's plan may not include the data;
            on the real-time feed of a key whose snapshot carried no label, that
            `websocket_hosts` pins another host, or the delayed feed if Massive serves the
            data delayed; on the real-time feed of a key whose snapshot Massive refused, that
            refusal, and that REST reads the data instead; on the real-time feed otherwise, how
            to use Massive's delayed feed instead; for fair-market-value topics, the one host
            Massive documents for them, which `websocket_hosts` never changes; on another
            host, that `websocket_hosts` chooses it; and for index values, the one host Massive
            documents for them.
        """
        subscription = subscription_of(self.asset_class)
        evidence = self.decision.evidence
        if evidence == "websocket_hosts":
            hint = (
                f"MassiveConfig.websocket_hosts[AssetClass.{self.asset_class.name}] pins this "
                "host; the key's plan may not include this data there"
            )
        elif self.decision.host.startswith(DELAYED_PREFIX):
            hint = (
                f"MassiveConfig.delayed_subscriptions lists dm.Subscription.{subscription.name}, "
                f"so its streams use Massive's delayed feed; to use the real-time feed, remove "
                f"dm.Subscription.{subscription.name} from delayed_subscriptions"
            )
        elif self.decision.host == REALTIME_HOST and shows_real_time(evidence):
            hint = (
                f"the {subscription} snapshot is real-time ({evidence}), but the key's plan may "
                "not include this data"
            )
        elif self.decision.host == REALTIME_HOST and unlabelled(evidence):
            pin = f'{{dm.AssetClass.{self.asset_class.name}: "<host>"}}'
            hint = (
                f"the {subscription} snapshot carried no recency label, so the library used the "
                "real-time host; if Massive serves this data on another host, pin it with "
                f"MassiveConfig(websocket_hosts={pin}); "
                f"if Massive serves your {subscription} data delayed, use its delayed feed: "
                f"{delayed_fix(subscription)}"
            )
        elif self.decision.host == REALTIME_HOST and refused_snapshot(evidence):
            hint = (
                f"Massive also refused the {subscription} snapshot ({evidence}), so the key's "
                f"plan may include no live (WebSocket) {subscription} data; read it with "
                "fetch(), poll(), or history() instead"
            )
        elif self.decision.host == REALTIME_HOST:
            hint = (
                "the key's plan may not include this data in real time; if Massive serves your "
                f"{subscription} data delayed, use its delayed feed: "
                f"{delayed_fix(subscription)}"
            )
        elif any(channel_of(t) == _FMV_CHANNEL for t in refused):
            hint = (
                f"fair-market-value (FMV) data streams only from {FMV_HOST}, the one host "
                "Massive documents for it; MassiveConfig.websocket_hosts never applies to FMV"
            )
        else:
            hint = (
                "the key's plan may not include this data on this "
                f"host; MassiveConfig.websocket_hosts[AssetClass.{self.asset_class.name}] chooses "
                "the host"
            )
        if self.asset_class is AssetClass.INDICES and any(channel_of(t) == "V" for t in refused):
            hint += " (Massive documents index values only on business.massive.com)"
        return hint

    # --- loop thread: outages and notes --------------------------------------------------------

    def _slot_text(self) -> str:
        """Return what a down stream's `Error` adds while the connection waits for a slot."""
        return (
            f"waiting for a WebSocket slot on {self.decision.host}{self.path}: another "
            "connection with this API key, perhaps from another process, holds it; the "
            "account allows one connection per asset class"
        )

    def _outage(self, *, detail: str | None) -> None:
        """Start or continue an outage: hold every stream down, and fail each at its limit.

        The first `RECONNECTING` or `WAITING_SLOT` of an outage dates it at the last frame the
        connection received (when the data stopped), or now if none ever arrived; the note and
        the failure name that time. Each stream not yet down is held down with a loop timer
        (`_stream_expired`) that fires `MassiveConfig.max_outage` seconds after the later of
        that last frame and the drop less `KEEPALIVE_WINDOW`: a socket's keepalive pings are
        no frames, so after a quiet spell (a night, a weekend, an illiquid symbol) the last
        frame can be long past while the connection was healthy until moments before the
        drop. A connection that never received a frame (it never connected) counts from now,
        so its streams get the full `max_outage`. A limit of zero, or one already passed,
        fires on the loop's next turn. A stream an outage already failed is never held down
        again. Every attempt rewrites the note, since a wait for a slot, a taken connection,
        and a plain reconnect read differently.

        Args:
            detail: What the note adds after "no data", such as why the connection waits; None
                for a plain reconnect.
        """
        episodes = self._episodes
        now = time.time()
        last = max(episodes.last_frame_at, self._last_message_at or 0) / 1e9
        if episodes.down_since is None:
            episodes.down_since = last or now
            episodes.quiet_at = None  # the outage's note replaces the silence's
            episodes.quiet_note = None
        since = utc_text(episodes.down_since)
        note = f"connection down since {since}; no data"
        if detail is not None:
            note += f"; {detail}"
        start = max(last, now - KEEPALIVE_WINDOW) if last else now
        left = max(start + self._group.max_outage - now, 0.0)
        newly = [s for s in self._streams if s not in episodes.down and s not in episodes.expired]
        for stream_id in newly:
            self._hold_down(stream_id, left, since=episodes.down_since)
        logger.debug(
            "%s: outage since %s (the last frame); streams %s held down, failing in %.0f s "
            "unless they recover (max_outage %g s)",
            self.url,
            since,
            newly,
            left,
            self._group.max_outage,
        )
        changed = note != episodes.down_note
        episodes.down_note = note
        self._show_outage_notes(list(episodes.down) if changed else newly)

    def _reconnect_deadline(self) -> float | None:
        """Return the seconds by which the next attempt must authenticate to save every stream.

        The client asks before each reconnect wait. A stream held down by the outage fails
        when its timer fires, so the next attempt must authenticate, and its subscriptions be
        acknowledged within `subscription_confirmation_timeout`, before the earliest timer: the
        client shortens its wait to fit (never below the reconnect backoff). The waits alone (30,
        60, then 100 to 120 seconds) would otherwise put the last attempt before a 300-second
        `max_outage` too late to save it.

        Returns:
            The earliest held-down stream's time left, less `subscription_confirmation_timeout`, in
            seconds; None when no stream is held down.
        """
        timers = self._episodes.down.values()
        if not timers:
            return None
        earliest = min(timer.when() for timer in timers)
        return earliest - self._client.loop.time() - self._group.subscription_confirmation_timeout

    def _hold_down(self, stream_id: int, left: float, *, since: float) -> None:
        """Hold a stream down in the outage: it fails in `left` seconds unless it recovers.

        Args:
            stream_id: The stream, one of the connection's.
            left: Seconds until its `max_outage` runs out.
            since: When its rows may have started to go missing, seconds since the epoch: the
                start of its gap, should it recover.
        """
        episodes = self._episodes
        episodes.gap_since[stream_id] = since
        episodes.down[stream_id] = self._client.loop.call_later(
            left, self._stream_expired, stream_id
        )

    def _release_down(self, stream_id: int) -> None:
        """Stop holding a stream down: cancel its timer. Does nothing for a stream not down.

        Args:
            stream_id: The stream.
        """
        timer = self._episodes.down.pop(stream_id, None)
        if timer is not None:
            timer.cancel()
        self._episodes.gap_since.pop(stream_id, None)

    def _stream_expired(self, stream_id: int) -> None:
        """Fail a stream the outage has held down for `max_outage`.

        Its rows from the outage's start on are missing, so a table that kept running (a
        `last_by`, say) would show the rows from before the drop as if they were current. It
        fails through its flush thread's `publish_failure`, and a table derived from it fails
        with it. The connection keeps reconnecting.

        Args:
            stream_id: The stream; still on the connection, since its release cancels this.
        """
        episodes = self._episodes
        del episodes.down[stream_id]
        episodes.gap_since.pop(stream_id, None)
        episodes.expired.add(stream_id)
        logger.debug("%s: stream %d stayed down for max_outage; failing it", self.url, stream_id)
        if episodes.failure is None:
            since = utc_text(required(episodes.down_since, "the outage's start"))
            episodes.failure = MassiveError(
                f"the WebSocket connection to {self.decision.host}{self.path} has been down "
                f"since {since}, longer than MassiveConfig.max_outage "
                f"({self._group.max_outage:g} s); the rows from {since} on are missing, so this "
                "table stopped; call the verb again for a new live table",
                source=ErrorSource.NETWORK,
            )
        self.flusher.fail([stream_id], episodes.failure)

    @staticmethod
    def _recovered(stream: ConnectionStream, topics: _TopicStates) -> bool:
        """Tell whether a down stream's data flows again on the reconnected socket.

        It does once rows that arrived on the current socket reached it (its sink's
        `rows_generation`), so rows of the dropped socket that a slow flush published late
        never count, or once each of its topics the options cap admits is acknowledged
        (directly or through its channel's wildcard). A topic in `NO_ACK` keeps down a stream
        that has received rows: Massive never confirmed it, so its rows may never come, and
        its last values could look current. A stream that has never received a row shows no
        stale data, so it recovers once each such topic is acknowledged or in `NO_ACK`, and
        runs on with its no-acknowledgement note.

        Args:
            stream: A down stream.
            topics: The connection's topic states, read once per check.

        Returns:
            Whether it has recovered.
        """
        sink = stream.sink
        if sink.rows_generation == topics.generation:
            return True
        resolved = topics.acknowledged if sink.rows_published else topics.answered
        return all(
            t in resolved or wildcard(channel_of(t)) in topics.acknowledged
            for t in stream.topics - topics.deferred
        )

    def _check_recovered(self) -> None:
        """Release each down stream whose data flows again; end the outage when all have.

        Runs while the connection is `CONNECTED` after a change of its subscriptions, a data
        frame, each summary, and a flush that gave a stream its first rows of the socket. A
        recovered stream's timer is cancelled and its note cleared, while a stream with rows
        and a topic in `NO_ACK` stays down and fails at its limit (`_recovered`), so one
        silent topic never holds the others down. The outage
        ends once no stream is down and the socket has proved itself: a topic is
        acknowledged, or data arrived; that end is logged at INFO with the outage's length. A
        socket that the server accepts but never answers is still down, so a stream that opens
        on it is held down too.
        """
        episodes = self._episodes
        if episodes.down_since is None or self._state is not ConnectionState.CONNECTED:
            return
        subscriptions = self.subscriptions
        acknowledged = subscriptions.acknowledged
        topics = _TopicStates(
            generation=required(subscriptions.generation, "a connected socket's generation"),
            acknowledged=acknowledged,
            answered=acknowledged | subscriptions.no_ack,
            deferred=subscriptions.deferred,
        )
        recovered = [s for s in episodes.down if self._recovered(self._streams[s], topics)]
        starts = {s: episodes.gap_since[s] for s in recovered}
        for stream_id in recovered:
            self._release_down(stream_id)
        if recovered:
            logger.debug(
                "%s: streams %s recovered: data again, their topics acknowledged, or, for a "
                "stream that never received a row, each topic acknowledged or in NO_ACK; each "
                "ends its outage span in m.problems(), which began when its rows may have gone "
                "missing: %s",
                self.url,
                bounded_list(recovered),
                bounded_list(f"{s} from {utc_text(start)}" for s, start in starts.items()),
            )
            self._show_outage_notes(recovered)
        if episodes.down or not (episodes.data_since_open or self.subscriptions.acknowledged):
            return
        logger.log(
            # INFO is the recovery of the outage's WARNING, which only live tables cause.
            logging.INFO if self._streams else logging.DEBUG,
            "%s: the outage that began at %s is over after %.0f s: Massive confirmed the "
            "subscriptions of its %d live tables, or sent them rows",
            self.url,
            utc_text(episodes.down_since),
            time.time() - episodes.down_since,
            len(self._streams),
        )
        episodes.down_since = None
        episodes.slot_warned = False
        episodes.down_note = None
        episodes.failure = None

    def _cancel_outage_timers(self) -> None:
        """Cancel every down stream's timer: the connection has ended or handed its streams on."""
        for stream_id in list(self._episodes.down):
            self._release_down(stream_id)

    def _show_outage_notes(self, stream_ids: Iterable[int]) -> None:
        """Report each stream's outage and quiet-feed spans, or end them.

        A down stream has an `outage` span from when its rows may have gone missing
        (`_Episodes.gap_since`) until it recovers; every stream a `quiet_feed` span while the
        connection hears nothing during trading hours. The other spans (the options cap, a
        stale symbol) are their sources' own.

        Args:
            stream_ids: The streams whose spans may have changed.
        """
        episodes = self._episodes
        quiet = (
            {} if episodes.quiet_note is None else {"quiet": (None, None, episodes.quiet_note, 0)}
        )
        changes: dict[int, dict[ProblemCause, tuple[_Current, int | None]]] = {}
        for stream_id in stream_ids:
            note = episodes.down_note if stream_id in episodes.down else None
            since = episodes.gap_since.get(stream_id)
            changes[stream_id] = {
                ProblemCause.OUTAGE: (
                    {} if note is None else {"outage": (None, None, note, 0)},
                    None if since is None else int(since * 1e9),
                ),
                ProblemCause.QUIET_FEED: (quiet, episodes.quiet_at),
            }
        self._report_spans(changes)

    def _report_spans(
        self, changes: Mapping[int, Mapping[ProblemCause, tuple[_Current, int | None]]]
    ) -> None:
        """Hand each stream's changed spans to its own session, one call per session.

        Args:
            changes: Per stream, per cause, the spans that hold now (`SpanTracker.update`) and
                when a span that holds anew began (None for now).
        """
        by_core: dict[SessionCore, list[Problem]] = {}
        for stream_id, causes in changes.items():
            # Every caller passes streams the connection holds: a stream held down leaves
            # `_episodes.down` when it leaves `_streams`.
            stream = self._streams[stream_id]
            core = stream.sink.core()
            if core is None:  # a freed session shows no problem
                continue
            trackers = self._spans.setdefault(stream_id, {})
            for cause, (current, since) in causes.items():
                tracker = trackers.get(cause)
                if tracker is None:
                    if not current:
                        continue
                    tracker = trackers[cause] = SpanTracker(
                        cause, dataset=stream.plan.dataset, stream_id=stream_id, where=self.url
                    )
                spans = tracker.update(current, since=since)
                if spans is not None:
                    by_core.setdefault(core, []).append(spans)
        for core, found in by_core.items():
            core.problems(found)

    def _end_spans(
        self, stream: ConnectionStream, trackers: Mapping[ProblemCause, SpanTracker]
    ) -> None:
        """End the spans of a stream that leaves the connection.

        Args:
            stream: The stream.
            trackers: Its spans, by cause.
        """
        core = stream.sink.core()
        ended = [spans for t in trackers.values() if (spans := t.update({})) is not None]
        if core is not None and ended:
            core.problems(ended)

    @staticmethod
    def _no_ack_text(missing: frozenset[str]) -> str:
        """Return the `Error` part of a stream whose topics Massive has not acknowledged.

        Args:
            missing: The stream's topics in `NO_ACK`; at least one.

        Returns:
            The text, naming the first `_NOTE_TOPICS` of them.
        """
        shown = sorted(missing)[:_NOTE_TOPICS]
        more = len(missing) - len(shown)
        return (
            f"no acknowledgement from Massive for {', '.join(shown)}"
            f"{f' and {more} more' if more else ''}; "
            f"{'its' if len(missing) == 1 else 'their'} rows may be missing; the subscription "
            "is re-sent until Massive acknowledges it"
        )

    def _cap_text(self) -> str:
        """Return the `Detail` of an options stream's `options_cap` span.

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
        """Publish the streams' notes and rewrite the row for the changes of a loop turn."""
        self._change_scheduled = False
        self._show_notes()
        self._check_recovered()
        self._write_row()

    def _show_notes(self, *, opened: Collection[int] = ()) -> None:
        """Publish the admitted options topics, and report each stream's cap and no-ack spans.

        A stream's spans come from its options topics the cap defers (with `Deferred`, the stream's
        own deferred topics, one per contract since a stream reads one channel) and its topics
        Massive has not acknowledged after a re-send (`NO_ACK`). Neither fails the stream; each
        is a span of its problems (`options_cap`, `unacknowledged_topics`), opened and ended
        alone. A span ends once it no longer holds: room frees, a late acknowledgement arrives,
        or a new generation starts. On
        `/options`, the admitted set is published, and an opening stream's pending topics
        dropped, in one section of the group's lock, so a verb's cap check never sees the
        stream's topics in neither.

        Args:
            opened: The streams whose pending topics are now counted through the admitted set.
        """
        options = self.asset_class is AssetClass.OPTIONS
        if options:
            with self._lock:
                self.cap_admitted = self.subscriptions.admitted
                for stream_id in opened:
                    self.cap_pending.pop(stream_id, None)
        deferred = self.subscriptions.deferred
        no_ack = self.subscriptions.no_ack
        if not deferred and not no_ack and not self._notes_shown:
            return
        updates: dict[int, StreamUpdate] = {}
        spans: dict[int, dict[ProblemCause, tuple[_Current, int | None]]] = {}
        for stream in self._streams.values():
            count = len(stream.topics & deferred) if deferred else 0
            missing = stream.topics & no_ack if no_ack else frozenset()
            old = self._notes_shown.get(stream.stream_id)
            old_count, old_missing = old or (0, frozenset())
            if (count, missing) == (old_count, old_missing):
                continue
            note = (count, missing) if count or missing else None
            if note is None:
                del self._notes_shown[stream.stream_id]
            else:
                self._notes_shown[stream.stream_id] = note
            self._log_note(stream, count=count, old_count=old_count, missing=missing)
            # Two spans of the stream's problems, each opened or ended alone.
            spans[stream.stream_id] = {
                ProblemCause.OPTIONS_CAP: (
                    {"cap": (None, None, self._cap_text(), count)} if count else {},
                    None,
                ),
                ProblemCause.UNACKNOWLEDGED_TOPICS: (
                    {"no_ack": (None, None, self._no_ack_text(missing), len(missing))}
                    if missing
                    else {},
                    None,
                ),
            }
            if options:
                updates[stream.stream_id] = StreamUpdate(deferred=count)
        self._report_spans(spans)
        if updates:
            self._update_streams(updates)

    def _log_note(
        self, stream: ConnectionStream, *, count: int, old_count: int, missing: frozenset[str]
    ) -> None:
        """Log a change of a stream's notes: the cap's at three levels, no-acks at DEBUG.

        A stream whose deferred contracts go from none to some logs a WARNING, each later
        change of their count a DEBUG line, and their return to none an INFO, so a cap that
        moves with every subscription change warns once per episode. The no-acks were logged
        at WARNING, once per step, when they happened.

        Args:
            stream: The stream.
            count: Its deferred options topics now.
            old_count: Its deferred options topics before.
            missing: Its unacknowledged topics now.
        """
        if count and not old_count:
            logger.warning(
                "%s stream %d: %s", stream.plan.dataset, stream.stream_id, self._cap_text()
            )
        elif count != old_count and count:
            logger.debug(
                "%s stream %d: %d contracts deferred, %d before; %s",
                stream.plan.dataset,
                stream.stream_id,
                count,
                old_count,
                self._cap_text(),
            )
        elif old_count and not count:
            logger.info(
                "%s stream %d: no contract is deferred any more",
                stream.plan.dataset,
                stream.stream_id,
            )
        logger.debug(
            "%s stream %d: unacknowledged topics %s",
            stream.plan.dataset,
            stream.stream_id,
            bounded_list(sorted(missing)),
        )

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
        on_data: Callable[[int, str, list[tuple[object, ...]], RowMarks, DriftReport], None],
        on_state: Callable[[WsState, float | WireError | None], None],
        reconnect_deadline: Callable[[], float | None],
    ) -> _Client:
        """Return a new, unstarted `WsClient` of `url`.

        Args:
            url: The endpoint.
            sources: The WebSocket sources of the cluster.
            on_open: The connection's `on_open`.
            on_status: The connection's `on_status`.
            on_data: The connection's `on_data`.
            on_state: The connection's `on_state`.
            reconnect_deadline: The connection's `_reconnect_deadline`.

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
            reconnect_deadline=reconnect_deadline,
            max_reconnect_backoff=self._max_backoff,
            thread_prefix=self._thread_prefix,
        )


_groups_lock = threading.Lock()

_groups: dict[Hashable, ConnectionGroup] = {}


def _sharing_value(name: str, value: object) -> Hashable:
    """Return a connection setting as it appears in a group's sharing key.

    The key is process-wide and public (`ConnectionGroup.key`), so the API key appears as its
    SHA-256 digest, never itself: no repr or traceback of the key can show the secret. A mapping
    (`websocket_hosts`) becomes a frozenset of its items.

    Args:
        name: One of `CONNECTION_SETTINGS`.
        value: The session's value of it.

    Returns:
        A hashable value, equal for equal settings.
    """
    if name == "api_key":
        return hashlib.sha256(
            required_instance(value, str, "the api_key setting").encode()
        ).digest()
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
    session of each stream, and the crypto pairs its sessions looked up.
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
        subscription_confirmation_timeout: float,
        flush_interval: float,
        max_outage: float,
    ) -> None:
        """Create an empty group; `attach` creates them.

        Args:
            key: Its key in the process's registry: the sharing key of `attach`.
            label: Its label, `"massive group #<n>"`, which starts the name of every thread its
                connections run (their clients and flush threads), since
                they serve several sessions.
            factory: Creates each connection's client.
            subscription_confirmation_timeout: `MassiveConfig.subscription_confirmation_timeout`
                of its sessions.
            flush_interval: `MassiveConfig.flush_interval` of its sessions.
            max_outage: `MassiveConfig.max_outage` of its sessions, in seconds.
        """
        self.key: Final = key
        self.label: Final = label
        self.lock: Final = threading.RLock()
        self.factory: Final = factory
        self.subscription_confirmation_timeout: Final = subscription_confirmation_timeout
        self.flush_interval: Final = flush_interval
        self.max_outage: Final = max_outage
        # Guarded by lock.
        self.connections: dict[str, Connection] = {}
        self.streams: dict[int, Connection] = {}
        self.owners: dict[int, WsConnections] = {}
        self.sessions: set[GroupSession] = set()
        self._calendar: FuturesCalendar | None = None
        # The base and quote of each crypto symbol a session looked up: vendor facts, the same
        # for every session of the key, which the connections' topics are built from.
        self.pairs: dict[str, tuple[str, str]] = {}

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
                    subscription_confirmation_timeout=config.subscription_confirmation_timeout,
                    flush_interval=config.flush_interval,
                    max_outage=config.max_outage,
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

    def wake_schedule_waiters(self) -> None:
        """Wake every thread waiting for futures schedules, so each checks its stop event.

        A session's close calls it after it set its stop event (`FuturesCalendar.wake`). The
        calendar is read under the lock and woken outside it; a group without a calendar has
        no waiter. Never blocks.
        """
        with self.lock:
            calendar = self._calendar
        if calendar is not None:
            calendar.wake()

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
            SessionClosedError: If every session has left the group meanwhile.
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
            raise SessionClosedError(
                f"every session of the WebSocket connection group has closed; {NEW_SESSION}"
            )
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
            SessionClosedError: If no session of the group is open.
            MassiveError: If the read fails.
        """
        with self.lock:
            open_ = [o for o in self.sessions if not o.closed and o.core() is not None]
        if not open_:
            raise SessionClosedError(
                f"every session of the WebSocket connection group has closed; {NEW_SESSION}"
            )
        return open_[0].fetch_rows(template, query, bucket=bucket, cancel=cancel)

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
        return True
