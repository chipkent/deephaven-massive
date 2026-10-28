"""The WebSocket transport: one connection per URL over `WsClient`, and the streams on them.

`WsTransport` is the session's `StreamTransport`, over the session's one `WsConnections`. A
stream verb (`blink()`, `append_only()`, `ring()`, `last_by()`) that finds no live stream for
its `(dataset, channel, symbols)` asks it to open one: the transport decides the host of the
class and family (`RestTransport.resolve_host`, once per session), finds or creates the
session's connection to `wss://<host>/<cluster>`, creates the stream's tables
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
- **Lifetime.** A connection with no stream attached for `IDLE_GRACE` seconds closes itself
  and leaves the registry. A fatal failure (a rejected key, a handshake error retrying cannot
  fix) fails every stream on it, a stream opening at that moment included. `m.close()` closes
  every connection and waits a bounded time for its threads.

**Threads and locks.** Each connection's subscription state is confined to its client's loop
thread; other threads reach it only through `loop.call_soon_threadsafe` with plain data
(`Connection.post`). The registries that map URLs to connections and streams to connections,
each connection's leases, failure, and options-cap bookkeeping, and the session's record of
`NO_ACK` topics are guarded by the session's `registry_lock`, which is a leaf lock here too: no
Deephaven call, network call, or wait happens under it (creating a client loads TLS settings, so
it happens outside). The loop thread makes no Java call: every failure goes to a flush thread,
or, for a stream whose connection failed while it opened, to the opening caller. A stream's
blink-table shutdown callback takes the lock only to update the registry and then posts the
release of the stream's topics to its connection's loop; it never sends and never calls Java.
The transport and its connections hold the session core weakly, so a session dropped without
`close()` is still collected and its closer runs.
"""

from __future__ import annotations

import functools
import logging
import threading
import time
import weakref
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Protocol, cast

from deephaven_massive import _massive
from deephaven_massive._datasets.request import HostDecision
from deephaven_massive._datasets.symbols import SymbolTable, canonical
from deephaven_massive._enums import AssetClass, ChannelFamily, Recency
from deephaven_massive._errors import EntitlementError, MassiveError, from_wire
from deephaven_massive._ingest.subscriptions import (
    Sender,
    Subscriptions,
    Timer,
    channel_of,
    is_wildcard,
    stream_topics,
    wildcard,
)
from deephaven_massive._massive import Transport, WireAuthError, WsState
from deephaven_massive._tables.diagnostics import (
    ConnectionRow,
    ConnectionState,
    StreamUpdate,
    describe_symbols,
)
from deephaven_massive._tables.stream import ChannelPlan, Flusher, StreamSink, open_stream_tables
from deephaven_massive._types import _All

if TYPE_CHECKING:
    import asyncio
    from collections.abc import Callable, Sequence

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._massive import DriftReport, Reject, Source, Status, WireError
    from deephaven_massive._tables.core import SessionCore
    from deephaven_massive._tables.liveness import StreamHandle

__all__ = [
    "IDLE_GRACE",
    "JOIN_TIMEOUT",
    "OPTIONS_CAP",
    "ClientFactory",
    "Connection",
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
        sink: The stream as the flush thread publishes to it.
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
    """The session's connection to one `wss://<host>/<cluster>`, its flush thread, and its streams.

    Created by `WsConnections` (outside every lock: creating the client loads TLS settings) and
    registered under `registry_lock`; started by its creator, or after a delay when it replaces
    a connection the server's real-time verdict moved. Its loop-thread state (the
    `Subscriptions`, the streams, the diagnostics row) is touched only by the client's loop
    thread, which runs the callbacks below and the work other threads `post`. `families`,
    `leases`, `retired`, `failure`, `awaiting`, and the options-cap fields are guarded by
    `registry_lock`. `close()` and `join()` may be called from any thread.

    It writes its diagnostics row (and one per further family it serves) under `registry_lock`
    and only while it is its key's current connection, so a connection that left the registry
    never overwrites the row of the one that replaced it.
    """

    def __init__(
        self,
        transport: WsConnections,
        core: SessionCore,
        *,
        url: str,
        asset_class: AssetClass,
        family: ChannelFamily,
        decision: HostDecision,
        client_factory: ClientFactory,
    ) -> None:
        """Create the connection, its client, and its flush thread; nothing starts yet.

        Args:
            transport: The transport that owns it (for the registry and the verdict).
            core: The session's core, held weakly.
            url: The endpoint, `wss://<host>/<cluster>`.
            asset_class: The cluster's class.
            family: The family of the stream that created it.
            decision: The host decision behind `url`.
            client_factory: Creates the client.
        """
        self.url: Final = url
        self.asset_class: Final = asset_class
        self.decision: Final = decision
        self.path: Final = f"/{asset_class.value}"
        self._transport = transport
        self._core = weakref.ref(core)
        self._lock: Final = core.registry_lock
        # Guarded by registry_lock.
        self.families: set[ChannelFamily] = {family}
        self.leases = 0
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
        config = core.config
        self.flusher: Final = Flusher(
            core, f"massive-flush {decision.host}{self.path}", interval=config.flush_interval
        )
        cluster = f"{asset_class.value}/"
        sources = [s for k, s in _massive.sources(Transport.WS).items() if k.startswith(cluster)]
        self._client = client_factory(
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
            ack_timeout=config.ack_timeout,
            cap=OPTIONS_CAP if asset_class is AssetClass.OPTIONS else None,
            on_refused=self._refused,
            on_step_refused=self._step_refused,
            first_no_ack=functools.partial(transport.first_no_ack, self.path),
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

    @property
    def key(self) -> tuple[str, str, str]:
        """The connection's owner key in the drift registry: transport, class, and family."""
        with self._lock:
            family = min(self.families)
        return Transport.WS.value, self.asset_class.value, family.value

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
                included) before it ends; False when the session closes and tables go static.
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

        Args:
            stream: The stream.
        """
        self._register_stream(stream)
        self._after_change(opened=stream.stream_id)

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
        topics = stream_topics(self.asset_class, stream.channel, symbols)
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
        with self._lock:
            awaited = set(self.awaiting)
        for stream_id in [s for s in self._early if s not in awaited]:
            del self._early[stream_id]
        self._after_change()

    def _after_change(self, *, opened: int | None = None) -> None:
        """After a change of streams or topics: reconcile, publish the cap, maybe go idle.

        Args:
            opened: The stream just added, whose topics now count against the options cap
                through the admitted set instead of as pending.
        """
        self.subscriptions.reconcile()
        self._show_deferred(opened=opened)
        if not self._streams and self._idle is None:
            self._idle = self._client.loop.call_later(IDLE_GRACE, self._idle_close)

    def _idle_close(self) -> None:
        """Close the connection if it has had no stream for the grace period."""
        self._idle = None
        if self._streams or not self._transport.retire(self, idle=True):
            return
        logger.info("closing %s: no stream for %.0f s", self.url, IDLE_GRACE)
        self.close()

    def hand_over(self, successor: Connection) -> None:
        """Move every stream to `successor` and close. Loop thread.

        Called when the server's real-time verdict moves the connection to another host. Under
        `registry_lock`, the streams are routed on `successor` too (or, if it has already
        failed, they fail here); their topics reach its loop with `adopt`; a release or symbol
        change that still arrives here is forwarded. The streams stay routed on this
        connection as well, until its flush thread ends: its last flush applies the failures it
        has queued and publishes the rows it has buffered (a stream's publish lock keeps the two
        flush threads from interleaving on one stream).

        Args:
            successor: The connection that takes over.
        """
        streams = list(self._streams.values())
        self._streams.clear()
        self._successor = successor
        with self._lock:
            failure = successor.failure
            if failure is None:
                for stream in streams:
                    successor.flusher.routes.add(stream.channel, stream.sink)
        if failure is not None:
            self.flusher.fail([s.stream_id for s in streams], failure)
        else:
            successor.post(successor.adopt, streams)
        self.subscriptions.ended()
        self.close(final_flush=True)

    def adopt(self, streams: list[_Stream]) -> None:
        """Take over the streams of a connection the verdict moved, and the calls they missed.

        Args:
            streams: The streams, with their topics.
        """
        with self._lock:
            self.awaiting.difference_update(s.stream_id for s in streams)
        for stream in streams:
            self._register_stream(stream)
        update = StreamUpdate(host=self.decision.host, path=self.path)
        self._update_streams({s.stream_id: update for s in streams})
        for stream in streams:
            for callback, args in self._early.pop(stream.stream_id, ()):
                callback(stream.stream_id, *args)
        self._after_change()

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
        if state in {WsState.RECONNECTING, WsState.WAITING_SLOT}:
            self.subscriptions.ended()
            self._next_reconnect_at = int(cast("float", detail) * 1e9)
        elif state is WsState.FAILED:
            self.subscriptions.ended()
            self._failed(cast("WireError", detail))
        elif state is WsState.CLOSED:
            self.subscriptions.ended()
            self._next_reconnect_at = None
        self._state = ConnectionState(state.value)
        self._write_row()

    def _failed(self, error: WireError) -> None:
        """Fail every stream after a fatal failure, and leave the registry.

        The failure is recorded under `registry_lock` together with the routed streams it
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
            self._transport.retire(self, idle=False)
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
        if reject.symbol is not None:
            try:
                symbol = canonical(self.asset_class, reject.symbol)
            except ValueError:
                symbol = None
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
        return self._transport.reroute(self, status.message)

    def _record_drift(self, channel: str, drift: DriftReport) -> None:
        """Add a frame's drift to the connection's row and hand it to the flush thread.

        Args:
            channel: The channel.
            drift: Its drift in the frame.
        """
        core = self._core()
        if core is not None:
            text = core.drift.record(self.key, channel, drift)
            if text is not None:
                self._drift = text
                self._write_row()
        self.flusher.drift(channel, drift)

    # --- loop thread: diagnostics --------------------------------------------------------------

    def _changed(self) -> None:
        """Publish the options-cap state and rewrite the row after a subscription change."""
        self._show_deferred()
        self._write_row()

    def _show_deferred(self, *, opened: int | None = None) -> None:
        """Publish the admitted options contracts and show each stream's deferred count.

        The admitted set is published, and an opening stream's pending topics dropped, in one
        `registry_lock` section, so a verb's cap check never sees the stream's topics in
        neither.

        Args:
            opened: A stream whose pending topics are now counted through the admitted set.
        """
        if self.asset_class is not AssetClass.OPTIONS:
            return
        with self._lock:
            self.cap_admitted = self.subscriptions.admitted
            if opened is not None:
                self.cap_pending.pop(opened, None)
        deferred = self.subscriptions.deferred
        updates: dict[int, StreamUpdate] = {}
        wanted = self.subscriptions.explicit
        for stream in self._streams.values():
            count = len(stream.topics & deferred)
            if count == self._deferred_shown.get(stream.stream_id, 0):
                continue
            self._deferred_shown[stream.stream_id] = count
            if count:
                text = f"over capacity: {wanted}/{OPTIONS_CAP}; {len(deferred)} deferred"
                logger.warning("%s stream %d: %s", stream.plan.dataset, stream.stream_id, text)
                updates[stream.stream_id] = StreamUpdate(deferred=count, error=text)
            else:
                logger.warning(
                    "%s stream %d: no contract is deferred any more",
                    stream.plan.dataset,
                    stream.stream_id,
                )
                updates[stream.stream_id] = StreamUpdate(deferred=0, clear_error=True)
        if updates:
            self._update_streams(updates)

    def _update_streams(self, updates: dict[int, StreamUpdate]) -> None:
        """Apply changes to streams' `m.streams()` rows, if the session still exists.

        Args:
            updates: The change of each stream, by id.
        """
        core = self._core()
        if core is not None:
            core.update_streams(updates)

    def _write_row(self) -> None:
        """Write the connection's diagnostics rows, if it is still its keys' connection."""
        core = self._core()
        if core is None:
            return
        topics, acked, pending, no_ack = self.subscriptions.counts()
        now = time.time_ns()
        self._message_row_at = now
        with self._lock:
            for family in sorted(self.families):
                if not self._transport.owns_row(self, family):
                    continue
                core.diagnostics.write(
                    ConnectionRow(
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
                        reconnects=self._reconnects,
                        last_message_at=self._last_message_at,
                        next_reconnect_at=self._next_reconnect_at,
                        updated_at=now,
                        last_error=self._last_error,
                        schema_drift=self._drift,
                    )
                )


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


def _wanted(
    asset_class: AssetClass, channel: str, symbols: NormalizedSymbols
) -> tuple[frozenset[str] | None, frozenset[str]]:
    """Return what a new stream wants: its symbols and its topics at the start.

    Args:
        asset_class: The stream's class.
        channel: Its channel.
        symbols: Its normalized symbols.

    Returns:
        `(symbols, topics)`: for `dm.ALL`, None and the channel's wildcard; for a symbol
        list, the symbols and their topics; for a reactive table, two empty sets, which its
        watch fills once it starts.
    """
    if isinstance(symbols, _All):
        return None, frozenset({wildcard(channel)})
    if isinstance(symbols, SymbolTable):
        return frozenset(), frozenset()
    return frozenset(symbols), stream_topics(asset_class, channel, symbols)


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


class WsConnections:
    """The session's WebSocket connections, by URL, and the streams on them.

    Created once per session by `SessionClients.streams()`, which registers `close` with the
    session and hands it to each `WsTransport` it returns. It lives as long as the session's
    clients, so it holds the session core only weakly: the core holds it (through its
    `SessionClients`), and its `close` is a session closer, which must not keep the core alive.
    `open_stream` runs on the caller thread, which passes the core; the blink tables' shutdown
    callbacks and the symbol watches call in from any thread. Its registries (connections by
    URL, the connection of each stream, the connection that owns each diagnostics key, the
    `NO_ACK` topics, and the channel plans) are guarded by the session's `registry_lock`.

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
        client_factory: ClientFactory | None = None,
    ) -> None:
        """Create the registry; no connection is opened.

        Args:
            core: The session's core, held weakly.
            resolve_host: Decides the host of a (class, family), once per session; the REST
                transport's `resolve_host`, which may make one REST request.
            record_verdict: Replaces the cached decision of a (class, family) with the one the
                server's real-time verdict implies; the REST transport's `record_verdict`.
            client_factory: Creates each connection's client; by default a `WsClient` with the
                session's key and backoffs.
        """
        config = core.config
        self._core = weakref.ref(core)
        self._label = core.label
        self._lock: Final = core.registry_lock
        self._resolve = resolve_host
        self._record_verdict = record_verdict
        self._factory = client_factory or _WsClientFactory(
            config.api_key,
            backoff=config.reconnect_backoff,
            max_backoff=config.max_reconnect_backoff,
        )
        self._reconnect_backoff = config.reconnect_backoff
        self._pinned = frozenset(config.ws_hosts)
        # Guarded by registry_lock.
        self._connections: dict[str, Connection] = {}
        self._streams: dict[int, Connection] = {}
        self._row_owners: dict[tuple[AssetClass, ChannelFamily], Connection] = {}
        self._no_acked: set[tuple[str, str]] = set()
        self._plans: dict[tuple[DatasetSpec, str], ChannelPlan] = {}
        self._closed = False

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
        `/options` connection raise the `ValueError` of `_check_cap`, and a closed transport
        the `MassiveError` of `_check_open`; host resolution raises the `EntitlementError` or
        `MassiveError` of `resolve_host`.
        """
        asset_class = cast("AssetClass", spec.asset_class)
        plan = self._plan(core, spec, channel)
        symbols = request.symbol_key()
        wanted, topics = _wanted(asset_class, channel, symbols)
        decision = self._resolve(asset_class, spec.family)
        connection, created = self._lease(core, spec, decision, stream_id=stream_id, topics=topics)
        try:
            if created:
                connection.start()
            opened = open_stream_tables(
                plan,
                core.catalog.dataset_schema(spec),
                symbols,
                stream_id=stream_id,
                host=decision.host,
                path=connection.path,
                on_shutdown=functools.partial(self._shut_down, stream_id),
                on_symbols=functools.partial(self._symbols_changed, stream_id),
            )
        except BaseException:
            self._abandon(connection, stream_id)
            raise
        try:
            static = not isinstance(symbols, SymbolTable)
            stream = _Stream(stream_id, channel, plan, opened.sink, wanted, topics, static)
            self._attach(core, stream)
            if opened.watch is not None:
                opened.watch.start()
        except BaseException:
            opened.handle.scope.release()
            self._abandon(connection, stream_id)
            raise
        return opened.handle

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
            built = ChannelPlan.build(core.catalog, spec, channel)
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
    ) -> tuple[Connection, bool]:
        """Find or create the connection of a stream and lease it for the stream.

        A new connection is created outside the lock (its client loads TLS settings) and
        registered under it. The registry is read again under the lock, and as often as it
        takes: a connection found before may have been retired meanwhile, and one registered by
        another thread meanwhile wins, the new one then closing unused.

        Args:
            core: The session's core.
            spec: The stream's dataset.
            decision: Its host decision.
            stream_id: The stream.
            topics: Its static topics, for the options cap.

        Returns:
            `(connection, created)`: created is True when the caller must start it.

        Static topics that would cross the options cap raise the `ValueError` of `_check_cap`,
        and a closed transport the `MassiveError` of `_check_open`.
        """
        asset_class = cast("AssetClass", spec.asset_class)
        url = f"wss://{decision.host}/{asset_class.value}"
        candidate: Connection | None = None
        try:
            while True:
                with self._lock:
                    self._check_open()
                    connection = self._connections.get(url)
                    self._check_cap(spec, connection, topics)
                    created = connection is None and candidate is not None
                    if created:
                        connection = self._connections[url] = cast("Connection", candidate)
                        candidate = None
                    if connection is not None:
                        connection.families.add(spec.family)
                        self._row_owners[asset_class, spec.family] = connection
                        if asset_class is AssetClass.OPTIONS:
                            connection.cap_pending[stream_id] = topics
                        connection.leases += 1
                        self._streams[stream_id] = connection
                        return connection, created
                candidate = Connection(
                    self,
                    core,
                    url=url,
                    asset_class=asset_class,
                    family=spec.family,
                    decision=decision,
                    client_factory=self._factory,
                )
        finally:
            if candidate is not None:  # lost the race, or refused: never registered
                candidate.close()

    def _check_open(self) -> None:
        """Refuse a stream once the transport is closed; `registry_lock` is held.

        Raises:
            MassiveError: If `close()` has run.
        """
        if self._is_closed():
            raise MassiveError(f"{self._label} is closed")

    def _is_closed(self) -> bool:
        """Tell whether `close()` has run; `registry_lock` is held.

        Returns:
            True once the transport is closed.
        """
        return self._closed

    @staticmethod
    def _check_cap(
        spec: DatasetSpec, connection: Connection | None, topics: frozenset[str]
    ) -> None:
        """Refuse static options topics that would cross the cap; `registry_lock` is held.

        In use are the contracts the connection admitted and those of streams still opening.

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
                f"{spec.name}: {len(new)} new option contracts and {len(in_use)} in use exceed "
                f"the {OPTIONS_CAP} contracts one /options connection carries"
            )

    def _attach(self, core: SessionCore, stream: _Stream) -> None:
        """Route a new stream's rows and hand the stream to its connection's loop thread.

        The stream goes to the connection that holds its lease now (the verdict may have moved
        it). Routing and the check for the connection's failure happen in one `registry_lock`
        section, the one the connection's `_failed` takes, so a connection that failed meanwhile
        never leaves the stream running: the stream fails here, on the caller thread. A stream
        whose lease is gone (the session closed) is left as it is.

        Args:
            core: The session's core.
            stream: The stream, with its topics.
        """
        with self._lock:
            connection = self._streams.get(stream.stream_id)
            failure = None if connection is None else connection.failure
            if connection is not None and failure is None:
                connection.flusher.routes.add(stream.channel, stream.sink)
                connection.post(connection.add_stream, stream)
        if failure is not None:
            stream.sink.fail(core, failure)

    def _abandon(self, connection: Connection, stream_id: int) -> None:
        """Return the lease of a stream whose open failed, and let its connection go idle.

        Idempotent: the shutdown callback may already have returned the lease.

        Args:
            connection: The stream's connection.
            stream_id: The stream.
        """
        with self._lock:
            connection.cap_pending.pop(stream_id, None)
            if self._streams.get(stream_id) is connection:
                del self._streams[stream_id]
                connection.leases -= 1
        connection.flusher.routes.remove(stream_id)
        connection.post(connection.check_idle)

    # --- callbacks from any thread -------------------------------------------------------------

    def _shut_down(self, stream_id: int) -> None:
        """The blink table's `on_shutdown_callback`: unregister the stream, release its topics.

        Runs on whichever thread dropped the table's last reference (the reaper, an
        update-graph thread, a gRPC thread). It does two things only: the registry updates
        (`SessionCore.stream_shut_down`, then the stream's lease, under `registry_lock`) and one
        `call_soon_threadsafe` to the connection's loop, which releases the topics there. A
        closed loop (the connection ended) is logged at INFO. It never raises.

        Args:
            stream_id: The stream.
        """
        try:
            core = self._core()
            if core is not None:
                core.stream_shut_down(stream_id)
            with self._lock:
                connection = self._streams.pop(stream_id, None)
                if connection is not None:
                    connection.leases -= 1
            if connection is not None:
                connection.post(connection.release_stream, stream_id)
        except Exception:
            logger.exception("%s: the shutdown of stream %d failed", self._label, stream_id)

    def _symbols_changed(
        self, stream_id: int, added: frozenset[str], removed: frozenset[str]
    ) -> None:
        """A reactive symbol table changed: post the change to the stream's connection.

        Runs on an update-graph thread (or the caller thread for the initial replay), with the
        update-graph lock held: it takes `registry_lock` only to find the connection, and never
        blocks.

        Args:
            stream_id: The stream.
            added: Canonical symbols the table now holds.
            removed: Canonical symbols it no longer holds.
        """
        with self._lock:
            connection = self._streams.get(stream_id)
        if connection is not None:
            connection.post(connection.change_symbols, stream_id, added, removed)

    # --- registry (connections call these) ------------------------------------------------------

    def owns_row(self, connection: Connection, family: ChannelFamily) -> bool:
        """Tell whether a connection writes the diagnostics row of its class and a family.

        Called with `registry_lock` held.

        Args:
            connection: The connection.
            family: One of its families.

        Returns:
            True while it is the latest connection created for the key.
        """
        return self._row_owners.get((connection.asset_class, family)) is connection

    def first_no_ack(self, path: str, topic: str) -> bool:
        """Record a topic's `NO_ACK` for the session; tell whether it is the topic's first.

        Kept here, not per connection, so a topic reconnects at most once per session, also
        across an idle close or a verdict's move. Takes `registry_lock` briefly.

        Args:
            path: The cluster path of the connection, such as `/stocks`.
            topic: The topic.

        Returns:
            True for the first `NO_ACK` of the topic on the cluster in the session.
        """
        with self._lock:
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
        core = self._core()
        with self._lock:
            if idle and connection.leases:
                return False
            if self._connections.get(connection.url) is connection:
                del self._connections[connection.url]
            connection.retired = True
            if core is not None:
                core.drift.forget(connection.key)
        return True

    def reroute(self, connection: Connection, message: str) -> bool:
        """Accept the server's real-time verdict on a connection: move it to the `delayed.` host.

        Called on the connection's loop thread. Never for a host pinned by
        `MassiveConfig.ws_hosts`, and once per connection; the decision is recorded with the
        REST transport, so later streams of the class and family resolve to the delayed host
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
        with self._lock:
            if self._is_closed() or connection.retired:
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
            self._reconnect_backoff,
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

        Runs on the thread `reroute` starts. The successor is the delayed host's connection,
        created here unless one exists; under `registry_lock`, the old connection leaves the
        registry and every lease moves, and the successor awaits those streams, so a release or
        watchlist change that reaches it before `adopt` waits there. The successor starts after
        `reconnect_backoff`; the old connection's loop then hands the streams over.

        Args:
            connection: The connection the verdict arrived on.
            decision: The delayed host's decision.
        """
        asset_class = connection.asset_class
        try:
            self._record_verdict(asset_class, ChannelFamily.TICK, decision)
        except Exception:
            logger.exception("could not record the real-time verdict for %s", connection.url)
        core = self._core()
        if core is None:
            return
        url = f"wss://{decision.host}{connection.path}"
        candidate = Connection(
            self,
            core,
            url=url,
            asset_class=asset_class,
            family=ChannelFamily.TICK,
            decision=decision,
            client_factory=self._factory,
        )
        successor: Connection | None = None
        moved: list[int] = []
        with self._lock:
            # Not if the session closed, or the connection failed (its streams fail with it).
            if not self._is_closed() and connection.failure is None:
                # A stream of the class may already have opened the delayed host's connection.
                successor = self._connections.setdefault(url, candidate)
                successor.families.add(ChannelFamily.TICK)
                if self._connections.get(connection.url) is connection:
                    del self._connections[connection.url]
                self._row_owners[asset_class, ChannelFamily.TICK] = successor
                for stream_id, owner in list(self._streams.items()):
                    if owner is connection:
                        self._streams[stream_id] = successor
                        successor.awaiting.add(stream_id)
                        successor.leases += 1
                        connection.leases -= 1
                        moved.append(stream_id)
        if successor is not candidate:
            candidate.close()
        if successor is None:  # the session closed or the connection failed meanwhile
            connection.close()
            return
        successor.start(delay=self._reconnect_backoff)  # does nothing if it already started
        if not connection.post(connection.hand_over, successor):
            # The old loop ended first: its streams were failed there and never arrive.
            with self._lock:
                successor.awaiting.difference_update(moved)
            successor.post(successor.check_idle)

    # --- closing -------------------------------------------------------------------------------

    def close(self) -> None:
        """Close every connection and wait a bounded time for its threads; idempotent.

        Runs as a session closer: on the thread that closes the session, or on the reaper for
        a session collected unclosed. Each connection's client is asked to close (which
        interrupts a backoff wait at once) and its flush thread to stop; then each is joined for
        at most `JOIN_TIMEOUT` seconds, with a WARNING for one that has not ended. Streams are not
        failed: their tables go static.
        """
        with self._lock:
            self._closed = True
            connections = list(self._connections.values())
            self._connections.clear()
            self._streams.clear()
        for connection in connections:
            connection.close()
        for connection in connections:
            if not connection.join(JOIN_TIMEOUT):
                logger.warning(
                    "%s: the connection to %s has not ended %.0f s after close()",
                    self._label,
                    connection.url,
                    JOIN_TIMEOUT,
                )
