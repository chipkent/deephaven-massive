"""The WebSocket transport: a session's registry of streams on shared connections.

`WsTransport` is the session's `StreamTransport`, over the session's one `WsConnections`. A
stream verb (`blink()`, `append_only()`, `ring()`, `last_by()`) that finds no live stream for
its `(dataset, channel, symbols)` asks it to open one: the transport decides the host of the
class and family (`RestTransport.resolve_host`, once per session), finds or creates the
connection to `wss://<host>/<cluster>`, creates the stream's tables
(`_tables/stream.py`), and adds the stream's topics to the connection. The wire itself (the
socket, authentication, frame decoding, reconnecting with backoff) belongs to the connection's
`_massive.WsClient`; the subscription protocol to `Subscriptions` (`_ingest/subscriptions.py`);
one connection, and the group that shares connections between sessions, to
`_ingest/ws_connection.py` (`Connection`, `ConnectionGroup`); the session's side of
everything else belongs here:

- **Refusals.** A refused topic fails exactly the streams that want it, with
  `EntitlementError`; nothing tears down the connection. A refusal that names no topic
  applies to the whole step in flight, unless it is the server's real-time verdict on a
  `socket.` connection whose recency was unknown: then the decision is recorded with the REST
  transport (`RestTransport.record_verdict`), the connection's streams move to the `delayed.`
  host, and they keep running.
- **Rows, rejects, and drift.** Rows go to the connection's flush thread (`Flusher`), which
  publishes them. A rejected row fails the streams that would have received it, its raw wire
  symbol mapped to the canonical one (`_datasets/wire_symbols.py`); a row with no symbol fails only
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
  leases, and waits a bounded time for its threads; a connection the real-time verdict is
  moving closes itself once it has handed its streams over. While a connection waits for a slot
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
import logging
import weakref
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Final, cast

from deephaven_massive._datasets.symbols import SymbolTable
from deephaven_massive._enums import AssetClass, ChannelFamily
from deephaven_massive._errors import MassiveError
from deephaven_massive._ingest.subscriptions import is_wildcard, stream_topics, wildcard
from deephaven_massive._ingest.ws_connection import (
    OPTIONS_CAP,
    ClientFactory,
    Connection,
    ConnectionGroup,
    ConnectionStream,
)
from deephaven_massive._tables.execution_context import holds_update_graph_lock
from deephaven_massive._tables.stream import ChannelPlan, open_stream_tables
from deephaven_massive._tables.watchlist import read_symbols
from deephaven_massive._types import _All

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._ingest.futures_calendar import FuturesCalendar, RowFetch
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._tables.transports import HostDecision, StreamHandle

__all__ = [
    "JOIN_TIMEOUT",
    "WsConnections",
    "WsTransport",
]

logger = logging.getLogger(__name__)

JOIN_TIMEOUT: Final = 5.0
"""Seconds `m.close()` waits for each connection's threads before it logs a WARNING."""


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


# The connection groups of the process, by sharing key (`ConnectionGroup.attach`). Its lock is
# taken before a group's lock, never under it.


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
        self._exec_ctx = core.exec_ctx  # for the closing thread's lock check
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
        self._lookups: Final = ThreadPoolExecutor(1, f"{core.thread_prefix} crypto pairs")
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
            stream = ConnectionStream(stream_id, channel, plan, opened.sink, wanted, topics, static)
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

    def _attach(self, stream: ConnectionStream) -> None:
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
        A connection the server's real-time verdict is moving, between the move and its
        hand-over, is neither closed nor joined here: it is out of the registry and its leases
        are on its successor, and its hand-over closes it (`Connection.hand_over`).
        Last, the session leaves the group.

        A closing thread that holds the update graph's lock (a console command) joins nothing,
        neither the connections nor the group's futures schedule worker: no live table updates
        while it waits, and against an unresponsive server each join could last its full
        bound. Those threads are daemons that never need the lock, so they end on their own;
        each closing connection's last row, `CLOSED`, is written by this thread instead
        (`Connection.show_closed`), since the client's own would come after the session's
        diagnostics are released.
        """
        wait = not holds_update_graph_lock(self._exec_ctx)  # a Java call: before the lock
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
            if not wait:  # the daemons end on their own; their last rows would come too late
                connection.show_closed(self)
            elif not connection.join(JOIN_TIMEOUT):
                logger.warning(
                    "%s: the connection to %s has not ended %.0f s after close()",
                    self._label,
                    connection.url,
                    JOIN_TIMEOUT,
                )
        self.group.detach(self, wait=wait)

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
