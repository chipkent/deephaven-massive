"""The WebSocket transport: a session's registry of streams on shared connections.

`WsConnections` is the session's `StreamTransport`, one per session. A
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
  `EntitlementError`; nothing tears down the connection, and no connection moves to another
  host. A refusal that names no topic applies to the whole step in flight.
- **Rows and drift.** Rows go to the connection's flush thread (`Flusher`), which publishes
  them. A row the library cannot read whole is withheld, counted, and reported, and fails only
  a stream whose policy is `RAISE` for its cause. A connection logs one WARNING per outage,
  and one when it goes silent during its market's trading hours (`NO_FRAMES_WARNING`), each
  with its recovery at INFO. Drift is recorded on the connection's `SchemaDrift` here and
  handed to the flush thread for the streams' rows.
- **The options cap.** An `/options` connection carries at most `OPTIONS_CAP` explicit topics.
  A static symbol list that would cross it raises `ValueError` when the verb is called; a
  reactive table's excess topics are deferred, in sorted order, and admitted as room frees.
- **Watchlist changes.** A change of a reactive symbol table reaches the stream's connection
  directly, except for crypto, whose changes wait for a lookup first. Those run one at a time,
  in order, on the session's `symbol changes` worker, so a removal never overtakes an earlier
  addition still waiting. A futures contract subscribes at once: its product and schedule are
  asked for (`FuturesCalendar.watch`), and the flush holds its rows until they are dated. Any
  exception of a change is logged with its trace and fails the stream.
- **Crypto pairs.** A topic needs a pair's two currencies, which only Massive's crypto ticker
  list gives. Every pair, whatever its source, is split on the `symbol changes` worker before
  it is subscribed: a crypto stream of a symbol list opens with no topic and submits its pairs
  as its first change, as a symbol table's watch does. A pair the list gives no split for is
  one Massive has no data for, under `ErrorPolicy.unavailable_symbols`: skipped, logged once,
  and listed in the stream's `Error` under `LOG` and `IGNORE`, or the stream's failure under
  `RAISE`. A list that cannot be read is an outage of the pairs it leaves unsplit: they wait,
  the stream's `Error` says why, the worker tries again after the reconnect backoff, and the
  stream fails once they have waited `MassiveConfig.max_outage`. A failure no retry can fix (a
  rejected key, a plan without reference data) fails the stream at once.
- **Sharing.** Sessions in one process with the same API key and equal connection settings
  (`CONNECTION_SETTINGS`, and the same Deephaven update graph) share one `ConnectionGroup`, so
  their streams ride the same connections: individual plans allow one connection per cluster,
  and two sessions with connections of their own would knock each other off. A connection is
  leased per stream, and remembers which sessions use it, so each session's `m.connections()`
  shows it, each stream reports to its own session, and a session's close takes only its own
  streams away. The options cap, refusals, and fatal failures are the
  connection's, so they act on every session's streams on it.
- **Lifetime.** A connection with no stream attached for `IDLE_GRACE` seconds closes itself
  and leaves the registry. A fatal failure (a rejected key, a handshake error retrying cannot
  fix) fails every stream on it, a stream opening at that moment included. `m.close()` removes
  the session's streams from their connections, closes every connection no other session
  leases, and waits a bounded time for its threads. While a connection is down
  (`RECONNECTING` or `WAITING_SLOT`), each of its streams says so in its `Error`, and an outage
  longer than `MassiveConfig.max_outage` fails them (`Connection`).

**Threads and locks.** Each connection's subscription state is confined to its client's loop
thread; other threads reach it only through `loop.call_soon_threadsafe` with plain data
(`Connection.post`). The registries that map URLs to connections and streams to connections,
each connection's leases, sessions, failure, and options-cap bookkeeping, and each session's
registry state here are guarded by the group's lock, a leaf lock like the sessions'
`registry_lock` (the two are never held together): no Deephaven call, network call, or wait
happens under it (creating a client loads TLS settings, so it happens outside). The loop
thread makes no Java call: every failure goes to a flush thread, or,
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
import threading
import time
import weakref
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, assert_never

from deephaven_massive._datasets.symbols import AllSymbols, SymbolTable
from deephaven_massive._enums import AssetClass, ChannelFamily, OnError, Recency
from deephaven_massive._error_policy import policy_hint
from deephaven_massive._errors import (
    LIBRARY_BUG,
    NEW_SESSION,
    BrokenGuaranteeError,
    ErrorSource,
    MassiveError,
    SessionClosedError,
    UnavailableSymbolError,
    copy_error,
    failure_text,
    library_bug,
    required,
    unmatched_form,
)
from deephaven_massive._ingest.host_decision import real_time_note
from deephaven_massive._ingest.subscriptions import is_wildcard, stream_topics, wildcard
from deephaven_massive._ingest.ws_connection import (
    OPTIONS_CAP,
    ClientFactory,
    Connection,
    ConnectionGroup,
    ConnectionStream,
)
from deephaven_massive._log_limits import BoundedLog, bounded_list, mark_failure_logged
from deephaven_massive._on_error import effect_of
from deephaven_massive._tables.diagnostics import NotePart, StreamUpdate
from deephaven_massive._tables.execution_context import holds_update_graph_lock
from deephaven_massive._tables.liveness import release_scope
from deephaven_massive._tables.stream import ChannelPlan, open_stream_tables
from deephaven_massive._tables.transports import Unsplit

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.request import Request
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._error_policy import ResolvedPolicy
    from deephaven_massive._ingest.futures_calendar import FuturesCalendar, RowFetch
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._tables.transports import HostDecision, StreamHandle

__all__ = [
    "JOIN_TIMEOUT",
    "WsConnections",
]

logger = logging.getLogger(__name__)

JOIN_TIMEOUT: Final = 5.0
"""Seconds `m.close()` waits for each connection's threads before it logs a WARNING."""

_SKIPPED_PAIR_LIMIT: Final = 100
"""The most crypto pairs with no data logged per process under each of `LOG` and `IGNORE`."""
_logged_pairs: Final = BoundedLog(logger, limit=_SKIPPED_PAIR_LIMIT, level=logging.WARNING)
"""The pairs skipped under `unavailable_symbols=LOG`, logged at WARNING."""
_ignored_pairs: Final = BoundedLog(logger, limit=_SKIPPED_PAIR_LIMIT, level=logging.DEBUG)
"""The pairs skipped under `unavailable_symbols=IGNORE`: logged at DEBUG only."""


@dataclass(slots=True)
class _PairState:
    """A crypto stream's pairs that are not subscribed, and why; guarded by the group's lock.

    Attributes:
        policy: The stream's resolved policy (`Request.on_error`): what a pair Massive has no
            data for, and a listed pair whose currencies do not join, do.
        skipped: Each pair the list gives no split for, with why, until the stream no longer
            holds it.
        waiting: The pairs waiting for Massive's crypto ticker list to be read.
        since: When the pairs began to wait (`time.monotonic`), or None when none waits.
        tries: The failed reads of the list since then.
        failure: The last failed read of the list, while pairs wait.
        retry_due: Whether a retry is scheduled, so a change meanwhile schedules no second one.
    """

    policy: ResolvedPolicy
    skipped: dict[str, str] = field(default_factory=dict)
    waiting: set[str] = field(default_factory=set)
    since: float | None = None
    tries: int = 0
    failure: MassiveError | None = None
    retry_due: bool = False

    def note(self) -> str | None:
        """Say which pairs the stream does not subscribe and why, for its `Error`.

        Returns:
            The text, or None when every pair is subscribed.
        """
        parts = []
        if self.skipped:
            parts.append(
                "skipped "
                + bounded_list([f"{pair} ({why})" for pair, why in sorted(self.skipped.items())])
            )
        if self.waiting:
            parts.append(
                f"crypto pairs {bounded_list(sorted(self.waiting))} wait for Massive's crypto "
                f"ticker list, which could not be read ({self.failure}); retrying"
            )
        return "; ".join(parts) or None


def _weakly(owner_ref: Callable[[], WsConnections | None], method: str, *args: object) -> None:
    """Call a session's `WsConnections` method through a weak reference, if it still exists.

    The blink table's shutdown callback and a watchlist's change callback are held from Java
    until the JVM collects the publisher or the listener. Holding the session's connections
    strongly there would keep a finished session alive until the next JVM collection. While a
    stream runs, its session (and so its `WsConnections`) is alive anyway. `WsConnections`
    holds its core only weakly too, so this is defense in depth: it keeps the transport, not
    only the session, from waiting for the JVM.

    Args:
        owner_ref: Returns the session's `WsConnections`, or None once it is freed: a weak
            reference.
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

    Raises:
        BrokenGuaranteeError: For a form of symbols with no rule here.
    """
    match symbols:
        case tuple():
            return frozenset(symbols), stream_topics(asset_class, channel, symbols, pairs=pairs)
        case AllSymbols():
            return None, frozenset({wildcard(channel)})
        case SymbolTable():
            return frozenset(), frozenset()
        case _:
            raise BrokenGuaranteeError(unmatched_form("a stream's starting topics", symbols))


def _pairs_to_split(asset_class: AssetClass, symbols: NormalizedSymbols) -> frozenset[str]:
    """Return the crypto pairs a new stream submits as its first change (`_change_pairs`).

    A stream of a crypto symbol list opens with no topic: its pairs are split on the session's
    `symbol changes` worker, as a symbol table's rows are, which its watch submits.

    Args:
        asset_class: The stream's class.
        symbols: Its normalized symbols.

    Returns:
        The pairs of a crypto symbol list; none for another class, `dm.ALL` (a wildcard needs
        no split), or a symbol table.

    Raises:
        BrokenGuaranteeError: For a form of symbols with no rule here.
    """
    match asset_class:
        case AssetClass.CRYPTO:
            pass
        case (
            AssetClass.STOCKS
            | AssetClass.OPTIONS
            | AssetClass.INDICES
            | AssetClass.FOREX
            | AssetClass.FUTURES
        ):
            return frozenset()  # a topic takes the symbol itself, or a forex pair's fixed halves
        case _:
            assert_never(asset_class)  # a new class decides here whether its topics need a split
    match symbols:
        case tuple():
            return frozenset(symbols)
        case AllSymbols() | SymbolTable():
            return frozenset()
        case _:
            raise BrokenGuaranteeError(unmatched_form("a stream's crypto pairs to split", symbols))


# The connection groups of the process, by sharing key (`ConnectionGroup.attach`). Its lock is
# taken before a group's lock, never under it.


class WsConnections:
    """One session's side of its `ConnectionGroup`: its streams, its rows, and its close.

    Created once per session by `SessionClients.streams()`, which registers `close` with the
    session and returns it as the session's `StreamTransport`. It joins the group of the session's
    key and settings at once (`ConnectionGroup.attach`) and leaves it in `close`. It lives as
    long as the session's clients, so it holds the session core only weakly: the core holds it
    (through its `SessionClients`), and its `close` is a session closer, which must not keep the
    core alive. `open_stream` runs on the caller thread, inside a verb of the core, which
    keeps the core alive meanwhile; the blink
    tables' shutdown callbacks and the symbol watches call in from any thread. Its state (the
    session's streams, the connection whose row it shows for each class and family, the
    closed flag, and the channel plans) is guarded by the group's lock.

    Its futures connections use their group's schedule calendar (`FuturesCalendar`,
    `ConnectionGroup.futures_calendar`), which dates streamed futures rows by their trading
    session; a futures stream's explicit symbols, and those a reactive table adds, are looked up
    as soon as the stream opens.

    Example:
        ::

            connections = WsConnections(core, resolve_host=rest.resolve_host)
            handle = connections.open_stream(spec, request, "T", stream_id=7)
    """

    def __init__(
        self,
        core: SessionCore,
        *,
        resolve_host: Callable[[AssetClass, ChannelFamily], HostDecision],
        fetch_rows: RowFetch,
        crypto_pair: Callable[[str, ResolvedPolicy], tuple[str, str] | Unsplit],
        client_factory: ClientFactory | None = None,
    ) -> None:
        """Join the session's connection group; no connection is opened.

        Args:
            core: The session's core, held weakly.
            resolve_host: Decides the host of a (class, family), once per session; the REST
                transport's `resolve_host`, which may make one REST request.
            fetch_rows: Returns every row of a REST path for a query; the REST transport's
                `fetch_rows`, through which the group's futures schedule calendar reads while
                the session is open.
            crypto_pair: Returns the base and quote of a crypto ticker from Massive's crypto
                ticker list, which the session reads once, or why Massive has no data for it;
                the REST transport's `crypto_pair`, which may make one REST request.
            client_factory: Creates each connection's client; by default a `WsClient` with the
                session's key and backoffs. Sessions share connections only with the same
                factory.
        """
        self._core = weakref.ref(core)
        self._label = core.label
        self._exec_ctx = core.exec_ctx  # for the closing thread's lock check
        self._resolve = resolve_host
        self.fetch_rows = fetch_rows
        self._crypto_pair = crypto_pair
        config = core.config
        # A list that cannot be read is retried on the reconnect backoff, up to max_outage.
        self._backoff: Final = (config.reconnect_backoff, config.max_reconnect_backoff)
        self._max_outage: Final = config.max_outage
        # What this session's streams do when another connection takes theirs; read by its
        # connections under the group's lock, so sessions differing in it still share them.
        self.connection_taken: Final = core.config.connection_taken
        # The futures schedule calendar reads the paths of the futures reference datasets.
        by_name = core.catalog.by_name
        self._calendar_paths: Final = (
            required(by_name["futures.snapshots"].fetch_path, "futures.snapshots's fetch path"),
            required(by_name["futures.contracts"].fetch_path, "futures.contracts's fetch path"),
            required(by_name["futures.schedules"].fetch_path, "futures.schedules's fetch path"),
        )
        self.group: Final = ConnectionGroup.attach(
            self, config=core.config, exec_ctx=core.exec_ctx, client_factory=client_factory
        )
        self._lock: Final = self.group.lock
        # Guarded by the group's lock.
        self._mine: set[int] = set()
        self._row_owners: dict[tuple[AssetClass, ChannelFamily], Connection] = {}
        self._plans: dict[tuple[DatasetSpec, str], ChannelPlan] = {}
        # One worker, started on the first futures or crypto watchlist change: a contract's
        # schedule and a pair's split are waited for off the update-graph thread, and no change
        # of a stream overtakes another.
        self._changes: Final = ThreadPoolExecutor(1, f"{core.thread_prefix} symbol changes")
        # Set by close(), so a worker waiting for schedules stops waiting.
        self._closing: Final = threading.Event()
        # The crypto pairs each stream does not subscribe, by stream; guarded by the group's lock.
        self._pair_states: dict[int, _PairState] = {}
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
        self, spec: DatasetSpec, request: Request, channel: str, *, stream_id: int
    ) -> StreamHandle:
        """Open a stream: decide its host, lease its connection, create its tables, subscribe.

        Runs on the caller thread inside the session's execution context, with no lock held
        while it resolves the host (one REST request at the class's first stream) or creates
        tables. The stream's topics reach the connection's loop thread after its tables exist,
        and a reactive symbol table's watch starts after that, so no change is lost. If
        anything raises, even `KeyboardInterrupt`, the stream's scope (when created) is
        released and its lease and topics returned.

        Args:
            spec: A WebSocket dataset.
            request: The normalized arguments; its symbols are the stream's.
            channel: The channel.
            stream_id: The id the session minted.

        Returns:
            The stream's handle.

        Static options symbols that would put more than `OPTIONS_CAP` explicit topics on the
        `/options` connection (counting every session's topics on it) raise the `ValueError`
        of `_check_cap`, and a closed session the `MassiveError` of `_check_open`; host
        resolution raises the `AuthenticationError`, `EntitlementError`, or `MassiveError` of
        `resolve_host`. A crypto stream of a symbol list opens with no topic, and its
        pairs go to the `symbol changes` worker once it is attached (`_change_pairs`), as a
        symbol table's rows do, so nothing about a pair is decided at the call.
        """
        # The calling verb runs on the core, which is alive while the verb runs.
        core = required(self._core(), "the session")
        asset_class = spec.single_class
        plan = self._plan(core, spec, channel)
        pairs = _pairs_to_split(asset_class, request.symbol_key())
        symbols = () if pairs else request.symbol_key()  # subscribed once split
        wanted, topics = _wanted(asset_class, channel, symbols, pairs=self.pairs())
        decision = self._resolve(asset_class, spec.channel_family)
        if spec.real_time_only and decision.recency is not Recency.REALTIME:
            # Massive's answer decides: the library refuses nothing it might serve.
            logger.info(
                "%s stream %d: Massive documents it only on the real-time feed; subscribing on "
                "%s (%s), so Massive's answer decides whether it streams",
                spec.name,
                stream_id,
                decision.host,
                decision.evidence,
            )
        connection, created = self._lease(core, spec, decision, stream_id=stream_id, topics=topics)
        try:
            # Started first, so a failure or interrupt below leaves a running connection
            # that `_abandon` retires once idle: a registered connection that never started
            # would be reused by later streams and never connect. It subscribes nothing until
            # `_attach`.
            if created:
                connection.start()
            if asset_class is AssetClass.FUTURES and wanted:
                # The lookups start now; the flush holds the rows until they are dated.
                self.futures_calendar().watch(wanted)
            opened = open_stream_tables(
                plan,
                core.dataset_schema(spec),
                symbols,
                core=core,
                policy=request.on_error,
                stream_id=stream_id,
                host=decision.host,
                evidence=decision.evidence,
                recency=decision.recency,
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
            stream = ConnectionStream(
                stream_id,
                channel,
                plan,
                opened.sink,
                wanted,
                topics,
                static,
                refusal_note=real_time_note(spec) if spec.real_time_only else "",
            )
            if asset_class is AssetClass.CRYPTO:  # its pairs are split by the stream's policy
                with self._lock:
                    self._pair_states[stream_id] = _PairState(request.on_error)
            self._attach(stream)
            if pairs:
                self._submit_change(self._change_pairs, stream_id, added=pairs, removed=frozenset())
            if opened.watch is not None:
                opened.watch.start()
        except BaseException:
            release_scope(core.exec_ctx, opened.handle.scope)
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
        """Find or create the group's connection of a stream and lease it for the stream.

        A new connection is created outside the lock (its client loads TLS settings) and
        registered under it. The registry is read again under the lock, and as often as it
        takes: a connection found before may have been retired meanwhile, and one registered by
        another thread (of this session or another) meanwhile wins, the new one then closing
        unused. The session becomes one of the connection's users and
        shows its row.

        Args:
            core: The session's core, whose execution context a new connection's flush thread
                enters.
            spec: The stream's dataset.
            decision: Its host decision.
            stream_id: The stream.
            topics: Its static topics, for the options cap.

        Returns:
            `(connection, created)`: created is True when the caller must start it.

        Static topics that would cross the options cap raise the `ValueError` of `_check_cap`,
        and a closed session the `MassiveError` of `_check_open`.
        """
        group = self.group
        asset_class = spec.single_class
        url = f"wss://{decision.host}/{asset_class.value}"
        candidate: Connection | None = None
        try:
            while True:
                with self._lock:
                    self._check_open()
                    connection = group.connections.get(url)
                    self._check_cap(spec, connection, topics)
                    created = False
                    if connection is None and candidate is not None:
                        connection = group.connections[url] = candidate
                        candidate = None
                        created = True
                    if connection is not None:
                        connection.families.add(spec.channel_family)
                        self._row_owners[asset_class, spec.channel_family] = connection
                        if asset_class is AssetClass.OPTIONS:
                            connection.cap_pending[stream_id] = topics
                        connection.take(self)
                        group.streams[stream_id] = connection
                        group.owners[stream_id] = self
                        self._mine.add(stream_id)
                        break
                futures = asset_class is AssetClass.FUTURES
                candidate = Connection(
                    group,
                    exec_ctx=core.exec_ctx,
                    url=url,
                    asset_class=asset_class,
                    family=spec.channel_family,
                    decision=decision,
                    sessions=self.futures_calendar() if futures else None,
                )
        finally:
            if candidate is not None:  # lost the race, or refused
                candidate.close()
        return connection, created

    def _check_open(self) -> None:
        """Refuse a stream once the session's side is closed; the group's lock is held.

        Raises:
            SessionClosedError: If `close()` has run.
        """
        if self._closed:
            raise SessionClosedError(f"{self._label} is closed; {NEW_SESSION}")

    @staticmethod
    def _check_cap(
        spec: DatasetSpec, connection: Connection | None, topics: frozenset[str]
    ) -> None:
        """Refuse static options topics that would cross the cap; the group's lock is held.

        In use are the topics the connection admitted and those of streams still opening, of
        every session on the connection. The cap counts topics, one per contract and channel,
        so `T.X` and `Q.X` are two: Massive documents "1,000 option contracts", but whether its
        server counts contracts or topics is not verified, and the stricter count never
        crosses it.

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
                in_use.update(t for t in pending if not is_wildcard(t))  # a wildcard is no contract
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

        The stream goes to the connection that holds its lease. Routing and the check for the
        connection's failure happen in one section of the group's lock, the one the connection's
        `_failed` takes, so a connection that failed meanwhile never leaves the stream running:
        the stream fails here, on the caller thread. So does a stream of a
        `connection_taken=FAIL` session whose connection was taken while it opened
        (`Connection.pending_failures`).
        A stream whose lease is gone (the session closed) is left as it is.

        Args:
            stream: The stream, with its topics.
        """
        with self._lock:
            connection = self.group.streams.get(stream.stream_id)
            failure = None if connection is None else connection.failure
            if failure is None and connection is not None:
                failure = connection.pending_failures.pop(stream.stream_id, None)
            if connection is not None and failure is None:
                connection.flusher.routes.add(stream.channel, stream.sink)
                connection.post(connection.add_stream, stream)
        if failure is not None:
            stream.sink.fail(failure)

    def _abandon(self, connection: Connection, stream_id: int) -> None:
        """Return the lease of a stream whose open failed, and let its connection go idle.

        Idempotent: the shutdown callback may already have returned the lease.

        Args:
            connection: The connection the stream leased.
            stream_id: The stream.
        """
        with self._lock:
            self._forget(stream_id)
        connection.flusher.routes.remove(stream_id)
        connection.post(connection.check_idle)

    def _forget(self, stream_id: int) -> Connection | None:
        """Drop a stream's lease from the group; the group's lock is held. Idempotent.

        The connection that held the lease also stops counting the stream's topics against the
        options cap while it was still opening.

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
        self._pair_states.pop(stream_id, None)
        current.cap_pending.pop(stream_id, None)
        current.pending_failures.pop(stream_id, None)
        current.give(self)
        return current

    # --- callbacks from any thread -------------------------------------------------------------

    def _shut_down(self, stream_id: int) -> None:
        """The blink table's `on_shutdown_callback`: unregister the stream, release its topics.

        Runs on whichever thread dropped the table's last reference (the reaper, an
        update-graph thread, a gRPC thread, or Deephaven's liveness-cleanup thread). It does
        two things only: the registry updates (`SharedStreams.shut_down`, then the
        stream's lease, under the group's lock) and one `call_soon_threadsafe` to the
        connection's loop, which releases the topics there. A closed loop (the connection ended)
        is logged at INFO. It never raises. The session is alive: the stream's record keeps it
        in the live set until `SharedStreams.shut_down` has run, and while the stream opens the
        opening caller holds it.

        Args:
            stream_id: The stream.
        """
        try:
            required(self._core(), "the session").streams.shut_down(stream_id)
            with self._lock:
                connection = self._forget(stream_id)
            if connection is not None:
                # A loop that has ended holds no stream any more (`Connection._on_state`).
                connection.post(connection.release_stream, stream_id)
        except Exception:
            logger.exception(
                "%s: the shutdown of stream %d failed. %s", self._label, stream_id, LIBRARY_BUG
            )

    def _symbols_changed(
        self, stream_id: int, added: frozenset[str], removed: frozenset[str]
    ) -> None:
        """A reactive symbol table changed: post the change to the stream's connection.

        Runs on an update-graph thread (or the caller thread for the initial replay), with the
        update-graph lock held: it takes the group's lock only to find the connection, and
        never blocks or raises. A crypto change, which waits for a lookup first, goes to the
        `symbol changes` worker (`_run_change`), which runs a session's changes one at a time,
        in order; every other change goes to the connection's loop at once. The added futures
        contracts are handed to the schedule calendar first (`FuturesCalendar.watch`, which
        only queues their lookups); the flush holds their rows until they are dated. A change
        that finds the session closing, or the worker shut down by `close()`, is dropped and
        logged at DEBUG, since the stream goes with the session.

        Args:
            stream_id: The stream.
            added: Canonical symbols the table now holds.
            removed: Canonical symbols it no longer holds.
        """
        with self._lock:
            connection = self.group.streams.get(stream_id)
        if connection is None:
            return
        asset_class = connection.asset_class
        if asset_class is AssetClass.CRYPTO:
            self._submit_change(self._change_pairs, stream_id, added=added, removed=removed)
            return
        if asset_class is AssetClass.FUTURES and added:
            try:
                calendar = self.futures_calendar()
            except SessionClosedError as error:
                self._drop_change(stream_id, added, removed, error=error)
                return
            calendar.watch(added)
        connection.post(connection.change_symbols, stream_id, added, removed)

    def _submit_change(
        self,
        change: Callable[[int, frozenset[str], frozenset[str]], None],
        stream_id: int,
        *,
        added: frozenset[str],
        removed: frozenset[str],
    ) -> None:
        """Queue a change on the `symbol changes` worker (`_run_change`). Any thread; never blocks.

        A change that finds the worker shut down by `close()` is dropped and logged at DEBUG.

        Args:
            change: `_change_pairs`, the only change that runs on the worker.
            stream_id: The stream.
            added: Canonical symbols to subscribe.
            removed: Canonical symbols to unsubscribe.
        """
        try:
            self._changes.submit(
                self._run_change, change, stream_id=stream_id, added=added, removed=removed
            )
        except RuntimeError as error:
            self._drop_change(stream_id, added, removed, error=error)

    @staticmethod
    def _drop_change(
        stream_id: int, added: frozenset[str], removed: frozenset[str], *, error: Exception
    ) -> None:
        """Log at DEBUG a watchlist change dropped because the session is closing.

        Args:
            stream_id: The stream.
            added: Canonical symbols the table now holds.
            removed: Canonical symbols it no longer holds.
            error: What showed the session closing.
        """
        logger.debug(
            "stream %d: a watchlist change (%d added, %d removed) was dropped; the session is "
            "closing (%s)",
            stream_id,
            len(added),
            len(removed),
            error,
        )

    def _run_change(
        self,
        change: Callable[[int, frozenset[str], frozenset[str]], None],
        *,
        stream_id: int,
        added: frozenset[str],
        removed: frozenset[str],
    ) -> None:
        """Run one watchlist change on the `symbol changes` worker, failing its stream on a defect.

        Nothing reads the worker's futures, so an exception that escaped here would vanish.
        Any exception of the change is a defect of the library: it is logged at ERROR with its
        trace, and it fails the stream (`Flusher.fail`), if the stream still exists. A
        `MassiveError` that already names itself a defect (`ErrorSource.LIBRARY`, such as
        one from the crypto pair lookup) fails the stream with its own message, ended with
        `LIBRARY_BUG` unless it holds it already; any other exception is wrapped once in a
        `MassiveError` that names the change and asks for a bug report. Either way the message
        asks for the report exactly once.

        Args:
            change: `_change_pairs`, the only change that runs on the worker.
            stream_id: The stream.
            added: Canonical symbols the table now holds.
            removed: Canonical symbols it no longer holds.
        """
        try:
            change(stream_id, added, removed)
        except Exception as error:  # a defect: logged with its trace, and it fails the stream
            if isinstance(error, MassiveError) and error.source is ErrorSource.LIBRARY:
                # Its own message, ended with how to report it unless it already says so.
                failure = MassiveError(failure_text(error), source=ErrorSource.LIBRARY)
            else:
                failure = MassiveError(
                    library_bug(
                        f"the watchlist change of stream {stream_id} (added "
                        f"{bounded_list(sorted(added))}, removed "
                        f"{bounded_list(sorted(removed))}) failed: {error!r}"
                    ),
                    source=ErrorSource.LIBRARY,
                )
            # Logged here, with the trace; the stream's failure then logs it at DEBUG only.
            mark_failure_logged(failure)
            logger.exception("%s: %s", self._label, failure)
            with self._lock:
                connection = self.group.streams.get(stream_id)
            if connection is not None:
                connection.flusher.fail([stream_id], failure)

    def _post_change(self, stream_id: int, added: frozenset[str], removed: frozenset[str]) -> None:
        """Post a change to the stream's connection's loop, if the stream still exists.

        Args:
            stream_id: The stream.
            added: Canonical symbols to subscribe.
            removed: Canonical symbols to unsubscribe.
        """
        with self._lock:
            connection = self.group.streams.get(stream_id)
        if connection is None:
            logger.debug(
                "stream %d: ended before its watchlist change (%d added, %d removed) was posted",
                stream_id,
                len(added),
                len(removed),
            )
            return
        connection.post(connection.change_symbols, stream_id, added, removed)

    def _change_pairs(self, stream_id: int, added: frozenset[str], removed: frozenset[str]) -> None:
        """Split a crypto stream's added pairs and those still waiting, then post the change.

        Runs on the `symbol changes` worker (`_run_change`, which turns any unexpected
        exception into the stream's failure), so a retry and a removal stay in order. Its only
        Deephaven call is the check, when a lookup starts or joins one already under way, of
        whether its thread holds the update graph's lock, which sets how long the lookup waits
        out rate-limit pauses (`rate_limits.read_patience`; the worker never holds it, so 10
        minutes). Each pair (`_split`) is subscribed, or skipped, or waits for the list
        (`_wait`). A skipped pair Massive has no data for follows the stream's
        `unavailable_symbols` (`_no_data`); one whose currencies the library cannot read was
        reported under its `unreadable_symbols` by the lookup.

        Args:
            stream_id: The stream.
            added: Canonical symbols the table now holds; for a retry, none.
            removed: Canonical symbols it no longer holds.
        """
        with self._lock:
            state = self._pair_states.get(stream_id)
            if state is None:  # the stream has ended
                return
            for symbol in removed:
                state.skipped.pop(symbol, None)
                state.waiting.discard(symbol)
            asked = sorted(added | state.waiting)
        reader = f"crypto stream {stream_id}"
        split, unsplit, waiting, failure = self._split(asked, state.policy)
        no_data = {pair: why.why for pair, why in unsplit.items() if why.no_data}
        unavailable = state.policy.unavailable_symbols
        if no_data and not self._no_data(stream_id, no_data, reader=reader, setting=unavailable):
            return
        if failure is not None and not failure.is_retryable:
            self._fail(stream_id, copy_error(failure, f"{reader}: {failure}"))
            return
        skipped = {pair: why.why for pair, why in unsplit.items()}
        delay = self._wait(stream_id, state, skipped=skipped, waiting=waiting, failure=failure)
        if delay is None:
            return
        self._show_pairs(stream_id, state)
        if split or removed:
            self._post_change(stream_id, frozenset(split), removed)
        if delay > 0:
            self._schedule_retry(stream_id, delay)

    def _split(
        self, symbols: Iterable[str], policy: ResolvedPolicy
    ) -> tuple[set[str], dict[str, Unsplit], list[str], MassiveError | None]:
        """Split crypto pairs by Massive's crypto ticker list, reading none after a failed read.

        A split is kept for the group (`pairs`), so each pair is looked up once, and a pair
        split before is split even while the list cannot be read.

        Args:
            symbols: Canonical crypto tickers, sorted.
            policy: The stream's resolved policy, for the lookup (`crypto_pair`).

        Returns:
            The pairs split, those the list gives no split for with why (`Unsplit`), those not
            split before and left unread by a failed read of the list, and that failure (None
            when the list was read).

        Raises:
            MassiveError: The lookup's own error, when its source is `ErrorSource.LIBRARY`: a
                defect, which fails the stream.
            BrokenGuaranteeError: For an outcome of a lookup with no rule here.
        """
        split: set[str] = set()
        unsplit: dict[str, Unsplit] = {}
        waiting: list[str] = []
        failure: MassiveError | None = None
        for symbol in symbols:
            with self._lock:
                known = symbol in self.group.pairs
            if known:
                split.add(symbol)
                continue
            if failure is not None:  # the list just failed to read: wait with the failed pair
                waiting.append(symbol)
                continue
            try:
                found = self._crypto_pair(symbol, policy)
            except MassiveError as error:
                if error.source is ErrorSource.LIBRARY:
                    raise  # a defect is never retried or shown as a waiting pair
                failure = error
                waiting.append(symbol)
                continue
            match found:
                case Unsplit():
                    unsplit[symbol] = found
                case tuple():
                    with self._lock:
                        self.group.pairs[symbol] = found
                    split.add(symbol)
                case _:
                    raise BrokenGuaranteeError(unmatched_form("a crypto pair's lookup", found))
        return split, unsplit, waiting, failure

    def _no_data(
        self, stream_id: int, no_data: Mapping[str, str], *, reader: str, setting: OnError
    ) -> bool:
        """Apply the stream's `unavailable_symbols` to the pairs Massive has no data for.

        Under `LOG` each pair is logged once per process at WARNING, under `IGNORE` at DEBUG,
        a bounded number of pairs; under `RAISE` the stream fails.

        Args:
            stream_id: The stream.
            no_data: The pairs, with why.
            reader: Who read them, for the log.
            setting: The stream's `unavailable_symbols`.

        Returns:
            False when the stream failed.
        """
        described = bounded_list([f"{pair} ({why})" for pair, why in sorted(no_data.items())])
        effect = effect_of(setting)
        if effect.fails:
            self._fail(
                stream_id,
                UnavailableSymbolError(
                    f"{reader}: Massive has no data for {described}. Remove the pair; to skip it "
                    f"and stream the others, {policy_hint('unavailable_symbols', OnError.LOG)}",
                    source=ErrorSource.MASSIVE,
                ),
            )
            return False
        pairs = _logged_pairs if effect.level > logging.DEBUG else _ignored_pairs
        for pair, why in sorted(no_data.items()):
            pairs.log(pair, "%s: Massive has no data for %s (%s); it is skipped", reader, pair, why)
        return True

    def _wait(
        self,
        stream_id: int,
        state: _PairState,
        *,
        skipped: Mapping[str, str],
        waiting: Iterable[str],
        failure: MassiveError | None,
    ) -> float | None:
        """Record a change's skipped pairs and those waiting, and when to try again.

        Pairs wait while Massive's crypto ticker list cannot be read: an outage of those
        pairs, timed from the first failed read. The next try comes after the reconnect
        backoff, doubled with each failed try up to `max_reconnect_backoff`, and no later than
        `MassiveConfig.max_outage` after the first; a list still unread then fails the stream.

        Args:
            stream_id: The stream.
            state: The stream's pairs not subscribed.
            skipped: The pairs the list gives no split for, with why.
            waiting: The pairs left unsplit by `failure`.
            failure: The failed read of the list, or None.

        Returns:
            Seconds until the next try; 0 for none; None when the stream ended or failed.
        """
        now = time.monotonic()
        with self._lock:
            if stream_id not in self.group.streams:
                return None
            state.skipped.update(skipped)
            state.waiting = set(waiting)
            if failure is None or not state.waiting:
                state.since, state.tries, state.failure = None, 0, None
                return 0.0
            began = state.since is None
            since = now if state.since is None else state.since
            state.since, state.failure = since, failure
            state.tries += 1
            left = since + self._max_outage - now
            first, most = self._backoff
            delay = min(first * 2 ** (state.tries - 1), most, max(left, 0.0))
            due = state.retry_due
            state.retry_due = True
            outage = self._outage_error(stream_id, state, now)
        if left <= 0:
            self._fail(stream_id, outage)
            return None
        if began:  # the outage's limit holds even while a try still waits on its lookup
            self._schedule(stream_id, left, "_pairs_expired", stream_id, since)
        logger.info(
            "crypto stream %d: %s; %s wait, and the list is read again in %.0f s",
            stream_id,
            failure,
            bounded_list(sorted(state.waiting)),
            delay,
        )
        return 0.0 if due else delay

    def _outage_error(self, stream_id: int, state: _PairState, now: float) -> MassiveError:
        """Return the failure of a stream whose pairs waited past `MassiveConfig.max_outage`.

        Called with the group's lock held.

        Args:
            stream_id: The stream.
            state: Its pairs not subscribed, some waiting since `state.since`.
            now: The time (`time.monotonic`).

        Returns:
            A copy of the last failed read of the list (`copy_error`), naming the wait.
        """
        failure = required(state.failure, f"stream {stream_id}'s failed read of the list")
        since = required(state.since, f"when stream {stream_id}'s pairs began to wait")
        return copy_error(
            failure,
            f"crypto stream {stream_id}: Massive's crypto ticker list could not be read for "
            f"{now - since:.0f} s, longer than MassiveConfig.max_outage "
            f"({self._max_outage:g} s), so {bounded_list(sorted(state.waiting))} cannot be "
            f"streamed: {failure}",
        )

    def _schedule(self, stream_id: int, delay: float, method: str, *args: object) -> None:
        """Run a method of this transport on the stream's connection's loop after `delay` s.

        The loop keeps the timer (`Connection.post_later`), which ends with the connection,
        and holds this transport weakly (`_weakly`).

        Args:
            stream_id: The stream.
            delay: Seconds to wait.
            method: `"_retry_pairs"` or `"_pairs_expired"`, which never block.
            *args: Its arguments.
        """
        with self._lock:
            connection = self.group.streams.get(stream_id)
        if connection is not None:
            connection.post_later(
                delay, functools.partial(_weakly, weakref.ref(self), method, *args)
            )

    def _schedule_retry(self, stream_id: int, delay: float) -> None:
        """Try a stream's waiting pairs again after `delay` seconds, on the worker.

        The timer only queues the retry (`_retry_pairs`).

        Args:
            stream_id: The stream.
            delay: Seconds to wait.
        """
        self._schedule(stream_id, delay, "_retry_pairs", stream_id)

    def _pairs_expired(self, stream_id: int, since: float) -> None:
        """Fail a stream whose pairs still wait `MassiveConfig.max_outage` after they began to.

        Runs on the connection's loop thread, so it fails the stream even while the worker
        still waits on a lookup. Pairs that subscribed since, or a new wait, change nothing.

        Args:
            stream_id: The stream.
            since: When the wait this timer was set for began (`_PairState.since`).
        """
        with self._lock:
            state = self._pair_states.get(stream_id)
            if state is None or state.since != since or not state.waiting:
                return
            outage = self._outage_error(stream_id, state, time.monotonic())
        self._fail(stream_id, outage)

    def _retry_pairs(self, stream_id: int) -> None:
        """Queue a retry of a stream's waiting pairs; runs on the connection's loop thread.

        Args:
            stream_id: The stream.
        """
        with self._lock:
            state = self._pair_states.get(stream_id)
            if state is None:
                return
            state.retry_due = False
        self._submit_change(self._change_pairs, stream_id, added=frozenset(), removed=frozenset())

    def _fail(self, stream_id: int, failure: MassiveError) -> None:
        """Fail a stream from the `symbol changes` worker, if it still exists.

        Args:
            stream_id: The stream.
            failure: Its error.
        """
        logger.debug("crypto stream %d: fails: %s", stream_id, failure)
        with self._lock:
            connection = self.group.streams.get(stream_id)
        if connection is not None:
            connection.flusher.fail([stream_id], failure)

    def _show_pairs(self, stream_id: int, state: _PairState) -> None:
        """Show a crypto stream's pairs that are not subscribed in its `Error`.

        The text is the stream's `NotePart.SKIPPED_PAIRS` warning (`_PairState.note`), cleared
        once every pair is subscribed. `SessionCore.update_streams` takes only the leaf
        `registry_lock`.

        Args:
            stream_id: The stream.
            state: Its pairs not subscribed.
        """
        with self._lock:
            text = state.note()
        core = self._core()
        if core is not None:
            core.update_streams({stream_id: StreamUpdate(notes={NotePart.SKIPPED_PAIRS: text})})

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
        snapshot, contracts, schedules = self._calendar_paths
        return self.group.futures_calendar(
            snapshot_path=snapshot, contracts_path=contracts, schedules_path=schedules
        )

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
        self._closing.set()
        self._changes.shutdown(wait=False, cancel_futures=True)
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
        mine = [(s, required(self._forget(s), f"stream {s}'s lease")) for s in sorted(self._mine)]
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
        return mine, closing
