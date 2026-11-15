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
  them. A row whose key cannot be read is dropped and counted, and fails no stream; a value
  that does not fit is null. A connection logs one WARNING per outage, and one when it goes
  silent during its
  market's trading hours (`NO_FRAMES_WARNING`), each with its recovery at INFO. Drift is
  recorded on the connection's `SchemaDrift` here and handed to the flush thread for the
  streams' rows.
- **The options cap.** An `/options` connection carries at most `OPTIONS_CAP` explicit topics.
  A static symbol list that would cross it raises `ValueError` when the verb is called; a
  reactive table's excess topics are deferred, in sorted order, and admitted as room frees.
- **Watchlist changes.** A change of a reactive symbol table reaches the stream's connection
  directly, except for futures and crypto, whose changes wait for a lookup first. Those run one
  at a time, in order, on the session's `symbol changes` worker, so a removal never overtakes
  an earlier addition still waiting: a futures contract waits up to `SCHEDULE_WAIT` for its
  trading schedule, so its first rows are dated, and a crypto pair is split by Massive's crypto
  ticker list. A pair the list does not hold, or that a failing read of the list cannot split
  after `PAIR_RETRY_DELAYS`, follows `MassiveConfig.unlisted_symbols`: under `LOG` and
  `IGNORE` it is skipped and listed in the stream's `Error`, and the stream keeps its other
  pairs; under `RAISE` it fails the stream. A symbol table's pairs present at the call follow
  the same setting (`tradable_pairs`). A lookup that meets a defect of the library is never
  retried or skipped: it fails the stream, or raises at the call. Any unexpected exception of
  a change is logged with its trace and fails the stream.
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

import enum
import functools
import logging
import threading
import weakref
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Final

from deephaven_massive._datasets.symbols import AllSymbols, SymbolTable
from deephaven_massive._enums import AssetClass, ChannelFamily, OnError
from deephaven_massive._errors import (
    LIBRARY_BUG,
    NEW_SESSION,
    AuthenticationError,
    EntitlementError,
    ErrorSource,
    MassiveError,
    PermanentError,
    SessionClosedError,
    copy_error,
    failure_text,
    library_bug,
    required,
)
from deephaven_massive._ingest.host_decision import real_time_note, require_real_time
from deephaven_massive._ingest.subscriptions import is_wildcard, stream_topics, wildcard
from deephaven_massive._ingest.ws_connection import (
    OPTIONS_CAP,
    ClientFactory,
    Connection,
    ConnectionGroup,
    ConnectionStream,
)
from deephaven_massive._log_limits import BoundedLog, bounded_list, mark_failure_logged
from deephaven_massive._tables.diagnostics import NotePart, StreamUpdate
from deephaven_massive._tables.execution_context import holds_update_graph_lock
from deephaven_massive._tables.liveness import release_scope
from deephaven_massive._tables.stream import ChannelPlan, open_stream_tables
from deephaven_massive._tables.watchlist import SymbolRowRule, read_symbols

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
    "PAIR_RETRY_DELAYS",
    "SCHEDULE_WAIT",
    "SCHEDULE_WAIT_HOLDING_LOCK",
    "WsConnections",
]

logger = logging.getLogger(__name__)

JOIN_TIMEOUT: Final = 5.0
"""Seconds `m.close()` waits for each connection's threads before it logs a WARNING."""

SCHEDULE_WAIT: Final = 15.0
"""Seconds a futures stream waits for its contracts' products and schedules before it
subscribes them, so their first rows are dated by the schedules: the verb waits for explicit
contracts, and the session's `symbol changes` worker for those a symbol table holds or adds. A
lookup still running then dates its rows once it ends, and they have a null `Date` meanwhile."""

SCHEDULE_WAIT_HOLDING_LOCK: Final = 1.0
"""Seconds the wait of `SCHEDULE_WAIT` lasts instead when the verb's thread holds the update
graph's lock (a console command): no live table updates while it waits, and its rows get a null
`Date` until the schedules load anyway, so it waits only long enough for a lookup that is
already answered."""

PAIR_RETRY_DELAYS: Final = (2.0, 8.0)
"""Seconds the `symbol changes` worker waits before each further try to split the crypto pairs
a symbol table added, after reading Massive's crypto ticker list failed. Each try reads the list
once for the whole change; after the last try fails, the pairs still unsplit are skipped. A
failure no retry can fix (a rejected key, a plan without reference data) skips them at once."""

_UNREADABLE: Final = (AuthenticationError, EntitlementError)
"""The failures to read Massive's crypto ticker list that the same read would meet again."""


_SKIPPED_PAIR_LIMIT: Final = 100
"""The most skipped crypto pairs logged per process under each of `LOG` and `IGNORE`."""
_logged_pairs: Final = BoundedLog(logger, limit=_SKIPPED_PAIR_LIMIT, level=logging.WARNING)
"""The pairs skipped under `unlisted_symbols=LOG`: a row the user must fix, logged at WARNING."""
_ignored_pairs: Final = BoundedLog(logger, limit=_SKIPPED_PAIR_LIMIT, level=logging.DEBUG)
"""The pairs skipped under `unlisted_symbols=IGNORE`: logged at DEBUG only."""


class _SkipReason(enum.Enum):
    """Why a crypto pair a symbol table added is skipped."""

    UNLISTED = enum.auto()
    """Massive's crypto ticker list does not hold the pair, or its currencies do not join to it."""
    LIST_UNREAD = enum.auto()
    """Massive's crypto ticker list could not be read."""


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
    if isinstance(symbols, AllSymbols):
        return None, frozenset({wildcard(channel)})
    if isinstance(symbols, SymbolTable):
        return frozenset(), frozenset()
    return frozenset(symbols), stream_topics(asset_class, channel, symbols, pairs=pairs)


# The connection groups of the process, by sharing key (`ConnectionGroup.attach`). Its lock is
# taken before a group's lock, never under it.


def _skipped_pairs_parts(skipped: Mapping[str, tuple[_SkipReason, str]]) -> list[str]:
    """Say which crypto pairs cannot be streamed and why, one part per reason.

    Args:
        skipped: The pairs, with why; at least one.

    Returns:
        The pairs Massive's list does not hold, then those that could not be split because the
        list could not be read, with the first such pair's error.
    """
    unlisted = sorted(s for s, (reason, _) in skipped.items() if reason is _SkipReason.UNLISTED)
    unread = sorted(s for s, (reason, _) in skipped.items() if reason is _SkipReason.LIST_UNREAD)
    parts = []
    if unlisted:
        parts.append(
            f"crypto pairs {bounded_list(unlisted)}, which Massive's crypto ticker list does not "
            "hold"
        )
    if unread:
        parts.append(
            f"crypto pairs {bounded_list(unread)}, since Massive's crypto ticker list could not "
            f"be read ({skipped[unread[0]][1]})"
        )
    return parts


def _pairs_error(
    message: str, skipped: Mapping[str, tuple[_SkipReason, str]], unread: MassiveError | None
) -> MassiveError:
    """Return the error of crypto pairs a stream cannot subscribe.

    Pairs Massive's crypto ticker list does not hold are Massive's answer, which a retry does
    not change. When every pair was skipped because the list could not be read, the error is a
    copy of that read's failure (`copy_error`), so it keeps its source and whether a retry may
    help.

    Args:
        message: The error's message.
        skipped: The skipped pairs, with why; at least one.
        unread: The failure to read the list, when it left pairs unsplit; else None.

    Returns:
        The error.
    """
    if unread is not None and all(r is _SkipReason.LIST_UNREAD for r, _ in skipped.values()):
        return copy_error(unread, message)
    return MassiveError(message)


def _skipped_pairs_text(skipped: Mapping[str, tuple[_SkipReason, str]]) -> str:
    """Say which crypto pairs a stream skips and why, for its `Error` in `m.streams()`.

    Args:
        skipped: The skipped pairs, with why; at least one.

    Returns:
        Each part of `_skipped_pairs_parts`, then how to look them up again.
    """
    parts = [f"skipped {part}" for part in _skipped_pairs_parts(skipped)]
    return "; ".join(
        [*parts, "a skipped pair stays skipped until its row is deleted and added again"]
    )


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
        crypto_pair: Callable[[str], tuple[str, str]],
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
        self.fetch_rows = fetch_rows
        self._crypto_pair = crypto_pair
        self._unlisted: Final = core.config.unlisted_symbols
        # What this session's streams do when another connection takes theirs; read by its
        # connections under the group's lock, so sessions differing in it still share them.
        self.connection_taken: Final = core.config.connection_taken
        # The futures schedule calendar reads the paths of the futures reference datasets.
        by_name = core.catalog.by_name
        self._calendar_paths: Final = (
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
        # Set by close(), so a worker waiting to retry a pair, or for schedules, stops waiting.
        self._closing: Final = threading.Event()
        # The crypto pairs each stream skips, with why, by stream; guarded by the group's lock.
        self._skipped_pairs: dict[int, dict[str, tuple[_SkipReason, str]]] = {}
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
        resolution raises the `AuthenticationError` or `MassiveError` of `resolve_host`, and
        for a real-time-only stream the `ValueError` or `EntitlementError` of
        `require_real_time`; a
        crypto ticker of a list that Massive's crypto ticker list does not split raises the
        `MassiveError` of `resolve_pairs`, and a reactive table's pairs raise as
        `tradable_pairs` says.
        """
        # The calling verb runs on the core, which is alive while the verb runs.
        core = required(self._core(), "the session")
        asset_class = spec.single_class
        plan = self._plan(core, spec, channel)
        symbols = request.symbol_key()
        if asset_class is AssetClass.CRYPTO:
            if isinstance(symbols, SymbolTable):
                rule = SymbolRowRule.of(spec, core.config.noncanonical_symbols)
                rows = read_symbols(rule, symbols.table, stream_id=stream_id).symbols
                self.tradable_pairs(spec, rows)  # the watch subscribes the rows it keeps
            elif isinstance(symbols, tuple):  # a list in code, or a table read and checked
                self.resolve_pairs(symbols)
        wanted, topics = _wanted(asset_class, channel, symbols, pairs=self.pairs())
        decision = require_real_time(
            spec,
            functools.partial(self._resolve, asset_class, spec.channel_family),
            delayed_subscriptions=core.config.delayed_subscriptions,
        )
        connection, created = self._lease(core, spec, decision, stream_id=stream_id, topics=topics)
        try:
            # Started before the schedule wait, so a failure or interrupt during that wait
            # leaves a running connection that `_abandon` retires once idle: a registered
            # connection that never started would be reused by later streams and never
            # connect. It subscribes nothing until `_attach`, after the schedules load.
            if created:
                connection.start()
            if asset_class is AssetClass.FUTURES and wanted:
                locked = holds_update_graph_lock(self._exec_ctx)
                self._load_schedules(wanted, stream_id=stream_id, holding_lock=locked)
            opened = open_stream_tables(
                plan,
                core.dataset_schema(spec),
                symbols,
                core=core,
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
            self._attach(stream)
            if opened.watch is not None:
                opened.watch.start()
        except BaseException:
            release_scope(core.exec_ctx, opened.handle.scope)
            self._abandon(connection, stream_id)
            raise
        return opened.handle

    def _load_schedules(
        self, contracts: frozenset[str], *, stream_id: int, holding_lock: bool
    ) -> None:
        """Load futures contracts' schedules before they subscribe, waiting a bounded time.

        Runs on the verb's thread or on the `symbol changes` worker, with no library lock
        held. It waits only on the calendar's worker, which makes no Deephaven call, so a
        console command holding the update graph's lock may run it; such a caller waits
        `SCHEDULE_WAIT_HOLDING_LOCK`, since no live table updates meanwhile, and any other
        `SCHEDULE_WAIT`. A lookup that does not end in time is logged at WARNING once; its rows
        get a null `Date` until it ends. A failed lookup logs its own WARNING. `close()` ends the
        wait at once (`FuturesCalendar.wake`), so the worker never delays the session's close
        or the interpreter's exit.

        Args:
            contracts: Canonical futures symbols; spreads are never looked up.
            stream_id: The stream, for the log.
            holding_lock: Whether the calling thread holds the update graph's lock.

        Raises:
            SessionClosedError: If the session is closed (`futures_calendar`), or closes during
                the wait.
        """
        wait = SCHEDULE_WAIT_HOLDING_LOCK if holding_lock else SCHEDULE_WAIT
        calendar = self.futures_calendar()
        calendar.watch(contracts)
        loaded = calendar.wait_loaded(contracts, wait, stop=self._closing)
        if self._closing.is_set():  # close() ended the wait: the stream goes with the session
            raise SessionClosedError(f"{self._label} is closed; {NEW_SESSION}")
        if not loaded:
            logger.warning(
                "futures stream %d: the schedules of %s did not load within %.0f s%s; their "
                "rows get a null Date until they do",
                stream_id,
                bounded_list(sorted(contracts)),
                wait,
                ", the most a caller holding the update graph's lock waits" if holding_lock else "",
            )
        else:
            logger.debug(
                "futures stream %d: schedules loaded for %s",
                stream_id,
                bounded_list(sorted(contracts)),
            )

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
                core.catalog, spec, channel, unreadable_symbols=core.config.unreadable_symbols
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
        self._skipped_pairs.pop(stream_id, None)
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
        never blocks or raises. A futures or crypto change, which waits for a lookup first, goes
        to the `symbol changes` worker (`_run_change`), which runs a session's changes one at a
        time, in order; every other change goes to the connection's loop at once. The added
        futures contracts are handed to the schedule calendar here (`FuturesCalendar.watch`,
        which only queues them), so every lookup starts at once and only the wait runs on the
        worker. A change that finds the session closing, or the worker shut down by
        `close()`, is dropped and logged at DEBUG, since the stream goes with the session.

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
        if asset_class is AssetClass.FUTURES:
            change = self._change_contracts
            if added:
                try:
                    calendar = self.futures_calendar()
                except SessionClosedError as error:
                    self._drop_change(stream_id, added, removed, error=error)
                    return
                calendar.watch(added)
        elif asset_class is AssetClass.CRYPTO:
            change = self._change_pairs
        else:
            connection.post(connection.change_symbols, stream_id, added, removed)
            return
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
            change: `_change_contracts` or `_change_pairs`.
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

    def _change_contracts(
        self, stream_id: int, added: frozenset[str], removed: frozenset[str]
    ) -> None:
        """Load a futures stream's added contracts' schedules, then post the change.

        Runs on the `symbol changes` worker (`_run_change`), which holds no lock and never holds
        the update graph's, so it waits up to `SCHEDULE_WAIT` (`_load_schedules`); the lookups
        started when the change was reported (`_symbols_changed`). The contracts a symbol
        table holds when the stream opens, and those it adds later, subscribe once their
        schedules load, and their first rows are dated, as explicit contracts' are. A change
        that finds the session closed is dropped and logged at DEBUG: the close tears the
        stream down, so failing it from here would only blame its watchlist.

        Args:
            stream_id: The stream.
            added: Canonical symbols the table now holds.
            removed: Canonical symbols it no longer holds.
        """
        if added:
            try:
                self._load_schedules(added, stream_id=stream_id, holding_lock=False)
            except SessionClosedError as error:
                self._drop_change(stream_id, added, removed, error=error)
                return
        self._post_change(stream_id, added, removed)

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
        """Split a crypto stream's added pairs, then post the change, skipping those that fail.

        Runs on the `symbol changes` worker (`_run_change`, which turns any unexpected
        exception into the stream's failure). Its only Deephaven call is the check, when a
        lookup starts or joins one already under way, of whether its thread holds the update
        graph's lock, which sets how long the lookup waits out rate-limit pauses
        (`rate_limits.read_patience`; the worker never holds it, so 10 minutes). A pair that
        cannot be split (`_split_pairs`) follows `MassiveConfig.unlisted_symbols`. Under `LOG`
        and `IGNORE` it is skipped: never subscribed, logged once (`_note_skipped`), and listed
        in the stream's `Error` in `m.streams()` until the table no longer holds it; deleting
        and adding the row again looks it up again, and the stream keeps its other pairs.
        Under `RAISE` it fails the stream (`Flusher.fail`), and nothing is subscribed.

        Args:
            stream_id: The stream.
            added: Canonical symbols the table now holds.
            removed: Canonical symbols it no longer holds.
        """
        reader = f"crypto stream {stream_id}"
        skipped, unread = self._split_pairs(added, reader=reader, delays=PAIR_RETRY_DELAYS)
        if skipped and self._unlisted is OnError.RAISE:
            failure = _pairs_error(
                f"cannot stream the symbol table's {'; '.join(_skipped_pairs_parts(skipped))}; "
                "set dm.MassiveConfig(unlisted_symbols=dm.OnError.LOG) to skip such a pair "
                "instead",
                skipped,
                unread,
            )
            logger.debug("%s: fails, under unlisted_symbols=RAISE: %s", reader, failure)
            with self._lock:
                connection = self.group.streams.get(stream_id)
            if connection is not None:
                connection.flusher.fail([stream_id], failure)
            return
        self._note_skipped(skipped, reader=reader)
        self._show_skipped_pairs(stream_id, skipped, removed)
        subscribe = added - skipped.keys()
        if subscribe or removed:
            self._post_change(stream_id, subscribe, removed)

    def _split_pairs(
        self, symbols: Iterable[str], *, reader: str, delays: Iterable[float]
    ) -> tuple[dict[str, tuple[_SkipReason, str]], MassiveError | None]:
        """Split the crypto pairs of one change, retrying a failed read of the list as a whole.

        Runs on the `symbol changes` worker, or on a verb's thread for a symbol table's pairs
        at the call (`tradable_pairs`, with no delay). A pair the list does not hold is skipped
        at once. A read the same request would fail again (a rejected key, a plan without
        reference data) skips every pair not yet split. Any other failure to read the list ends
        the try: the thread waits each of `delays` in turn, logged at INFO, and tries again with
        only the pairs not yet split, so each try reads the list at most once. After the last,
        or when a close interrupts the wait, the pairs not yet split are skipped. A
        `MassiveError` whose source is `ErrorSource.LIBRARY` is a defect of the library,
        not a pair Massive does not list: it propagates at once, untried again, so the worker
        fails the stream (`_run_change`) and a verb raises it.

        Args:
            symbols: Canonical crypto tickers.
            reader: Who reads them, for the log: `crypto stream <id>`, or the dataset's call.
            delays: The seconds to wait before each further try; `PAIR_RETRY_DELAYS` on the
                worker, none at the call.

        Returns:
            The skipped pairs, each with why: its `_SkipReason` and the error's text; and the
            last failure to read the list, when it left pairs unsplit (`LIST_UNREAD`), else None.

        Raises:
            MassiveError: The lookup's own error, when its source is `ErrorSource.LIBRARY`.
        """
        skipped: dict[str, tuple[_SkipReason, str]] = {}
        pending = sorted(symbols)
        waits = iter(delays)
        while pending:
            failure: MassiveError | None = None
            for index, symbol in enumerate(pending):
                try:
                    self.resolve_pairs((symbol,))
                except MassiveError as error:
                    if error.source is ErrorSource.LIBRARY:
                        raise  # a defect is never retried or shown as a skipped pair
                    if isinstance(error, PermanentError):
                        skipped[symbol] = (_SkipReason.UNLISTED, str(error))
                        continue
                    failure, pending = error, pending[index:]
                    break
            else:
                break
            delay = None if isinstance(failure, _UNREADABLE) else next(waits, None)
            if delay is not None:
                logger.info(
                    "%s: %s; trying %s again in %.0f s",
                    reader,
                    failure,
                    bounded_list(pending),
                    delay,
                )
            if delay is None or self._closing.wait(delay):
                skipped.update((s, (_SkipReason.LIST_UNREAD, str(failure))) for s in pending)
                return skipped, failure
        return skipped, None

    def tradable_pairs(self, spec: DatasetSpec, symbols: tuple[str, ...]) -> tuple[str, ...]:
        """Return the pairs of a symbol table, read at the call, that a stream can subscribe.

        Called on the verb's thread, before a stream starts: for a static symbol table by the
        query (`StreamTransport.tradable_pairs`), and for a reactive one by `open_stream`. A
        crypto pair that Massive's crypto ticker
        list does not hold, or that a failed read of the list cannot split, follows
        `MassiveConfig.unlisted_symbols`: under `LOG` and `IGNORE` it is left out and logged
        once (`_note_skipped`); under `RAISE` it raises. No read is retried here, since a
        console caller holds the update graph's lock: a reactive table's skipped pairs are
        looked up again, with the retries, when the stream's watch reports its rows, and listed
        in the stream's `Error` then. May block for one read of the list, with no lock held.

        Args:
            spec: A crypto dataset.
            symbols: The table's canonical symbols, sorted.

        Returns:
            The symbols a stream can subscribe, in order.

        Raises:
            AuthenticationError: Under `RAISE`, if Massive rejects the key on the read of the
                list.
            EntitlementError: Under `RAISE`, if the plan does not include reference data.
            MassiveError: Under `RAISE`, if a pair cannot be split; under `LOG` and `IGNORE`,
                if no pair can, so the result would follow nothing, or if the lookup meets a
                defect of the library (`ErrorSource.LIBRARY`), which is never skipped.
        """
        if self._unlisted is OnError.RAISE:
            self.resolve_pairs(symbols)
            return symbols
        reader = f"{spec.name} call"
        skipped, unread = self._split_pairs(symbols, reader=reader, delays=())
        if skipped and len(skipped) == len(symbols):
            failure = _pairs_error(
                f"{spec.name}: no crypto pair of the symbol table can be streamed, so the result "
                f"would follow nothing: {'; '.join(_skipped_pairs_parts(skipped))}",
                skipped,
                unread,
            )
            raise failure
        self._note_skipped(skipped, reader=reader)
        return tuple(symbol for symbol in symbols if symbol not in skipped)

    def _note_skipped(self, skipped: Mapping[str, tuple[_SkipReason, str]], *, reader: str) -> None:
        """Log each skipped crypto pair once: at WARNING under `LOG`, at DEBUG under `IGNORE`.

        Each pair is logged once per process, a bounded number of pairs, so a table edited for
        weeks keeps the log small. Never called under `RAISE`.

        Args:
            skipped: The skipped pairs, with why.
            reader: Who read them: `crypto stream <id>`, or the dataset's call.
        """
        pairs = _logged_pairs if self._unlisted is OnError.LOG else _ignored_pairs
        for symbol, (_, why) in sorted(skipped.items()):
            pairs.log(
                symbol,
                "%s: %s; the pair is skipped until its row is deleted and added again",
                reader,
                why,
            )

    def _show_skipped_pairs(
        self,
        stream_id: int,
        skipped: Mapping[str, tuple[_SkipReason, str]],
        removed: frozenset[str],
    ) -> None:
        """Update the pairs a crypto stream skips, and its `Error` when they change.

        The pairs the table no longer holds leave the set, and those just skipped join it. The
        set is shown as the stream's `NotePart.SKIPPED_PAIRS` warning (`_skipped_pairs_text`),
        cleared once it is empty. `SessionCore.update_streams` takes only the leaf
        `registry_lock`.

        Args:
            stream_id: The stream.
            skipped: The pairs just skipped, with why.
            removed: The symbols the table no longer holds.
        """
        with self._lock:
            if stream_id not in self.group.streams:
                return
            before = self._skipped_pairs.get(stream_id, {})
            after = {s: why for s, why in before.items() if s not in removed} | dict(skipped)
            if after:
                self._skipped_pairs[stream_id] = after
            else:
                self._skipped_pairs.pop(stream_id, None)
        if after == before:
            return
        core = self._core()
        if core is None:
            return
        text = _skipped_pairs_text(after) if after else None
        core.update_streams({stream_id: StreamUpdate(notes={NotePart.SKIPPED_PAIRS: text})})

    def resolve_pairs(self, symbols: Iterable[str]) -> None:
        """Look up, once per group, the base and quote of each crypto symbol.

        A joined ticker does not say where its quote starts (`X:DOTUSD` could be `DOT-USD` or
        `DO-TUSD`), so each takes its base and quote from Massive's crypto ticker list, which
        the session reads once; a topic is never built from a guess. May block the calling thread
        for that read, with no lock held, so it runs on the caller thread before a stream
        starts, or on the `symbol changes` worker.

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
                named = copy_error(error, f"cannot stream {symbol!r}: {error}")
                raise named from error
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
        self.group.wake_schedule_waiters()  # a worker waiting for schedules sees the close
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
