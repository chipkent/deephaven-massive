"""`SessionCore`, the internal engine of a session, and the transport ports its verbs call.

`MassiveSession` (in `_session.py`) is what users create; it delegates everything stateful to
one `SessionCore`, which queries and namespaces share. The core owns:

- the session's label (`MassiveSession #1`), used in error messages and log records;
- the configuration, the process's dataset `Catalog`, and the captured Deephaven
  `ExecutionContext`, entered on every library thread;
- `registry_lock`, the one leaf lock that guards every session registry: no Deephaven call, network
  call, or blocking wait happens while it is held;
- the stream registry: every stream from just before it opens until its holds are released
  and its blink table has shut down, each with its diagnostics row (which carries its count of
  library-held results), and the key index that maps each `(dataset, channel, symbols)` to the
  one opening or held, unfailed, live stream that serves it;
  every diagnostics row is queued while `registry_lock` is held, so rows reach the tables in
  the order the registry changed;
- the `poll()` registry: the `m.streams()` row of every `poll()` table from its creation until
  its release;
- the result registry and the reaper thread (`liveness.py`);
- the diagnostics tables and their writer thread, and the drift registry that every transport
  reports schema drift to (`diagnostics.py`);
- the closers that run when the session closes, or when it is collected without `close()`;
- one factory hook per transport (`rest_transport`, `stream_transport`, `flat_file_transport`),
  each returning one of the ports defined here: `RestTransport` (`fetch`, `poll`, REST
  `history`, and the WebSocket host decision), `StreamTransport` (opening a WebSocket stream),
  and `FlatFileTransport` (flat-file `history`).

The core holds no wire client and imports no ingest code. The session's ingest side
(`SessionClients` in `_ingest/clients.py`) owns every wire client and connection and provides
the ports through the `IngestPorts` protocol defined here; `MassiveSession` builds the core,
then the clients, and attaches them once (`attach`), and each hook asks the attached ports.
So the core and everything else on the Deephaven side are tested without a fake server, and a
dependency never points from the tables up to the code that does the I/O.

The module also holds the process-wide stream-id allocator and three context checks.
`on_update_graph` runs at every entry point: `MassiveSession()`, every verb (through
`SessionCore.require_verb_thread`), and `close()`. `has_execution_context` and
`has_script_query_scope` run only when a `MassiveSession` is constructed, which is the one place
an execution context is captured.

Lock order: the update-graph (UG) lock, then `registry_lock`. A stream is registered under
`registry_lock` before it opens (with a session-minted id), then opened with no lock held; a
second verb on the same key waits for that open on its own thread, outside every lock, instead
of opening a second stream.
"""

from __future__ import annotations

import functools
import itertools
import logging
import threading
import time
import weakref
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final, Protocol

from deephaven.dherror import DHError
from deephaven.update_graph import has_exclusive_lock, has_shared_lock

from deephaven_massive._enums import UnknownSymbols
from deephaven_massive._errors import MassiveError
from deephaven_massive._massive import Transport
from deephaven_massive._tables.diagnostics import (
    Diagnostics,
    DriftRegistry,
    StreamRow,
    StreamState,
    StreamUpdate,
    describe_symbols,
)
from deephaven_massive._tables.liveness import Reaper, ResultRegistry, entered, make_result

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence
    from datetime import date, timedelta

    from deephaven.execution_context import ExecutionContext
    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table

    from deephaven_massive._config import MassiveConfig
    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.history_source import HistoryWindow
    from deephaven_massive._datasets.request import HostDecision, Request, StreamKey
    from deephaven_massive._datasets.schema import Schema
    from deephaven_massive._enums import AssetClass, ChannelFamily, PollMode
    from deephaven_massive._massive import DriftReport
    from deephaven_massive._tables.liveness import StreamHandle

__all__ = [
    "ON_UPDATE_GRAPH",
    "FlatFileTransport",
    "IngestPorts",
    "RestTransport",
    "SessionCore",
    "StreamTransport",
    "has_execution_context",
    "has_script_query_scope",
    "on_update_graph",
]

logger = logging.getLogger(__name__)


# A thread with no execution context gets Deephaven's empty default context, whose update graph
# is a PoisonedUpdateGraph: creating any refreshing table under it fails. That class is the
# only signal Deephaven offers for "no context is active".
_POISONED_UPDATE_GRAPH: Final = "PoisonedUpdateGraph"

# The query scope of a script session's context: formulas resolve names through the session's
# live globals, which is how a formula such as `update_view("Symbol = canonical_symbol(...)")`
# reaches a Python function the library defines. `make_user_exec_ctx(freeze_vars=[...])`
# builds a StandaloneQueryScope holding only the frozen variables instead.
_SCRIPT_QUERY_SCOPE: Final = (
    "io.deephaven.engine.util.AbstractScriptSession$ScriptSessionQueryScope"
)

# Stream ids are minted here, not by the transport, so a stream is registered before it opens.
# Its own leaf lock: the session module's registration lock is taken before a session's
# `registry_lock`, and ids are minted under `registry_lock`, so sharing that lock would invert
# that order.
_stream_id_lock = threading.Lock()
_stream_numbers = itertools.count(1)


def has_execution_context(exec_ctx: ExecutionContext) -> bool:
    """Tell whether an execution context is a real one, not the thread's empty default.

    Args:
        exec_ctx: The context `get_exec_ctx()` returned.

    Returns:
        True if live tables can be created under it.
    """
    j_update_graph = exec_ctx.update_graph.j_update_graph
    return str(j_update_graph.getClass().getSimpleName()) != _POISONED_UPDATE_GRAPH


def has_script_query_scope(exec_ctx: ExecutionContext) -> bool:
    """Tell whether formulas under an execution context can call the library's Python functions.

    Args:
        exec_ctx: The context `get_exec_ctx()` returned.

    Returns:
        True when the context's query scope is the Python script session's own scope, False
        for a frozen copy (`make_user_exec_ctx(freeze_vars=[...])`).
    """
    j_query_scope = exec_ctx.j_exec_ctx.getQueryScope()
    return str(j_query_scope.getClass().getName()) == _SCRIPT_QUERY_SCOPE


def on_update_graph(exec_ctx: ExecutionContext) -> bool:
    """Tell whether the calling thread is an update-graph thread or holds the UG lock.

    Args:
        exec_ctx: The session's execution context.

    Returns:
        True on a UG refresh thread (for example inside a table listener), or while the
        thread holds the UG's shared or exclusive lock.
    """
    update_graph = exec_ctx.update_graph
    return (
        bool(update_graph.j_update_graph.currentThreadProcessesUpdates())
        or has_exclusive_lock(update_graph)
        or has_shared_lock(update_graph)
    )


ON_UPDATE_GRAPH: Final = (
    "deephaven_massive calls must not run on a Deephaven update-graph thread "
    "(for example inside a table listener) or while holding the update-graph lock"
)
"""The `ValueError` text for a call on an update-graph thread: verbs create tables and take the
registry lock, which would deadlock against the update cycle there."""


def _next_stream_id() -> int:
    """Return the next stream id of the process.

    Returns:
        A process-unique id, numbered from 1 (the `StreamId` of `m.streams()`).
    """
    with _stream_id_lock:
        return next(_stream_numbers)


class RestTransport(Protocol):
    """The REST transport, from `SessionCore.rest_transport()` (`IngestPorts.rest`).

    Every method runs synchronously on the calling thread and raises `EntitlementError` on
    HTTP 403 and `MassiveError` on other failures; `poll()` instead fails its live table.
    """

    def resolve_host(self, asset_class: AssetClass, family: ChannelFamily) -> HostDecision:
        """Decide the WebSocket host of a (class, family), once per session.

        Called by the WebSocket transport on the caller thread at the first stream verb for the
        pair. The first call may make one REST request; the decision is then cached for the
        life of the session, so later calls return it without a request. Each new decision
        writes the pair's `Transport=REST` row of `m.connections()` (host, recency, evidence).

        Args:
            asset_class: The asset class of the stream.
            family: The channel family, `TICK` or `FMV`.
        """
        ...

    def crypto_pair(self, symbol: str) -> tuple[str, str]:
        """Return a crypto ticker's base and quote, looking them up in Massive if needed.

        Called by the WebSocket transport for each crypto ticker of a stream, before it starts
        or on its lookup worker. The first such call of the session blocks while it reads
        Massive's crypto ticker list (`/v3/reference/tickers?market=crypto`, every page, listed
        and delisted pairs), which the session then keeps; a failed read is read again by the
        next call.

        Args:
            symbol: A canonical crypto ticker.

        Returns:
            `(base, quote)`, such as `("DOT", "USD")` for `X:DOTUSD`.

        A failed read, or a ticker the list does not hold, raises a `MassiveError` naming the
        ticker.
        """
        ...

    def record_verdict(
        self, asset_class: AssetClass, family: ChannelFamily, decision: HostDecision
    ) -> None:
        """Replace the cached host decision of a (class, family) with a server's verdict.

        Called by the WebSocket transport when the explicit-verdict rule re-routes a
        connection (for example to the delayed host, with `Recency.DELAYED` and evidence
        `"server verdict: <message>"`). Every later `resolve_host` for the pair returns this
        decision, and the pair's `Transport=REST` row of `m.connections()` is rewritten with
        it. No request is made and nothing blocks.

        Args:
            asset_class: The asset class of the connection.
            family: The channel family.
            decision: The new decision.
        """
        ...

    def fetch_rows(self, template: str, query: Mapping[str, object]) -> list[dict[str, object]]:
        """Return every raw JSON row of a REST path for a query, following its pages.

        For the library's own lookups (the futures schedule calendar of the WebSocket
        streams), which read a few fields and build no table.

        Args:
            template: The path template; it takes no path parameter.
            query: The query parameters, by wire name.
        """
        ...

    def fetch(self, spec: DatasetSpec, request: Request) -> Table:
        """Return every row the dataset's `fetch` endpoint returns for the request, static.

        Args:
            spec: The dataset.
            request: The normalized arguments.
        """
        ...

    def poll(
        self, spec: DatasetSpec, request: Request, *, interval: timedelta, mode: PollMode
    ) -> Table:
        """Return a live table that re-runs `fetch` every `interval`, tracked by the session.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            interval: The time between cycles.
            mode: How cycles combine.
        """
        ...

    def history(self, spec: DatasetSpec, request: Request, window: HistoryWindow) -> Table:
        """Return the dataset's REST history over `window`, paged internally, static.

        Args:
            spec: The dataset.
            request: The normalized arguments, with the parsed `start` and `end` bounds.
            window: What the call reads and keeps (`history_window` in
                `_datasets/history_source.py`).
        """
        ...


class StreamTransport(Protocol):
    """The WebSocket transport, from `SessionCore.stream_transport()` (`IngestPorts.streams`)."""

    def open_stream(
        self, spec: DatasetSpec, request: Request, channel: str, *, stream_id: int
    ) -> StreamHandle:
        """Create a new stream: its publisher and blink table in a new stream scope.

        Runs on the caller thread; may resolve the host with one REST call. Must not hold the
        session's registry lock while calling Deephaven. The session has already registered the
        stream under `stream_id`, so `SessionCore.fail_stream(stream_id, ...)` and
        `SessionCore.stream_shut_down(stream_id)` may be called from the moment the publisher
        exists, even before this method returns. The publisher is named
        `massive.{dataset}#{stream_id}`, and the blink table's `on_shutdown_callback` must first
        call `SessionCore.stream_shut_down(stream_id)` and only then release the stream's topics.
        The returned handle carries `stream_id`. If the open raises anything, even
        `KeyboardInterrupt`, the transport has released the stream scope and the stream's
        references (its connection lease and topics) before the exception propagates.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            channel: The WebSocket channel.
            stream_id: The id the session minted for the stream.
        """
        ...


class FlatFileTransport(Protocol):
    """The flat-file transport, from `SessionCore.flat_file_transport()` (`IngestPorts.flat_files`).

    It only reads files: `HistoryQuery.history()` has already chosen flat files as the source
    (`choose_source` in `_datasets/history_source.py`).
    """

    def history(
        self,
        spec: DatasetSpec,
        request: Request,
        window: HistoryWindow,
        *,
        dates: Sequence[date],
    ) -> Table:
        """Return the rows of `window` from the dataset's flat files, static.

        Args:
            spec: The dataset, with a flat-file prefix for the request.
            request: The normalized arguments, with the raw `start` and `end` bounds and static
                symbols (never a reactive table).
            window: What the call reads and keeps (`history_window` in
                `_datasets/history_source.py`).
            dates: The partition dates to read (`partition_dates` in `_datasets/history_source.py`).
        """
        ...


class IngestPorts(Protocol):
    """The session's ingest side as the core sees it: one factory per transport.

    Implemented by `SessionClients` in `_ingest/clients.py`, which owns the session's wire
    clients and connections, and attached to the core once by `MassiveSession` (`attach`). The
    core's hooks (`rest_transport`, `stream_transport`, `flat_file_transport`) call these
    methods; verbs call the hooks. Each method runs on the calling thread, makes no network
    request, and may be called from any thread.
    """

    def rest(self) -> RestTransport:
        """Return the session's REST transport, over one REST client for the session's life.

        The first call creates the session's REST client and registers its close with the core
        (`SessionCore.on_close`); later calls share it.
        """
        ...

    def streams(self) -> StreamTransport:
        """Return a WebSocket transport over the session's one set of connections.

        Each call may return a new, lightweight transport; every one opens its streams on the
        connections the session keeps for its whole life, which the first call creates and
        registers with the session's closers. No connection is opened.
        """
        ...

    def flat_files(self) -> FlatFileTransport:
        """Return a new flat-file transport for one `history()` call.

        The first call creates the session's S3 client and registers its close with the core
        (`SessionCore.on_close`); later calls share it.

        Raises:
            ValueError: If the configuration has no S3 credentials.
        """
        ...


def _collect_session(
    label: str,
    reaper: Reaper,
    diagnostics: Diagnostics,
    *,
    results: ResultRegistry,
    closers: list[Callable[[], None]],
) -> None:
    """Finalizer of a session collected without `close()`: clean up, without waiting.

    The closers (connections, pollers) are handed to the reaper, which runs them; the
    diagnostics writer ends. The reaper itself keeps running while results are still tracked
    (a result whose release path does not hold the core, such as a `poll()` table, can outlive
    the session) and ends once the last one has been collected (`ResultRegistry.retire`).
    Garbage collection can run this on any thread at any allocation, so it takes no lock and
    makes no Deephaven call. The closer list needs no
    lock here: the core is unreachable, so no other thread can reach the list, and `close()`
    empties it, so a closed session's closers never run twice.

    Args:
        label: The session's label, for the WARNING.
        reaper: The session's reaper.
        diagnostics: The session's diagnostics.
        results: The session's result registry, retired here.
        closers: The session's registered closers; emptied here.
    """
    pending = tuple(closers)
    closers.clear()
    if pending:
        reaper.submit(functools.partial(_close_collected, label, pending))
    results.retire()  # the reaper ends once no result it may still have to release is tracked
    diagnostics.stop()


def _close_collected(label: str, closers: tuple[Callable[[], None], ...]) -> None:
    """Reaper job of a session collected without `close()`: log a WARNING and run its closers.

    Args:
        label: The session's label.
        closers: The closers, run in registration order; one that raises is logged at ERROR
            and the rest still run.
    """
    logger.warning(
        "%s was garbage-collected without close(); closing its connections and pollers", label
    )
    for closer in closers:
        try:
            closer()
        except Exception:
            logger.exception("%s: a transport failed to close", label)


@dataclass(frozen=True, slots=True)
class _StreamState:
    """One stream of the registry; replaced, never mutated, under `registry_lock`.

    A record is registered before the transport opens the stream (`OPENING`, `handle` None),
    with one hold for the opener, so a failure or a shutdown reported while the transport is
    still opening finds it. It leaves the registry only once both its holds have reached zero
    and its blink table has shut down (or the open failed): Deephaven's shutdown callback and
    the library's holds run on different clocks, so either may come last. Until then its row
    keeps reporting row counts and a late failure, even after the library's last result was
    released while a derived table or an open panel keeps the publisher alive.

    Its key maps to it while it is opening (so a second verb on the key waits on `ready`
    instead of opening a second stream) and then while it is held, not failed, and not shut
    down; `_release_hold`, `fail_stream`, and `stream_shut_down` unmap it.

    Attributes:
        key: The stream's key.
        handle: What the WebSocket transport returned; None while the stream is opening.
        row: The stream's current diagnostics row; `held_results` counts every hold, the
            opener's included.
        ready: Set once the open finished, whether it succeeded or not, and by `close()`.
        shut_down: True once the blink table has shut down while holds were outstanding.
    """

    key: StreamKey
    handle: StreamHandle | None
    row: StreamRow
    ready: threading.Event
    shut_down: bool = False


class SessionCore:
    """The internal engine of one `MassiveSession`, shared by its namespaces and queries.

    Created by `MassiveSession.__init__` on the caller thread, with an execution context
    active, which then attaches the session's ingest ports (`attach`) before any verb can
    run. Thread-safe: every mutable registry is guarded by `registry_lock`.
    """

    def __init__(
        self, config: MassiveConfig, exec_ctx: ExecutionContext, label: str, *, catalog: Catalog
    ) -> None:
        """Create the core, its reaper, the diagnostics tables and writer, and the drift registry.

        Args:
            config: The validated configuration.
            exec_ctx: The caller's execution context, captured for library threads.
            label: The session's label, such as `"MassiveSession #1"`.
            catalog: Every declared dataset (`CATALOG` in `_session.py`), read by the
                transports.
        """
        self.config: Final = config
        self.exec_ctx: Final = exec_ctx
        self.label: Final = label
        self.catalog: Final = catalog
        self.registry_lock: Final = threading.RLock()
        # The diagnostics come first: creating their tables is the step most likely to fail,
        # and nothing has started a thread yet if it does.
        self.diagnostics: Final = Diagnostics(exec_ctx, f"{label} diagnostics")
        try:
            self._reaper = Reaper(exec_ctx, f"{label} reaper")
        except BaseException:  # the writer is running: stop it, or it holds the tables forever
            self.diagnostics.stop()
            raise
        self.results: Final = ResultRegistry(self.registry_lock, self._reaper)
        self.drift: Final = DriftRegistry()
        self._stream_ids: dict[StreamKey, int] = {}
        self._streams: dict[int, _StreamState] = {}
        # The rows of the live `poll()` tables, by stream id; they have no key and no handle.
        self._polls: dict[int, StreamRow] = {}
        # Set once by `attach`, right after construction and before any verb can run.
        self._ports: IngestPorts | None = None
        # Shared with the finalizer, which runs the closers of a session collected unclosed.
        self._closers: list[Callable[[], None]] = []
        self._closed = False
        # Not at interpreter exit: stopping the threads then would call into a JVM that may
        # already be shutting down.
        finalizer = weakref.finalize(
            self,
            _collect_session,
            label,
            self._reaper,
            self.diagnostics,
            results=self.results,
            closers=self._closers,
        )
        finalizer.atexit = False

    def dataset_schema(self, spec: DatasetSpec) -> Schema:
        """Return a dataset's schema in this session: `Catalog.dataset_schema` with `RawSymbol`.

        Every result, `columns()` table, and transport plan of the session reads its columns
        here, so they agree on whether the `RawSymbol` column exists: it does when the
        session's `unknown_symbols` is `WARN` and the dataset reads its symbol from rows.

        Args:
            spec: The dataset.

        Returns:
            The cached schema. A bad dataset raises what `Catalog.dataset_schema` raises.
        """
        return self.catalog.dataset_schema(
            spec, raw_symbol=self.config.unknown_symbols is UnknownSymbols.WARN
        )

    @property
    def closed(self) -> bool:
        """True once `close()` has run."""
        with self.registry_lock:
            return self._closed

    def require_open(self) -> None:
        """Reject a call on a closed session.

        Raises:
            MassiveError: `MassiveError("MassiveSession #1 is closed")` after `close()`.
        """
        if self.closed:
            raise MassiveError(f"{self.label} is closed")

    def require_verb_thread(self) -> None:
        """The guard every verb runs first: this is not a UG thread, and the session is open.

        The thread is checked first, so a verb on an update-graph thread raises `ValueError`
        whether or not the session is closed.

        Raises:
            ValueError: On an update-graph thread, or while holding the update-graph lock.
            MassiveError: If the session is closed.
        """
        if on_update_graph(self.exec_ctx):
            raise ValueError(ON_UPDATE_GRAPH)
        if self.closed:
            raise MassiveError(f"{self.label} is closed")

    # --- Transport factory hooks: one per transport, served by the attached ingest ports. ---

    def attach(self, ports: IngestPorts) -> None:
        """Attach the session's ingest side, which serves the transport hooks.

        `MassiveSession.__init__` calls it exactly once, on the caller thread, right after it
        creates the core and the session's `SessionClients` and before the session is
        returned, so every verb finds the ports attached. Nothing else attaches ports.

        Args:
            ports: The session's ingest side.

        Raises:
            RuntimeError: If ports are already attached.
        """
        if self._ports is not None:
            raise RuntimeError(f"{self.label} already has its transports attached")
        self._ports = ports

    def _attached_ports(self) -> IngestPorts:
        """Return the attached ingest ports.

        Returns:
            The ports `attach` stored.

        Raises:
            RuntimeError: If no ports were attached: the core was built outside a
                `MassiveSession`.
        """
        if self._ports is None:
            raise RuntimeError(f"{self.label} has no transports attached")
        return self._ports

    def rest_transport(self) -> RestTransport:
        """Return the session's REST transport (`IngestPorts.rest`).

        No request is made; the first call creates the session's REST client, which closes
        with the session. Without attached ports this raises the `RuntimeError` of
        `_attached_ports`.

        Returns:
            The transport.
        """
        return self._attached_ports().rest()

    def stream_transport(self) -> StreamTransport:
        """Return the session's WebSocket transport (`IngestPorts.streams`).

        No connection is opened. Without attached ports this raises the `RuntimeError` of
        `_attached_ports`.

        Returns:
            The transport.
        """
        return self._attached_ports().streams()

    def flat_file_transport(self) -> FlatFileTransport:
        """Return a flat-file transport for one `history()` call (`IngestPorts.flat_files`).

        The first call creates the session's S3 client, which closes with the session; no
        request is made. Without attached ports this raises the `RuntimeError` of
        `_attached_ports`; without S3 credentials in the configuration the ports raise
        `ValueError` (`history()` never gets here then: its source choice has already required
        them).

        Returns:
            The transport.
        """
        return self._attached_ports().flat_files()

    # --- Streams --------------------------------------------------------------------------

    def on_close(self, closer: Callable[[], None]) -> None:
        """Register a transport's cleanup (close connections, stop pollers) for `close()`.

        If the session is already closed (a transport that passed `require_open` just before
        `close()` ran), the closer runs at once, on this thread and outside the lock, so it is
        still cleaned up. If the session is garbage-collected without `close()`, its closers
        run on the reaper thread after a WARNING. Either way each closer runs exactly once.

        The closer must not refer to the session's core strongly (hold it through
        `weakref.ref` if needed): the closers live as long as the core, and the finalizer
        that runs them keeps them alive, so a strong reference would keep the core from ever
        being collected.

        Args:
            closer: Called once, without any lock held: on the thread that closes the session,
                here when the session is already closed, or on the reaper thread when the
                session is collected unclosed. An exception it raises during `close()` or
                collection is logged at ERROR and the remaining closers still run; raised
                here, it propagates to the caller.
        """
        with self.registry_lock:
            closed = self._closed
            if not closed:
                self._closers.append(closer)
        if closed:
            closer()

    def stream_result(
        self,
        spec: DatasetSpec,
        request: Request,
        *,
        channel: str,
        build: Callable[[Table], Table],
    ) -> Table:
        """Create one live result over the stream of `(spec, channel, symbols)`.

        The stream is reused when one with the same key is open. When another thread is opening it,
        this call waits for that open to finish (outside every lock) and then reuses the stream, or
        opens it itself if that open failed. Otherwise the WebSocket transport opens it here. The
        result is built from the stream's blink table in its own result scope on this thread, inside
        the session's execution context, and tracked so that garbage collection or `release()` frees
        it. If building or tracking the result raises anything, even `KeyboardInterrupt` (including
        one landing between `make_result` and `track`), the result scope, if it exists, and the
        stream hold are released before the exception propagates. Asynchronous exceptions such as
        `KeyboardInterrupt` are handled on a best-effort basis: every window between the library's
        own steps is covered, and the windows that remain are inside the cleanup code itself (an
        interrupt while a hold or a scope is being released).

        A stream that failed while it was opening, or between the claim and the build, serves no
        result: every shape, `blink()` included, raises `MassiveError` naming the stream's
        failure, with the hold released. A failure that lands while the shape is being derived
        makes Deephaven raise `DHError` ("Can not listen to failed table"), or jpy's
        `RuntimeError` from the described copy; that error is wrapped in the same
        `MassiveError`. Either way `m.streams()` shows `FAILED`.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            channel: The WebSocket channel.
            build: Turns the stream's blink table into the result. It must create a new
                table, never return the blink table itself, so the result scope manages the
                result and each call returns a distinct table.

        Returns:
            The live result, a distinct Python object per call.

        Raises:
            MassiveError: If the stream has failed (`"stocks.trades stream 7 failed: ..."`).
        """
        key: StreamKey = (spec.name, channel, request.symbol_key())
        handle = self._acquire(spec, request, key, channel=channel)
        blink = handle.blink
        scope: LivenessScope | None = None
        try:
            self._require_unfailed(spec, handle.stream_id)
            with entered(self.exec_ctx):
                result, scope = make_result(lambda: build(blink))
            self.results.track(
                result, scope, functools.partial(self._release_hold, handle.stream_id)
            )
        except BaseException as error:  # nothing was tracked: the scope and the hold are ours
            # Read first: releasing the last hold of a shut-down stream deletes its record.
            # A failed table fails a shape with `DHError`, or with jpy's `RuntimeError` when the
            # Java call is direct (the described copy every result ends with).
            deephaven = isinstance(error, DHError | RuntimeError)
            failure = self._failure(handle.stream_id) if deephaven else None
            try:
                if scope is not None:
                    with entered(self.exec_ctx):
                        scope.release()
            finally:
                self._release_hold(handle.stream_id)
            if failure is not None:
                raise MassiveError(
                    f"{spec.name} stream {handle.stream_id} failed: {failure}"
                ) from error
            raise
        return result

    def _require_unfailed(self, spec: DatasetSpec, stream_id: int) -> None:
        """Refuse to build a result over a stream that has failed.

        Args:
            spec: The stream's dataset, named in the message.
            stream_id: The stream.

        Raises:
            MassiveError: If the stream has failed; the message names its failure.
        """
        failure = self._failure(stream_id)
        if failure is not None:
            raise MassiveError(f"{spec.name} stream {stream_id} failed: {failure}")

    def _failure(self, stream_id: int) -> str | None:
        """Return a stream's failure text, if it has failed and is still in the registry.

        Args:
            stream_id: The stream.

        Returns:
            Its `Error` text when its state is `FAILED`; None otherwise.
        """
        with self.registry_lock:
            state = self._streams.get(stream_id)
            if state is None or state.row.state is not StreamState.FAILED:
                return None
            return state.row.error or "failed"

    def _acquire(
        self, spec: DatasetSpec, request: Request, key: StreamKey, *, channel: str
    ) -> StreamHandle:
        """Take a hold on the stream of `key`: reuse it, wait for its open, or open it.

        Every hold `_claim` takes is reported in `holds` right after it is stored, and the claim,
        the open, and the wake-up of the waiters run inside one `try`/`except`; the handle is
        returned after it, with no checkpoint between the `try` and the caller's own cleanup. So
        whatever interrupts them, even a `KeyboardInterrupt` landing between two calls, the cleanup
        is exact: each hold this call took is released (a reused stream's extra hold; or the
        opener's hold, which deletes a never-opened record and unmaps its key, or releases an opened
        stream's scope), and a new record's `ready` event is set so waiting verbs wake. Waiting
        happens outside every lock. Asynchronous exceptions such as `KeyboardInterrupt` are handled
        on a best-effort basis: every window between the library's own steps is covered, and the
        windows that remain are inside the cleanup code itself (an interrupt while a hold or a scope
        is being released).

        Args:
            spec: The dataset.
            request: The normalized arguments.
            key: The stream key.
            channel: The WebSocket channel.

        Returns:
            The stream's handle, with one hold taken for the caller.

        A closed session, or one that closes while the stream opens, raises the `MassiveError`
        of `_claim` or `_open`.
        """
        while True:
            holds: list[tuple[int, threading.Event | None]] = []
            try:
                claim = self._claim(spec, request, key, holds=holds)
                if isinstance(claim, threading.Event):
                    claim.wait()  # another thread is opening the stream; never under a lock
                    continue
                if isinstance(claim, int):
                    claim = self._open(spec, request, channel=channel, stream_id=claim)
                for _, ready in holds:  # inside the try: an interrupt here still cleans up
                    if ready is not None:
                        ready.set()
            except BaseException:  # the hold did not reach the caller
                for stream_id, ready in holds:
                    try:
                        self._release_hold(stream_id)
                    finally:
                        if ready is not None:
                            ready.set()  # waiters must wake whatever happens
                raise
            return claim  # no checkpoint between the try and the caller's own cleanup

    def _claim(
        self,
        spec: DatasetSpec,
        request: Request,
        key: StreamKey,
        *,
        holds: list[tuple[int, threading.Event | None]],
    ) -> StreamHandle | threading.Event | int:
        """Take a hold on the stream with `key`, find its open in progress, or register it.

        One critical section under `registry_lock`; it never waits and makes no Deephaven
        call.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            key: The stream key.
            holds: Receives one `(stream_id, ready)` entry per hold this call takes, appended
                inside the lock right after the registry stores it, so the caller cleans up
                exactly what was taken: `(stream_id, None)` for a hold on an open stream it
                reuses, `(stream_id, ready)` for a new `OPENING` record and the opener's hold.
                An interrupt before the stores leaves nothing to clean up.

        Returns:
            The handle of an open stream, with one more hold taken on it; the `ready` event of
            a stream another thread is opening, to wait on before claiming again; or the id of
            a new `OPENING` record registered for this caller, with the opener's hold, which
            the caller must pass to `_open`.

        Raises:
            MassiveError: If the session is closed (a verb that passed the guard just before
                `close()` ran, or a waiter woken by `close()`); a closed stream is never reused.
        """
        with self.registry_lock:
            if self._closed:
                raise MassiveError(f"{self.label} is closed")
            existing = self._stream_ids.get(key)
            if existing is not None:
                state = self._streams[existing]
                if state.handle is None:
                    return state.ready
                row = replace(state.row, held_results=state.row.held_results + 1)
                self._streams[existing] = replace(state, row=row)
                holds.append((existing, None))  # right after the store: the caller cleans up
                self.diagnostics.write(row)
                return state.handle
            stream_id = _next_stream_id()
            count, text = describe_symbols(request.symbol_key())
            row = StreamRow(
                stream_id=stream_id,
                dataset=spec.name,
                symbol_count=count,
                symbols=text,
                held_results=1,
                transport=Transport.WS,
                created_at=time.time_ns(),
            )
            ready = threading.Event()
            state = _StreamState(key, None, row, ready)
            # The key first: hashing it can run Python (a SymbolTable box), so it may be
            # interrupted; the int-keyed store and the append after it run no Python code.
            # No row is written until the open finishes: m.streams() has no OPENING state.
            self._stream_ids[key] = stream_id
            self._streams[stream_id] = state
            holds.append((stream_id, ready))  # right after the stores: the caller cleans up
            return stream_id

    def _open(
        self, spec: DatasetSpec, request: Request, *, channel: str, stream_id: int
    ) -> StreamHandle:
        """Open the stream of a registered `OPENING` record through the WebSocket transport.

        Runs with no lock held while the transport works. The caller (`_acquire`) sets the
        record's `ready` event afterwards and, if this raises anything, even
        `KeyboardInterrupt`, drops the opener's hold, which deletes a never-opened record and
        unmaps its key; a waiting verb then opens the stream itself. If the interrupt lands
        after the transport returned the stream but before its handle is stored in the
        registry, nothing else knows the stream, so this method releases its scope itself. A
        failure or a shutdown the transport reported during the open has already been
        recorded on the record, and the stream is returned as is: `stream_result` then raises
        the `MassiveError` naming the failure instead of building a result.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            channel: The WebSocket channel.
            stream_id: The id `_claim` registered.

        Returns:
            The opened stream, with the opener's hold.

        Raises:
            MassiveError: If the session was closed while the transport opened the stream; the
                caller then releases the opener's hold, which releases the new stream's scope.
        """
        opened: StreamHandle | None = None
        stored = False
        try:
            with entered(self.exec_ctx):
                opened = self.stream_transport().open_stream(
                    spec, request, channel, stream_id=stream_id
                )
            with self.registry_lock:
                state = self._streams[stream_id]
                row = replace(state.row, host=opened.host, path=opened.path)
                self._streams[stream_id] = replace(state, handle=opened, row=row)
                stored = True  # from here on, releasing the opener's hold releases the stream
                self.diagnostics.write(row)
                closed = self._closed
        except BaseException:
            if opened is not None and not stored:  # the stream never reached the registry
                with entered(self.exec_ctx):
                    opened.scope.release()
            raise
        if closed:
            logger.debug("%s: releasing a stream opened during close()", self.label)
            raise MassiveError(f"{self.label} was closed while a stream was opening")
        return opened

    def _release_hold(self, stream_id: int) -> None:
        """Drop one hold on a stream; at zero, unmap its key and release its stream scope.

        Runs on the reaper thread when a result is released, or on the caller thread when
        building a result or opening the stream failed. At zero holds a running stream's row
        becomes `RELEASED` (a failed or closed one keeps its state). The record stays in the
        registry until the blink table shuts down; if it already has (`stream_shut_down` ran
        while holds were outstanding) or the stream never opened, the record is deleted now,
        and the drift registry forgets the stream. Releasing the stream scope drops the
        library's reference to the blink table; the table dies, and its shutdown callback
        releases the topics, when nothing else (a derived table, an open panel) refers to it.
        Releasing the scope of a stream that has already shut down is a harmless decrement.

        Args:
            stream_id: The stream whose hold is dropped.
        """
        with self.registry_lock:
            state = self._streams[stream_id]
            held = state.row.held_results - 1
            opened = state.handle is not None
            released = not held and opened and state.row.state is StreamState.RUNNING
            row = replace(
                state.row,
                held_results=held,
                state=StreamState.RELEASED if released else state.row.state,
            )
            if not held and self._stream_ids.get(state.key) == stream_id:
                # No new verb may reuse a stream the library no longer holds. The key may
                # already serve a newer stream (after `fail_stream`); then it stays.
                del self._stream_ids[state.key]
            if not held and (state.shut_down or not opened):
                del self._streams[stream_id]
                self.drift.forget(stream_id)
            else:
                self._streams[stream_id] = replace(state, row=row)
            if opened:
                self.diagnostics.write(row)
        if not held and state.handle is not None:
            with entered(self.exec_ctx):
                state.handle.scope.release()

    def fail_stream(self, stream_id: int, error: str) -> None:
        """Record that a stream failed and stop reusing it for new verbs.

        Called by the transport just before it fails the stream's table (`publish_failure`),
        so a verb that finds the table failed always finds the failure recorded. The stream's
        row becomes `FAILED` with `error`, and its key is unmapped, so the next verb
        with the same key opens a fresh stream instead of returning the dead table. A
        registered `poll()` table's row becomes `FAILED` the same way. The failed
        stream's record stays in the registry until its holds are released and its blink table
        has shut down.

        A failure is shown whenever the record still exists: while the stream is opening (the
        `FAILED` row is written when the open returns, and none if the open raises), after the
        library's last result was released while a derived table or an open panel keeps the
        publisher alive (the row moves from
        `RELEASED` to `FAILED`), and after the blink table shut down while results are still
        held. A failure for a stream that has left the registry is only logged at DEBUG, since
        no table can show it.

        Args:
            stream_id: The failed stream.
            error: The failure text for the `Error` column.
        """
        with self.registry_lock:
            state = self._streams.get(stream_id)
            polled = state is None and self._update_poll(
                stream_id, functools.partial(replace, state=StreamState.FAILED, error=error)
            )
            if state is not None:
                row = replace(state.row, state=StreamState.FAILED, error=error)
                self._streams[stream_id] = replace(state, row=row)
                if self._stream_ids.get(state.key) == stream_id:
                    del self._stream_ids[state.key]
                if state.handle is not None:  # an opening stream's row is written by _open
                    self.diagnostics.write(row)
        if state is None and not polled:
            logger.debug(
                "%s: stream %d failed after it left the registry: %s", self.label, stream_id, error
            )

    def next_stream_id(self) -> int:
        """Return a new process-unique stream id, the `StreamId` of an `m.streams()` row.

        The one allocator of stream ids: WebSocket streams get theirs when the session
        registers them, and the REST transport calls this for each `poll()` table's row, so
        the two kinds never collide. Thread-safe; takes only a leaf lock of its own, so it may
        be called with or without `registry_lock` held.

        Returns:
            The id, numbered from 1 within the process.
        """
        return _next_stream_id()

    def update_streams(self, changes: Mapping[int, StreamUpdate]) -> None:
        """Apply row changes (row counts, deferrals, hosts, request counts) to `m.streams()`.

        The WebSocket transport's flush thread calls this once per flush, after its
        `publisher.add` calls, never under any other lock; at the default `flush_interval` that
        is about 20 times a second per connection. A `poll()` table's poller calls it once per
        cycle. One short critical section under `registry_lock`: every listed stream still in
        the registry (opening, running, released, failed, or shut down but still held) and every
        registered `poll()` table gets its row updated, and written once the stream has opened
        (an opening stream's row is first written when its open returns, so an open that raises
        leaves no row behind); an id that has left the registry is ignored. No Deephaven call,
        no wait. It never changes `State`.

        Args:
            changes: The update of each stream, by stream id.
        """
        with self.registry_lock:
            for stream_id, update in changes.items():
                state = self._streams.get(stream_id)
                if state is None:
                    self._update_poll(stream_id, update.apply)
                    continue
                row = update.apply(state.row)
                self._streams[stream_id] = replace(state, row=row)
                if state.handle is not None:  # an opening stream's row is written by _open
                    self.diagnostics.write(row)

    def record_stream_drift(self, stream_id: int, prefix: str, drift: DriftReport) -> None:
        """Add a payload's drift to a stream's `SchemaDrift` and write its row if it changed.

        The REST poller and the WebSocket flush thread call this for each payload a stream receives,
        after reporting the drift's WARNINGs with `DriftRegistry.report` outside every lock; a
        registered `poll()` table is recorded the same way. Recording the drift and writing the row
        happen in one `registry_lock` section, so the row always shows the latest text however many
        threads report for the stream. An opening stream's row is first written when its open
        returns; an id that has left the registry is ignored, so no drift is kept for a stream that
        is gone. No Deephaven call, no wait.

        Args:
            stream_id: The stream whose payload drifted.
            prefix: The transport's value (`"WS"`, `"REST"`), the entries' prefix.
            drift: The payload's drift.
        """
        with self.registry_lock:
            state = self._streams.get(stream_id)
            if state is None:
                if stream_id in self._polls:
                    text = self.drift.record(stream_id, prefix, drift)
                    if text is not None:
                        self._update_poll(stream_id, functools.partial(replace, schema_drift=text))
                return
            text = self.drift.record(stream_id, prefix, drift)
            if text is None:
                return
            row = replace(state.row, schema_drift=text)
            self._streams[stream_id] = replace(state, row=row)
            if state.handle is not None:  # an opening stream's row is written by _open
                self.diagnostics.write(row)

    def register_poll(self, row: StreamRow) -> None:
        """Register a new `poll()` table's row in `m.streams()`, before its poller starts.

        A `poll()` table is no shared stream: it has no key, is never reused, and its one
        result is its only hold. Its row stays in the registry, and takes the poller's updates
        (`update_streams`, `fail_stream`, `record_stream_drift`), until `release_poll`. One
        short critical section under `registry_lock`; the row is queued for the diagnostics
        writer.

        Args:
            row: The table's first row: its `StreamId` (from `next_stream_id`), its dataset and
                symbols, one held result, `Transport=REST`, and its host and path.

        Raises:
            MassiveError: If the session is closed (`"MassiveSession #1 is closed"`): the table
                must not start a poller then.
        """
        with self.registry_lock:
            if self._closed:
                raise MassiveError(f"{self.label} is closed")
            self._polls[row.stream_id] = row
            self.diagnostics.write(row)

    def release_poll(self, stream_id: int) -> None:
        """Record that a `poll()` table was released: its row leaves the registry.

        Its last row is written with no held result and `State=RELEASED`, unless it failed or
        the session closed, which keep their state; the drift registry forgets it. Runs on the
        reaper thread when the result is released, or on the caller thread when `poll()` fails
        after registering the row; each table is released once. No Deephaven call, no wait.

        Args:
            stream_id: The table's `StreamId`.
        """
        with self.registry_lock:
            row = self._polls.pop(stream_id)
            self.drift.forget(stream_id)
            released = row.state is StreamState.RUNNING
            last = replace(
                row, held_results=0, state=StreamState.RELEASED if released else row.state
            )
            self.diagnostics.write(last)

    def _update_poll(self, stream_id: int, change: Callable[[StreamRow], StreamRow]) -> bool:
        """Change a registered `poll()` table's row and write it; `registry_lock` is held.

        Args:
            stream_id: The table's `StreamId`.
            change: Returns the new row from the current one.

        Returns:
            True if the id is a registered `poll()` table.
        """
        row = self._polls.get(stream_id)
        if row is None:
            return False
        self._polls[stream_id] = row = change(row)
        self.diagnostics.write(row)
        return True

    def stream_shut_down(self, stream_id: int) -> None:
        """Record that a stream's blink table has shut down.

        The WebSocket transport calls this first from the blink table's `on_shutdown_callback`,
        before it releases the stream's topics. That callback runs on whichever thread drops
        the last reference to the table: the reaper, an update-graph thread (`publish_failure`
        shuts the table down at once), or a gRPC thread. The stream's key is unmapped, so no
        new verb reuses the dead table. With no holds left the record is deleted and the drift
        registry forgets the stream; with holds outstanding (a failure shut the table down while
        results are still held) the record is marked shut down and deleted when the last hold
        is released. It takes only `registry_lock` (and the drift registry's leaf lock),
        makes no Deephaven call, and writes no row: the stream's last row stays in
        `m.streams()`. An unknown id is ignored.

        Args:
            stream_id: The stream whose blink table shut down.
        """
        with self.registry_lock:
            state = self._streams.get(stream_id)
            if state is None:
                return
            if self._stream_ids.get(state.key) == stream_id:
                del self._stream_ids[state.key]
            if state.row.held_results:
                self._streams[stream_id] = replace(state, shut_down=True)
            else:
                del self._streams[stream_id]
                self.drift.forget(stream_id)

    def close(self) -> None:
        """Close the session: run the closers, mark streams closed, release the diagnostics.

        Idempotent. Publishers are not failed: live tables stop receiving rows and go static, and
        keep their contents. Results stay tracked, so garbage collection and `release()` still free
        them. Every stream still in the registry is marked `CLOSED` in `m.streams()`, including a
        released stream that a derived table or an open panel still keeps alive, and so is every
        `poll()` table not yet released. Verbs waiting for a stream that is still opening wake and
        raise `MassiveError`; the opener releases the stream when its open returns. Logs the number
        of live tables that became static: a WARNING when there are any, INFO otherwise.

        Raises:
            ValueError: On an update-graph thread, even when the session is already closed.
        """
        if on_update_graph(self.exec_ctx):
            raise ValueError(ON_UPDATE_GRAPH)
        with self.registry_lock:
            if self._closed:
                return
            self._closed = True
            closers = tuple(self._closers)
            self._closers.clear()  # the collection finalizer must not run them again
            closed = {
                stream_id: replace(state, row=replace(state.row, state=StreamState.CLOSED))
                for stream_id, state in self._streams.items()
            }
            self._streams.update(closed)
            opening = [state.ready for state in closed.values() if state.handle is None]
            for state in closed.values():
                if state.handle is not None:  # an opening stream has no row yet
                    self.diagnostics.write(state.row)
            for stream_id in tuple(self._polls):
                self._update_poll(stream_id, functools.partial(replace, state=StreamState.CLOSED))
        for ready in opening:  # waiting verbs wake and raise MassiveError
            ready.set()
        for closer in closers:
            try:
                closer()
            except Exception:
                logger.exception("%s: a transport failed to close", self.label)
        self.diagnostics.release()
        live = self.results.held()
        level = logging.WARNING if live else logging.INFO
        logger.log(level, "session closed; %d live tables are now static", live)
