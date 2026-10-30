"""`SessionCore`, the internal engine of a session: its registries, live set, and close.

`MassiveSession` (in `_session.py`) is what users create; it delegates everything stateful to
one `SessionCore`, which queries and namespaces share. The core owns:

- the session's label (`MassiveSession #1`), used in error messages and log records, and its
  thread prefix (`massive #1`), the start of the name of every thread the session owns, so each
  log line's thread says which session wrote it;
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
  its result is released and its blink table has shut down;
- the result registry and the reaper thread (`liveness.py`);
- the diagnostics tables and their writer thread, the count of diagnostics views users hold, and
  the drift registry that every transport reports schema drift to (`diagnostics.py`);
- the closers that run when the session closes, or when it is freed without `close()`;
- one factory hook per transport (`rest_transport`, `stream_transport`, `flat_file_transport`),
  each returning one of the ports of `transports.py`: `RestTransport` (`fetch`, `poll`, REST
  `history`, and the WebSocket host decision), `StreamTransport` (opening a WebSocket stream),
  and `FlatFileTransport` (flat-file `history`).

The core holds no wire client and imports no ingest code. The session's ingest side
(`SessionClients` in `_ingest/clients.py`) owns every wire client and connection and provides
the ports through the `IngestPorts` protocol (`transports.py`); `MassiveSession` builds the core,
then the clients, and attaches them once (`attach`), and each hook asks the attached ports.
So the core and everything else on the Deephaven side are tested without a fake server, and a
dependency never points from the tables up to the code that does the I/O.

**The live set.** A session is kept alive by its user's references and by its own live tables,
and by nothing else. The module's live set holds every core whose stream registry, `poll()`
registry, or count of held diagnostics views is non-empty; every method that changes one of
them updates the set in the same `registry_lock` section (`_sync_live`). Everything else in the
library refers to a core weakly (connections, stream sinks, pollers, the clients, and the
owner callbacks of result records), so an unused session is freed by reference counting, at
once, and a session whose stream a derived table keeps running stays alive as long as that
table.
A core that leaves the set is handed to the reaper as a job argument, so its last reference
is not dropped under a lock; the thread that took it out may still drop it last, which is safe
because the core's finalizer only queues work on the reaper. On a free-threaded build, a
reference dropped on another thread reaches the thread that created the core only when that
thread next runs Python code (or a cyclic collection runs), so the core is freed then.

The module also holds the process-wide stream-id allocator. The checks of where a call runs,
which every verb and `close()` make first (through `SessionCore.run_verb`), and the way library
threads enter the session's context (`entered`) live in `execution_context.py`.

**The console holds the UG lock.** Deephaven's console runs every command holding the update
graph's exclusive lock, so no update cycle runs until the command returns; a user thread may
hold the shared lock to the same effect. Every call works there, and a call on a thread that
holds the lock never waits for an update cycle or on a thread that may need the lock (the
diagnostics writer, a poller, another caller's stream open, the reaper, whose releases take
the lock): `close()` waits neither for the
diagnostics writer nor for the cycle that applies its final rows, and a verb never waits for
another thread's open of the same stream (see below). Only update-graph threads themselves
(table listeners) are refused. A verb that runs long there pauses every live table of the
worker, which the session reports once (`SessionCore.run_verb`).

Lock order: the update-graph (UG) lock, then `registry_lock`, then the live set's leaf lock.
Every liveness scope is released holding the UG shared lock and outside `registry_lock`
(`release_scope`); a shutdown callback that the release runs takes `registry_lock` under it. A
stream is registered under `registry_lock` before it opens (with a session-minted id), then
opened with no lock held; a second verb on the same key waits for that open on its own thread,
outside every lock, instead of opening a second stream. A second verb on a thread that holds
the UG lock cannot wait (the open may need that lock, to start a watchlist's listener), so it
takes the key over: it registers and opens a stream of its own, which later verbs on the key
reuse, while the first open finishes for its own caller alone.

The module is one subject although it runs past 1,000 lines: the stream registry, the `poll()`
registry, the count of diagnostics views, the closers, and `close()` all change the same state
under the one `registry_lock` and keep the one live set in step with it, so each of those
critical sections reads whole only here.
"""

from __future__ import annotations

import functools
import itertools
import logging
import threading
import time
import weakref
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final, TypeVar, cast

from deephaven.dherror import DHError
from deephaven.update_graph import shared_lock

from deephaven_massive._datasets.schema import TRANSPORT_LABELS
from deephaven_massive._enums import UnknownSymbols
from deephaven_massive._errors import MassiveError
from deephaven_massive._log_limits import BoundedWarnings
from deephaven_massive._massive import Transport
from deephaven_massive._tables.diagnostics import (
    Diagnostics,
    DriftRegistry,
    StreamRow,
    StreamState,
    StreamUpdate,
    describe_symbols,
    shown_query,
)
from deephaven_massive._tables.execution_context import (
    entered,
    holds_update_graph_lock,
    refuse_update_graph_thread,
)
from deephaven_massive._tables.liveness import Reaper, ResultRegistry, make_result, release_scope

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from deephaven.execution_context import ExecutionContext
    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table

    from deephaven_massive._config import MassiveConfig
    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.request import Request, StreamKey
    from deephaven_massive._datasets.schema import Schema
    from deephaven_massive._massive import DriftReport
    from deephaven_massive._tables.transports import (
        FlatFileTransport,
        IngestPorts,
        RestTransport,
        StreamHandle,
        StreamTransport,
    )

__all__ = ["PAUSED_TABLES_WARNING_SECONDS", "SESSION_WARNINGS", "SessionCore"]

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


# Stream ids are minted here, not by the transport, so a stream is registered before it opens.
# Its own leaf lock: the session module's registration lock is taken before a session's
# `registry_lock`, and ids are minted under `registry_lock`, so sharing that lock would invert
# that order.
_stream_id_lock = threading.Lock()
_stream_numbers = itertools.count(1)

# The live set: every core with a running stream or poll, or a held diagnostics view, by id. It
# is what keeps such a session alive once its user has dropped it. Its lock is a leaf, taken
# only under the core's `registry_lock`.
_live_lock = threading.Lock()
_live: dict[int, SessionCore] = {}


PAUSED_TABLES_WARNING_SECONDS: Final = 5.0
"""Seconds a verb may run on a thread that holds the update graph's lock before the session
logs, once, that the worker's live tables are paused (`SessionCore.run_verb`)."""

SESSION_WARNINGS: Final = 1_000
"""The most distinct once-per-session warnings a session logs (`SessionCore.warnings`).

Far above what a session meets: `history()`'s unfilled-columns warning has at most two forms
per dataset. The bound keeps the remembered set finite all the same."""


def _next_stream_id() -> int:
    """Return the next stream id of the process.

    Returns:
        A process-unique id, numbered from 1 (the `StreamId` of `m.streams()`).
    """
    with _stream_id_lock:
        return next(_stream_numbers)


def _call(core_ref: weakref.ref[SessionCore], release: Callable[..., None], *args: object) -> None:
    """Run a core method through a weak reference: the owner callback of a result record.

    A result record lives as long as its result's Python wrapper, so a strong reference to the
    core there would let a forgotten wrapper keep the session alive, and would make a cycle
    (core, results, record, core). The core is always alive when the record is released: the
    record's unreleased hold (a stream hold, a poll, or a diagnostics view) keeps the core in
    the live set until this callback has run.

    Args:
        core_ref: The session's core, weakly.
        release: The unbound core method, such as `SessionCore._release_hold`.
        *args: Its arguments after the core.
    """
    release(cast("SessionCore", core_ref()), *args)


def _release_on(reaper: Reaper, exec_ctx: ExecutionContext, scope: LivenessScope) -> None:
    """Queue a scope's release (`release_scope`) on the reaper; `SessionCore.release_later`.

    Args:
        reaper: The session's reaper, which runs the release.
        exec_ctx: The session's execution context, whose update graph's lock the release takes.
        scope: The scope to release.
    """
    reaper.submit(functools.partial(release_scope, exec_ctx, scope))


def _let_go(core: SessionCore) -> None:
    """Reaper job that holds a core leaving the live set, so its last reference usually drops there.

    Args:
        core: The core; the job's reference to it ends when the job returns.
    """


def _collect_session(
    label: str,
    reaper: Reaper,
    diagnostics: Diagnostics,
    *,
    results: ResultRegistry,
    closers: list[Callable[[], None]],
) -> None:
    """Finalizer of a session freed without `close()`: clean up, without waiting.

    A freed core has, by the live set's rule, no running stream or poll and no held diagnostics
    view. The closers (the session's leases on its connections, its REST client) are handed to
    the reaper, which runs them, and the diagnostics writer's stop is queued behind them, so the
    rows they write are applied. The reaper itself keeps running while results are still tracked
    and ends once the last one has been collected (`ResultRegistry.retire`). Garbage collection
    can run this on any thread at any allocation, so it takes no lock and makes no Deephaven
    call. The closer list needs no lock here: the core is unreachable, so no other thread can
    reach the list, and `close()` empties it, so a closed session's closers never run twice.

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
    reaper.submit(diagnostics.stop)  # after the closers, whose rows it still applies
    results.retire()  # the reaper ends once no result it may still have to release is tracked


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
class _StreamEntry:
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
    down; `_release_hold`, `fail_stream`, and `stream_shut_down` unmap it. A verb that holds the
    update graph's lock and finds the key's stream still opening takes the key over for a
    record of its own (`_claim`), so the first record then lives and ends unmapped.

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


@dataclass(frozen=True, slots=True)
class _PollState:
    """One `poll()` table of the registry; replaced, never mutated, under `registry_lock`.

    Like a stream's record, it has two clocks: its one result's hold, and its blink table's
    shutdown. Releasing the result marks the row `RELEASED` and leaves the poller running (a
    derived table may still use the table); the shutdown stops the poller. The record leaves
    the registry once both have happened.

    Attributes:
        row: The table's current diagnostics row.
        fail: Stops the poller and fails the table with the error, once; returns False when
            the table had already failed. A Deephaven call: never under `registry_lock`.
        held: True until the result is released.
        shut_down: True once the blink table shut down while the result was held.
    """

    row: StreamRow
    fail: Callable[[MassiveError], bool]
    held: bool = True
    shut_down: bool = False


class SessionCore:
    """The internal engine of one `MassiveSession`, shared by its namespaces and queries.

    Created by `MassiveSession.__init__` on the caller thread, with an execution context
    active, which then attaches the session's ingest ports (`attach`) before any verb can
    run. It lives while its user holds it and while it is in the live set (see the module);
    a core freed without `close()` is cleaned up by its finalizer. Thread-safe: every mutable
    registry is guarded by `registry_lock`.
    """

    def __init__(
        self,
        config: MassiveConfig,
        exec_ctx: ExecutionContext,
        label: str,
        *,
        catalog: Catalog,
        thread_prefix: str,
    ) -> None:
        """Create the core, its reaper, the diagnostics tables and writer, and the drift registry.

        Args:
            config: The validated configuration.
            exec_ctx: The caller's execution context, captured for library threads.
            label: The session's label, such as `"MassiveSession #1"`.
            catalog: Every declared dataset (`CATALOG` in `_session.py`), read by the
                transports.
            thread_prefix: The start of the name of every thread the session owns, such as
                `"massive #1"`; each thread adds its role (`"massive #1 reaper"`,
                `"massive #1 call-7 rest"`).
        """
        self.config: Final = config
        self.exec_ctx: Final = exec_ctx
        self.label: Final = label
        self.thread_prefix: Final = thread_prefix
        self.catalog: Final = catalog
        self.registry_lock: Final = threading.RLock()
        me = weakref.ref(self)
        # The diagnostics come first: creating their tables is the step most likely to fail,
        # and nothing has started a thread yet if it does.
        self.diagnostics: Final = Diagnostics(
            exec_ctx,
            f"{thread_prefix} diagnostics",
            on_death=functools.partial(SessionCore._thread_died, me, "diagnostics writer"),
        )
        try:
            self._reaper = Reaper(
                exec_ctx,
                f"{thread_prefix} reaper",
                on_death=functools.partial(SessionCore._thread_died, me, "reaper"),
            )
        except BaseException:  # the writer is running: stop it, or it holds the tables forever
            self.diagnostics.stop()
            raise
        self.results: Final = ResultRegistry(self.registry_lock, self._reaper, exec_ctx)
        # Queues work on the reaper, after everything queued before it, for callers that must
        # not do it themselves: a listener on an update-graph thread, or a shutdown callback,
        # which must make no Deephaven call. Never blocks, takes no lock, safe from any thread;
        # a job that raises is logged at ERROR.
        self.defer: Final[Callable[[Callable[[], None]], None]] = self._reaper.submit
        # Releases a scope on the reaper, under the update graph's shared lock
        # (`release_scope`), for callers that may not take that lock themselves: a shutdown
        # callback, or an owner holding a lock that a holder of the update graph's lock may
        # wait for (a stream's publish lock). Never blocks, takes no lock, safe from any thread.
        # Like `defer`, it refers to the reaper and the context but not to the core, so a
        # shutdown callback that Java holds can keep it without keeping the session alive.
        self.release_later: Final[Callable[[LivenessScope], None]] = functools.partial(
            _release_on, self._reaper, exec_ctx
        )
        self.drift: Final = DriftRegistry()
        self._stream_ids: dict[StreamKey, int] = {}
        self._streams: dict[int, _StreamEntry] = {}
        # The live `poll()` tables, by stream id; they have no key and no handle.
        self._polls: dict[int, _PollState] = {}
        # How many diagnostics views (`diagnostics_view`) users hold.
        self._views = 0
        # Whether the core is in the live set; changed only by `_sync_live`.
        self._in_live = False
        # Set once by `attach`, right after construction and before any verb can run.
        self._ports: IngestPorts | None = None
        # Shared with the finalizer, which runs the closers of a session collected unclosed.
        self._closers: list[Callable[[], None]] = []
        self._closed = False
        # Whether a long verb under the update graph's lock has been reported.
        self._warned_paused_tables = False
        # The session's once-per-session warnings, such as history()'s unfilled columns.
        self.warnings: Final = BoundedWarnings(logger, limit=SESSION_WARNINGS)
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

    def _sync_live(self) -> None:
        """Put the core in the live set, or take it out, after a registry change.

        Called with `registry_lock` held, by every method that changes the stream registry, the
        `poll()` registry, or the count of diagnostics views. A core leaving the set is handed
        to the reaper (`_let_go`), so its last reference is never dropped under a lock; the
        changing thread may still drop it last once it returns, which is safe because the
        core's finalizer only queues work.
        """
        live = bool(self._streams or self._polls or self._views)
        if live is self._in_live:
            return
        self._in_live = live
        with _live_lock:
            if live:
                _live[id(self)] = self
                return
            del _live[id(self)]
        self._reaper.submit(functools.partial(_let_go, self))

    def run_verb(self, call: str, body: Callable[[], _T]) -> _T:
        """Run a verb: its guard first, then its body, timed when it holds the UG lock.

        The guard checks the thread first, so a verb on an update-graph thread raises the
        `ValueError` of `refuse_update_graph_thread` whether or not the session is closed, and
        then the session, before the body checks a single argument. A thread that holds the
        update graph's lock, such as a console command's, passes, and the body is timed there:
        while it runs no live table of the worker updates, so a verb that ran
        `PAUSED_TABLES_WARNING_SECONDS` or longer, such as a `history()` of many requests on a
        rate-limited key, logs one WARNING saying so, the first time only, whether it returned
        or raised.

        Args:
            call: The verb, as the warning names it, such as `"stocks.bars history()"`.
            body: The verb's work.

        Returns:
            What `body` returned.

        Raises:
            MassiveError: If the session is closed.
        """
        refuse_update_graph_thread(self.exec_ctx)
        if self.closed:
            raise MassiveError(f"{self.label} is closed")
        if not holds_update_graph_lock(self.exec_ctx):
            return body()
        began = time.monotonic()
        try:
            return body()
        finally:
            self._warn_paused_tables(call, time.monotonic() - began)

    def _warn_paused_tables(self, call: str, seconds: float) -> None:
        """Warn, once per session, that a verb held the update graph's lock for a long time.

        Args:
            call: The verb, as the warning names it.
            seconds: How long it ran; under `PAUSED_TABLES_WARNING_SECONDS` logs nothing.
        """
        if seconds < PAUSED_TABLES_WARNING_SECONDS:
            return
        with self.registry_lock:
            first = not self._warned_paused_tables
            self._warned_paused_tables = True
        if first:
            logger.warning(
                "%s: %s ran for %.0f s inside a Deephaven console command, which holds the "
                "update graph's lock: live tables do not update until the command ends; run "
                "long downloads from a thread of your own, outside a console command (logged "
                "once per session)",
                self.label,
                call,
                seconds,
            )

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

        If the session is already closed (a transport that passed the closed check just before
        `close()` ran), the closer runs at once, on this thread and outside the lock, so it is
        still cleaned up. If the session is freed without `close()`, its closers run on the
        reaper thread after a WARNING. Either way each closer runs exactly once.

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
        query: str,
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
        `MassiveError`. Either way `m.streams()` shows `FAILED`. A session that closes while
        the result is built is checked once more after the result is tracked: the result is
        released and the closed session's `MassiveError` raised, so a close that started before
        that check never leaves the verb returning a table about to fail; a close that starts
        after it fails the returned table like any other.

        The result record's owner callback holds the core weakly (`_call`); the stream's
        record, and through it the live set, keeps the session alive while the stream runs.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            channel: The WebSocket channel.
            query: The `repr` of the query whose verb asks, for the `Query` column of
                `m.streams()`; a reused stream keeps the text of the query that opened it.
            build: Turns the stream's blink table into the result. It must create a new
                table, never return the blink table itself, so the result scope manages the
                result and each call returns a distinct table.

        Returns:
            The live result, a distinct Python object per call.

        Raises:
            MassiveError: If the stream has failed (`"stocks.trades stream 7 failed: ..."`).
        """
        key: StreamKey = (spec.name, channel, request.symbol_key())
        handle = self._acquire(spec, request, key, channel=channel, query=query)
        blink = handle.blink
        scope: LivenessScope | None = None
        try:
            self._require_unfailed(spec, handle.stream_id)
            with entered(self.exec_ctx):
                result, scope = make_result(self.exec_ctx, lambda: build(blink))
            self.results.track(
                result,
                scope,
                functools.partial(
                    _call, weakref.ref(self), SessionCore._release_hold, handle.stream_id
                ),
            )
        except BaseException as error:  # nothing was tracked: the scope and the hold are ours
            # Read first: releasing the last hold of a shut-down stream deletes its record.
            # A failed table fails a shape with `DHError`, or with jpy's `RuntimeError` when the
            # Java call is direct (the described copy every result ends with).
            deephaven = isinstance(error, DHError | RuntimeError)
            failure = self._failure(handle.stream_id) if deephaven else None
            try:
                if scope is not None:
                    release_scope(self.exec_ctx, scope)
            finally:
                self._release_hold(handle.stream_id)
            if failure is not None:
                raise MassiveError(
                    f"{spec.name} stream {handle.stream_id} failed: {failure}"
                ) from error
            raise
        if self.closed:  # close() ran while the result was built; it is released on the reaper
            self.results.release(result)
            raise MassiveError(f"{self.label} is closed")
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
            Its `Error` text when its state is `FAILED` or `CLOSED` (the close's text); None
            otherwise.
        """
        with self.registry_lock:
            state = self._streams.get(stream_id)
            if state is None or state.row.state not in {StreamState.FAILED, StreamState.CLOSED}:
                return None
            return state.row.error or "failed"

    def _acquire(
        self, spec: DatasetSpec, request: Request, key: StreamKey, *, channel: str, query: str
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

        A caller that holds the update graph's lock never waits for another thread's open, which
        may need that lock to finish (starting a watchlist's listener does): it takes the key
        over and opens a stream of its own (`_claim`). The lock is checked once, here, outside
        `registry_lock`, since the check is a Java call.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            key: The stream key.
            channel: The WebSocket channel.
            query: The `repr` of the asking query, for a new stream's `Query` column.

        Returns:
            The stream's handle, with one hold taken for the caller.

        A closed session, or one that closes while the stream opens, raises the `MassiveError`
        of `_claim` or `_open`.
        """
        may_wait = not holds_update_graph_lock(self.exec_ctx)
        while True:
            holds: list[tuple[int, threading.Event | None]] = []
            try:
                claim = self._claim(spec, request, key, query=query, may_wait=may_wait, holds=holds)
                if isinstance(claim, threading.Event):
                    claim.wait()  # another thread is opening the stream; never under a lock
                    continue
                if isinstance(claim, int):
                    claim = self._open(spec, request, channel=channel, stream_id=claim)
                    logger.debug(
                        "stream %d opened: %s channel %s on %s%s",
                        claim.stream_id,
                        spec.name,
                        channel,
                        claim.host,
                        claim.path,
                    )
                else:
                    logger.debug(
                        "stream %d reused: %s channel %s", claim.stream_id, spec.name, channel
                    )
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
        query: str,
        may_wait: bool,
        holds: list[tuple[int, threading.Event | None]],
    ) -> StreamHandle | threading.Event | int:
        """Take a hold on the stream with `key`, find its open in progress, or register it.

        One critical section under `registry_lock`; it never waits and makes no Deephaven
        call.

        A caller that may not wait, because it holds the update graph's lock, takes over the key
        of a stream still opening on another thread: it registers a new record and maps the key
        to it. The first stream finishes opening for its own caller and is never reused, and
        the verbs waiting for it wake, claim again, and find the new record. Every unmapping
        checks that the key still maps to the stream being unmapped, so the first stream's end
        leaves the key alone. A take-over is logged at INFO, after the lock is released, so
        the two rows of the key in `m.streams()` are explained.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            key: The stream key.
            query: The `repr` of the asking query, the `Query` of a new record's row.
            may_wait: False when the caller holds the update graph's lock, so it may not wait
                for another thread's open.
            holds: Receives one `(stream_id, ready)` entry per hold this call takes, appended
                inside the lock right after the registry stores it, so the caller cleans up
                exactly what was taken: `(stream_id, None)` for a hold on an open stream it
                reuses, `(stream_id, ready)` for a new `OPENING` record and the opener's hold.
                An interrupt before the stores leaves nothing to clean up.

        Returns:
            The handle of an open stream, with one more hold taken on it; the `ready` event of
            a stream another thread is opening, to wait on before claiming again (only when
            `may_wait`); or the id of a new `OPENING` record registered for this caller, with
            the opener's hold, which the caller must pass to `_open`.

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
                if state.handle is not None:
                    row = replace(state.row, held_results=state.row.held_results + 1)
                    self._streams[existing] = replace(state, row=row)
                    holds.append((existing, None))  # right after the store: the caller cleans up
                    self.diagnostics.write(row)
                    return state.handle
                if may_wait:
                    return state.ready
                # Opening on another thread, which may need the UG lock this caller holds to
                # finish: take the key over below rather than wait.
            stream_id = _next_stream_id()
            count, text = describe_symbols(request.symbol_key())
            row = StreamRow(
                stream_id=stream_id,
                dataset=spec.name,
                query=shown_query(query),
                symbol_count=count,
                symbols=text,
                held_results=1,
                transport=Transport.WS,
                created_at=time.time_ns(),
            )
            ready = threading.Event()
            state = _StreamEntry(key, None, row, ready)
            # The key first: hashing it can run Python (a SymbolTable box), so it may be
            # interrupted; the int-keyed store and the append after it run no Python code.
            # No row is written until the open finishes: m.streams() has no OPENING state.
            self._stream_ids[key] = stream_id
            self._streams[stream_id] = state
            holds.append((stream_id, ready))  # right after the stores: the caller cleans up
            self._sync_live()
        if existing is not None:  # the key was taken over from a stream still opening
            logger.info(
                "%s: %s stream %d opens alongside stream %d for %s, which another thread is "
                "still opening: this call holds the update graph's lock (a console command) "
                "and cannot wait for that open, so m.streams() shows both; later calls reuse "
                "stream %d",
                self.label,
                spec.name,
                stream_id,
                existing,
                text,
                stream_id,
            )
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
        the `MassiveError` naming the failure instead of building a result. A reactive stream
        that shut down during the open has its watchlist listener stopped on the reaper here,
        since the shutdown had no handle to stop it through. The row takes the host and path
        the stream opened on, unless the transport already reported a later host while the
        open was finishing (a real-time verdict moved the stream to another host before the
        open returned): that report is newer, so the row keeps it.

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
                row = state.row
                if row.host is None:  # not moved while opening: it serves from where it opened
                    row = replace(row, host=opened.host, path=opened.path)
                self._streams[stream_id] = replace(state, handle=opened, row=row)
                stored = True  # from here on, releasing the opener's hold releases the stream
                self.diagnostics.write(row)
                if state.shut_down and opened.stop_watch is not None:
                    # The shutdown found no handle to stop the watchlist listener through.
                    self._reaper.submit(opened.stop_watch)  # a Deephaven call: never here
                closed = self._closed
        except BaseException:
            if opened is not None and not stored:  # the stream never reached the registry
                release_scope(self.exec_ctx, opened.scope)
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
        Releasing the scope of a stream that has already shut down is a harmless decrement. The
        scope is released after `registry_lock` is let go, under the update graph's shared lock
        (`release_scope`).

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
            self._sync_live()
        logger.debug(
            "stream %d: a hold released, %d left%s",
            stream_id,
            held,
            "; its stream scope is released" if not held and opened else "",
        )
        if not held and state.handle is not None:
            release_scope(self.exec_ctx, state.handle.scope)

    def fail_stream(self, stream_id: int, error: str) -> str | None:
        """Record that a stream failed and stop reusing it for new verbs.

        Called by the transport just before it fails the stream's table (`publish_failure`),
        so a verb that finds the table failed always finds the failure recorded. The stream's
        row becomes `FAILED` with `error`, and its key is unmapped, so the next
        verb with the same key opens a fresh stream instead of returning the dead table. A
        registered `poll()` table's row becomes `FAILED` the same way. A `CLOSED` stream or poll
        is left as it is, and no row is written: the close came first and is final. The failed
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

        Returns:
            The close's text when the close came first, so the caller fails the table with it
            and the table and its row name the same cause; None when `error` was recorded (or
            only logged).
        """
        with self.registry_lock:
            state = self._streams.get(stream_id)
            poll = self._polls.get(stream_id)
            current = state.row if state is not None else None if poll is None else poll.row
            if current is not None and current.state is StreamState.CLOSED:
                return current.error
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
        return None

    def next_stream_id(self) -> int:
        """Return a new process-unique stream id, the `StreamId` of an `m.streams()` row.

        The one allocator of stream ids: WebSocket streams get theirs when the session
        registers them, the REST transport calls this for each `poll()` table's row, and each
        static verb (`fetch()`, `history()`) takes one as its call id, which its log lines and
        thread names carry (`call 7`), so no two of them ever collide.
        Thread-safe; takes only a leaf lock of its own, so it may
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

    def record_stream_drift(self, stream_id: int, transport: Transport, drift: DriftReport) -> None:
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
            transport: The transport that carried the payload; its user-facing name
                (`TRANSPORT_LABELS`, `"WebSocket"` or `"REST"`) prefixes the entries.
            drift: The payload's drift.
        """
        prefix = TRANSPORT_LABELS[transport]
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

    def register_poll(self, row: StreamRow, *, fail: Callable[[MassiveError], bool]) -> None:
        """Register a new `poll()` table in `m.streams()` and the live set, before it polls.

        A `poll()` table is no shared stream: it has no key, is never reused, and its one
        result is its only hold. Its record stays in the registry, keeping the session alive and
        taking the poller's updates (`update_streams`, `fail_stream`, `record_stream_drift`),
        until its result is released (`release_poll`) and its blink table has shut down
        (`poll_shut_down`), in either order. One short critical section under `registry_lock`;
        the row is queued for the diagnostics writer.

        Args:
            row: The table's first row: its `StreamId` (from `next_stream_id`), its dataset and
                symbols, one held result, `Transport=REST`, and its host and path.
            fail: Stops the poller and fails the table, once (`_PollState.fail`); `close()` and a
                dying library thread call it.

        Raises:
            MassiveError: If the session is closed (`"MassiveSession #1 is closed"`): the table
                must not start a poller then.
        """
        with self.registry_lock:
            if self._closed:
                raise MassiveError(f"{self.label} is closed")
            self._polls[row.stream_id] = _PollState(row, fail)
            self.diagnostics.write(row)
            self._sync_live()

    def release_poll(self, stream_id: int) -> None:
        """Record that a `poll()` table's result was released; the poller keeps running.

        The row is written with no held result and `State=RELEASED`, unless it failed or the
        session closed, which keep their state. A derived table may still use the poll, so the
        poller runs on until the poll's blink table shuts down (`poll_shut_down`); if it already
        has, the record leaves the registry now and the drift registry forgets it. Runs on the
        reaper thread when the result is released, or on the caller thread when `poll()` fails
        after registering the row; each table is released once. No Deephaven call, no wait.

        Args:
            stream_id: The table's `StreamId`.
        """
        with self.registry_lock:
            state = self._polls[stream_id]
            released = state.row.state is StreamState.RUNNING
            row = replace(
                state.row,
                held_results=0,
                state=StreamState.RELEASED if released else state.row.state,
            )
            self.diagnostics.write(row)
            if state.shut_down:
                del self._polls[stream_id]
                self.drift.forget(stream_id)
            else:
                self._polls[stream_id] = replace(state, row=row, held=False)
            self._sync_live()

    def poll_shut_down(self, stream_id: int) -> None:
        """Record that a `poll()` table's blink table has shut down: nothing uses the poll.

        Called from the blink table's `on_shutdown_callback`, after the poller was told to
        stop, on whichever thread dropped the table's last reference. With the result released
        the record leaves the registry (its last row stays in `m.streams()`); while the result
        is still held (a failure shut the table down) it is marked, and the release deletes it.
        It takes only `registry_lock`, makes no Deephaven call, and writes no row. An unknown
        id (a poll whose registration failed) is ignored.

        Args:
            stream_id: The table's `StreamId`.
        """
        with self.registry_lock:
            state = self._polls.get(stream_id)
            if state is None:
                return
            if state.held:
                self._polls[stream_id] = replace(state, shut_down=True)
            else:
                del self._polls[stream_id]
                self.drift.forget(stream_id)
            self._sync_live()

    def _update_poll(self, stream_id: int, change: Callable[[StreamRow], StreamRow]) -> bool:
        """Change a registered `poll()` table's row and write it; `registry_lock` is held.

        Args:
            stream_id: The table's `StreamId`.
            change: Returns the new row from the current one.

        Returns:
            True if the id is a registered `poll()` table.
        """
        state = self._polls.get(stream_id)
        if state is None:
            return False
        row = change(state.row)
        self._polls[stream_id] = replace(state, row=row)
        self.diagnostics.write(row)
        return True

    def diagnostics_view(self, table: Table) -> Table:
        """Return a new view of a diagnostics table, tracked like a live result.

        `m.connections()`, `m.streams()`, and `m.rate_limits()` call this. The view is built in
        its own result scope on the caller thread, inside the session's execution context, and
        counts as a held diagnostics view until it is released or collected, which keeps the
        session in the live set: a view a user holds stays current. After `close()` it keeps
        the diagnostics table, and so its final rows, alive. If building or tracking the view
        raises anything, even `KeyboardInterrupt`, the scope and the count are released first;
        a `DHError` from a `close()` that released the diagnostics tables meanwhile becomes the
        closed session's `MassiveError`.

        Args:
            table: `diagnostics.connections`, `diagnostics.streams`, or
                `diagnostics.rate_limits`.

        Returns:
            The view: every column of `table`, the same rows.

        Raises:
            MassiveError: If the session is closed (`"MassiveSession #1 is closed"`), also when
                the close ran while the view was built.
        """
        with self.registry_lock:
            if self._closed:
                raise MassiveError(f"{self.label} is closed")
            self._views += 1
            self._sync_live()
        scope: LivenessScope | None = None
        try:
            columns = [column.name for column in table.columns]
            with entered(self.exec_ctx):
                result, scope = make_result(self.exec_ctx, lambda: table.view(columns))
            self.results.track(
                result, scope, functools.partial(_call, weakref.ref(self), SessionCore._drop_view)
            )
        except BaseException as error:  # nothing was tracked: the scope and the count are ours
            try:
                if scope is not None:
                    release_scope(self.exec_ctx, scope)
            finally:
                self._drop_view()
            if isinstance(error, DHError) and self.closed:
                raise MassiveError(f"{self.label} is closed") from error
            raise
        return result

    def _drop_view(self) -> None:
        """Count one diagnostics view less, after it was released or failed to build."""
        with self.registry_lock:
            self._views -= 1
            self._sync_live()

    def stream_shut_down(self, stream_id: int) -> None:
        """Record that a stream's blink table has shut down.

        The WebSocket transport calls this first from the blink table's `on_shutdown_callback`,
        before it releases the stream's topics. That callback runs on whichever thread drops
        the last reference to the table: the reaper, an update-graph thread (`publish_failure`
        shuts the table down on the next cycle), a gRPC thread, or Deephaven's liveness-cleanup
        thread. The stream's key is unmapped, so no new verb reuses the dead table, and a
        reactive stream's watchlist listener is stopped on the reaper. With no holds left the
        record is deleted and the drift registry forgets the stream; with holds outstanding (a
        failure shut the table down while results are still held) the record is marked shut
        down and deleted when the last hold is released. It takes only `registry_lock` (and the
        leaf locks of the drift registry and the live set), makes no Deephaven call, and writes
        no row: the stream's last row stays in `m.streams()`. An unknown id is ignored.

        Args:
            stream_id: The stream whose blink table shut down.
        """
        with self.registry_lock:
            state = self._streams.get(stream_id)
            if state is None:
                return
            if self._stream_ids.get(state.key) == stream_id:
                del self._stream_ids[state.key]
            handle = state.handle
            if handle is not None and handle.stop_watch is not None:
                self._reaper.submit(handle.stop_watch)  # a Deephaven call: never here
            if state.row.held_results:
                self._streams[stream_id] = replace(state, shut_down=True)
            else:
                del self._streams[stream_id]
                self.drift.forget(stream_id)
            self._sync_live()

    @staticmethod
    def _thread_died(core_ref: weakref.ref[SessionCore], what: str, death: BaseException) -> None:
        """The `on_death` of a session's reaper or diagnostics writer: fail its live tables.

        Runs on the dying thread. A thread that dies leaves the session's tables frozen (no
        release, or no diagnostics), so they fail instead, naming the thread and its cause. A
        dead reaper can no longer release results, so the session then stays in the live set
        until the process ends; only an interrupt raised inside the reaper kills it.

        Args:
            core_ref: The session's core, weakly: the thread must not keep it alive.
            what: The thread, as the message names it (`"reaper"`).
            death: What ended the thread.
        """
        core = core_ref()
        if core is not None:
            text = f"{core.label}: its {what} thread died ({death!r}); this table no longer updates"
            SessionCore._fail_live(core, MassiveError(text))

    def _fail_live(self, error: MassiveError) -> int:
        """Fail every stream and `poll()` table of the session that has not failed or shut down.

        For a library thread that is dying (`_thread_died`): without it, the tables it served
        would stop without an error. The tables to fail are collected under `registry_lock`
        and failed outside it, each once (`StreamHandle.fail`, `_PollState.fail`). A failure
        that raises is logged at ERROR and the others still fail.

        Args:
            error: The error every table fails with.

        Returns:
            How many tables were failed.
        """
        with self.registry_lock:
            fails = self._live_fails()
        return self._fail_all(fails, error)

    def _live_fails(self) -> list[Callable[[MassiveError], bool]]:
        """Return the fail hooks of every running table; `registry_lock` is held.

        Returns:
            The hook of each opened stream and each poll whose table has not shut down and has
            not failed or closed.
        """
        done = {StreamState.FAILED, StreamState.CLOSED}
        fails = [
            state.handle.fail
            for state in self._streams.values()
            if state.handle is not None and not state.shut_down and state.row.state not in done
        ]
        fails += [
            poll.fail
            for poll in self._polls.values()
            if not poll.shut_down and poll.row.state not in done
        ]
        return fails

    def _fail_all(
        self, fails: Sequence[Callable[[MassiveError], bool]], error: MassiveError
    ) -> int:
        """Fail tables outside every lock, each once; log a hook that raises and go on.

        Args:
            fails: The tables' fail hooks.
            error: The error they fail with.

        Returns:
            How many of the tables are failed now: by this call, or by a failure that raced it
            (which then fails the table with this call's error if this call recorded it first).
        """
        failed = 0
        for fail in fails:
            try:
                fail(error)
            except Exception:
                logger.exception("%s: could not fail a live table", self.label)
            else:
                failed += 1
        return failed

    def close(self) -> None:
        """Close the session: fail every live table, run the closers, release the diagnostics.

        Idempotent. Under `registry_lock` it only collects: it marks the session closed, takes
        the closers, marks every stream and `poll()` table not already failed `CLOSED` in
        `m.streams()` with the error text below (a released stream that a derived table or an
        open panel still keeps alive included), and gathers those whose tables still run. Then,
        outside every lock, verbs waiting for a stream that is still opening wake and raise
        `MassiveError` (the opener releases the stream when its open returns); every gathered
        table fails with `MassiveError("MassiveSession #1 was closed; this table no longer
        updates")`, and with it every table derived from it (a failed table keeps its rows for
        `snapshot()`, `to_pandas`, and `to_arrow`); the closers run (the session's streams leave
        their connections, connections no other session uses close, and the pollers stop); the
        final rows are written and the diagnostics scope released, and `close()` returns once
        the update cycle that applied them has ended, so a held view shows them. On a thread
        that holds the update graph's lock, such as a console command's, no cycle can run until
        the lock is released, so `close()` waits for neither the rows nor the cycle: the
        diagnostics writer applies the rows, and a held view shows them, once the caller
        releases the lock. There the closers join no thread either (`WsConnections.close`):
        while they would wait no live table updates, and the threads, daemons that never need
        the lock, end on their own. Results stay tracked, so garbage collection and `release()`
        still free them. Logs "MassiveSession #1 closed; N live tables failed": a WARNING when
        N > 0, INFO otherwise; the tables themselves are not logged one by one. On an
        update-graph thread it raises the `ValueError` of `refuse_update_graph_thread`, even
        when the session is already closed.
        """
        refuse_update_graph_thread(self.exec_ctx)
        locked = holds_update_graph_lock(self.exec_ctx)
        text = f"{self.label} was closed; this table no longer updates"
        with self.registry_lock:
            if self._closed:
                return
            self._closed = True
            closers = tuple(self._closers)
            self._closers.clear()  # the collection finalizer must not run them again
            fails = self._live_fails()
            opening = []
            for stream_id, state in tuple(self._streams.items()):
                if state.handle is None:
                    opening.append(state.ready)
                closed = _closed_row(state.row, text)
                self._streams[stream_id] = replace(state, row=closed)
                if state.handle is not None:  # an opening stream has no row yet
                    self.diagnostics.write(closed)
            for stream_id in tuple(self._polls):
                self._update_poll(stream_id, functools.partial(_closed_row, error=text))
        for ready in opening:  # waiting verbs wake and raise MassiveError
            ready.set()
        failed = self._fail_all(fails, MassiveError(text))
        for closer in closers:
            try:
                closer()
            except Exception:
                logger.exception("%s: a transport failed to close", self.label)
        # The writer's `InputTable.add` waits for an update cycle, which cannot run while this
        # thread holds the update graph's lock: waiting for the writer there would never end.
        self.diagnostics.release(wait=not locked)
        # The final rows are in the diagnostics tables, but a view of them (a held
        # `m.streams()`) updates later in the update cycle that applied them: wait for that
        # cycle to end, so a view read right after close() shows them. A thread that holds the
        # lock already takes it again at once: no cycle is running, and none has applied the
        # rows yet.
        with shared_lock(self.exec_ctx.update_graph):
            pass
        level = logging.WARNING if failed else logging.INFO
        logger.log(level, "%s closed; %d live tables failed", self.label, failed)


def _closed_row(row: StreamRow, error: str) -> StreamRow:
    """Return a row marked `CLOSED` with the close's text, unless it has already failed.

    Args:
        row: The current row.
        error: The close's text.

    Returns:
        The new row; a `FAILED` row keeps its state and its own error.
    """
    if row.state is StreamState.FAILED:
        return row
    return replace(row, state=StreamState.CLOSED, error=error)
