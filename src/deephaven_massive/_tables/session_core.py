"""`SessionCore`, the internal engine of a session: its lifecycle, registries, and close.

`MassiveSession` (in `_session.py`) is what users create; it delegates everything stateful to
one `SessionCore`, which queries and namespaces share. The core owns:

- the session's label (`MassiveSession #1`), used in error messages and log records, and its
  thread prefix (`massive #1`), the start of the name of every thread the session owns, so each
  log line's thread says which session wrote it;
- the configuration, the process's dataset `Catalog`, and the captured Deephaven
  `ExecutionContext`, entered on every library thread;
- `registry_lock`, the one leaf lock that guards every session registry: no Deephaven call, network
  call, or blocking wait happens while it is held; and the session's one closed flag, read
  under it;
- the two registries of `m.streams()` rows: the stream registry (`SharedStreams`,
  `shared_streams.py`), every WebSocket stream from just before it opens until its holds are
  released and its blink table has shut down, one per `(dataset, channel, symbols)` shared by
  every verb that asks for it; and the worker-table registry (`WorkerTables`,
  `worker_tables.py`), every table a library thread fills (a `poll()` table, a
  background read, a followed read) from its creation until its result is released and its
  blink table has shut down. Transports report row changes, failures, and drift by stream id
  to the core (`update_streams`, `fail_stream`, `record_stream_drift`), which applies each to
  whichever registry holds the id, in one `registry_lock` section;
- the latest `m.symbol_reads()` row of every symbol of every followed read, kept for the
  session's life so `close()` can mark the unfinished ones `CLOSED`, and the session's follow
  coordinator (`follow_coordinator.py`), which owns the followed reads' per-symbol state;
- the read pool (`read_pool.py`), at most `max_concurrency` threads that run every background
  and followed read, so the session's downloads share one limit;
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

A session is kept alive by its user's references and by its own live tables, and by nothing
else: the live set (`live_set.py`) holds every core whose stream registry, worker-table
registry, or count of held diagnostics views is non-empty, and everything else refers to the
core weakly. The process-wide stream-id allocator is `stream_ids.py`. The checks of where a call
runs, which every verb and `close()` make first (through `SessionCore.run_verb`), and the way
library threads enter the session's context (`entered`) live in `execution_context.py`.

**The console holds the UG lock.** Deephaven's console runs every command holding the update
graph's exclusive lock, so no update cycle runs until the command returns; a user thread may
hold the shared lock to the same effect. Every call works there, and a call on a thread that
holds the lock never waits for an update cycle or on a thread that may need the lock (the
diagnostics writer, a poller, another caller's stream open, the reaper, whose releases take
the lock): `close()` waits neither for the
diagnostics writer nor for the cycle that applies its final rows, and a verb never waits for
another thread's open of the same stream (`shared_streams.py`). Only update-graph threads
themselves (table listeners) are refused. A verb that runs long there pauses every live table
of the server, which the session reports once (`SessionCore.run_verb`).

Lock order: the update-graph (UG) lock, then `registry_lock`, then the leaf locks of the live
set (`live_set.py`) and the stream-id allocator (`stream_ids.py`).
Every liveness scope is released holding the UG shared lock and outside `registry_lock`
(`release_scope`); a shutdown callback that the release runs takes `registry_lock` under it.
"""

from __future__ import annotations

import functools
import logging
import threading
import time
import weakref
from dataclasses import replace
from typing import TYPE_CHECKING, Final, TypeVar

from deephaven.dherror import DHError

from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._enums import OnError
from deephaven_massive._errors import (
    LIBRARY_BUG,
    ErrorSource,
    MassiveError,
    SessionClosedError,
    library_bug,
    source_of,
)
from deephaven_massive._log_limits import BoundedLog, bounded_list
from deephaven_massive._tables import live_set
from deephaven_massive._tables.diagnostics import (
    Diagnostics,
    DriftRegistry,
    StreamRow,
    StreamState,
    StreamUpdate,
    thread_died,
)
from deephaven_massive._tables.execution_context import (
    entered,
    holds_update_graph_lock,
    refuse_update_graph_thread,
    update_cycle,
    wait_for_update_cycle,
)
from deephaven_massive._tables.follow_coordinator import FollowCoordinator
from deephaven_massive._tables.liveness import (
    Reaper,
    ResultRegistry,
    make_result,
    release_scope,
    weak_owner_callback,
)
from deephaven_massive._tables.read_pool import ReadPool
from deephaven_massive._tables.shared_streams import SharedStreams
from deephaven_massive._tables.stream_ids import next_stream_id
from deephaven_massive._tables.symbol_read_rows import SymbolReadRow, SymbolReadState
from deephaven_massive._tables.worker_tables import WorkerTables

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from deephaven.execution_context import ExecutionContext
    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table

    from deephaven_massive._config import MassiveConfig
    from deephaven_massive._datasets.catalog import Catalog, DatasetSpec
    from deephaven_massive._datasets.schema import Schema
    from deephaven_massive._massive import DriftReport, Transport
    from deephaven_massive._tables.transports import (
        FlatFileTransport,
        IngestPorts,
        RestTransport,
        StreamTransport,
    )

__all__ = [
    "CLOSE_SHOWN_TIMEOUT",
    "PAUSED_TABLES_WARNING_SECONDS",
    "SESSION_WARNINGS",
    "SessionCore",
]

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


_FINAL_SYMBOL_READS: Final = frozenset(
    {
        SymbolReadState.FAILED,
        SymbolReadState.REMOVED,
        SymbolReadState.STOPPED,
        SymbolReadState.CLOSED,
    }
)
"""The `m.symbol_reads()` states a close leaves as they are: the symbol's following has ended."""

CLOSE_SHOWN_TIMEOUT: Final = 30.0
"""The most seconds `close()` waits, on a thread that does not hold the update graph's lock, for
the update cycle that shows the final diagnostics rows; past it, `close()` returns after a
WARNING."""

PAUSED_TABLES_WARNING_SECONDS: Final = 5.0
"""Seconds a verb may run on a thread that holds the update graph's lock before the session
logs, once, that the server's live tables are paused (`SessionCore.run_verb`)."""

SESSION_WARNINGS: Final = 1_000
"""The most distinct once-per-session warnings a session logs (`SessionCore.warnings`).

Far above what a session meets: `history()`'s unfilled-columns warning has at most two forms
per dataset. The bound keeps the remembered set finite all the same."""


def _release_on(reaper: Reaper, exec_ctx: ExecutionContext, scope: LivenessScope) -> None:
    """Queue a scope's release (`release_scope`) on the reaper; `SessionCore.release_later`.

    Args:
        reaper: The session's reaper, which runs the release.
        exec_ctx: The session's execution context, whose update graph's lock the release takes.
        scope: The scope to release.
    """
    reaper.submit(functools.partial(release_scope, exec_ctx, scope))


def _collect_session(
    label: str,
    reaper: Reaper,
    diagnostics: Diagnostics,
    *,
    results: ResultRegistry,
    closers: list[Callable[[], None]],
    follows: FollowCoordinator,
    reads: ReadPool,
) -> None:
    """Finalizer of a session freed without `close()`: clean up, without waiting.

    A freed core has, by the live set's rule, no running stream or poll and no held diagnostics
    view; a table derived from a finished background read may still keep that read's blink
    table, which holds no failure pin and needs nothing from the session. The closers (the
    session's leases on its connections, its REST client) are handed to the reaper, which runs
    them, and the diagnostics writer's release is queued behind them, so the rows they write
    are applied. That job waits for the writer to end, and the reaper's
    retirement is queued after it, so the reaper outlives the writer: a writer that dies hands
    its tables' failure pins and scope to the reaper (`Diagnostics`), which must still be there
    to release them. The reaper itself keeps running while results are still tracked and ends
    once the last one has been collected (`ResultRegistry.retire`). Garbage collection
    can run this on any thread at any allocation, so it takes no lock and makes no Deephaven
    call. The closer list needs no lock here: the core is unreachable, so no other thread can
    reach the list, and `close()` empties it, so a closed session's closers never run twice.

    Args:
        label: The session's label, for the WARNING.
        reaper: The session's reaper.
        diagnostics: The session's diagnostics.
        results: The session's result registry, retired here.
        closers: The session's registered closers; emptied here.
        follows: The session's follow coordinator, whose thread the reaper stops.
        reads: The session's read pool, which the reaper stops.
    """
    pending = tuple(closers)
    closers.clear()
    if pending:
        reaper.submit(functools.partial(_close_collected, label, pending))
    reaper.submit(follows.stop)  # a freed session follows nothing: its results all stopped
    reaper.submit(reads.stop)  # and reads nothing: no result of it is held
    # After the closers, whose rows it still applies; it waits for the writer to end, which
    # never needs the reaper, and the reaper's retirement waits behind it.
    reaper.submit(diagnostics.release)
    results.retire()  # the reaper ends once no result it may still have to release is tracked


def _close_collected(label: str, closers: tuple[Callable[[], None], ...]) -> None:
    """Reaper job of a session collected without `close()`: log it at INFO and run its closers.

    Collection is how a script that never calls `close()` ends its session, so it is no
    problem to act on: the closers release what the session held.

    Args:
        label: The session's label.
        closers: The closers, run in registration order; one that raises is logged at ERROR
            and the rest still run.
    """
    logger.info(
        "%s was garbage-collected without close(); closing its connections and pollers", label
    )
    for closer in closers:
        try:
            closer()
        except Exception:
            logger.exception("%s: a transport failed to close. %s", label, LIBRARY_BUG)


def _unexpected(error: BaseException) -> bool:
    """Tell whether an exception escaping a verb is a bug in the library.

    Args:
        error: What escaped the verb.

    Returns:
        True for an `Exception` that is neither a `MassiveError` nor the `TypeError` or
        `ValueError` of a wrong argument; False for those, and for an interrupt.
    """
    return isinstance(error, Exception) and not isinstance(
        error, MassiveError | TypeError | ValueError
    )


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
        # The reaper comes first: the diagnostics tables' publishers release their scopes on it.
        self._reaper = Reaper(
            exec_ctx,
            f"{thread_prefix} reaper",
            on_death=functools.partial(SessionCore._thread_died, me, "reaper"),
        )
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
        try:
            self.diagnostics: Final = Diagnostics(
                exec_ctx,
                f"{thread_prefix} diagnostics",
                label=label,
                release_later=self.release_later,
                on_death=functools.partial(SessionCore._thread_died, me, "diagnostics writer"),
            )
        except BaseException:  # the reaper is running: stop it, or its thread runs forever
            self._reaper.stop()
            raise
        self.drift: Final = DriftRegistry()
        # Owns every followed read's per-symbol state; starts its thread with the first one.
        # Runs every background and followed read; starts its threads with the first one.
        self.reads: Final = ReadPool(thread_prefix, config.max_concurrency)
        self.follows: Final = FollowCoordinator(exec_ctx, thread_prefix, self.reads)
        # The session's WebSocket streams, and its `poll()` tables and background and followed
        # reads: the two registries of `m.streams()` rows, guarded by `registry_lock`.
        self.streams: Final = SharedStreams(self)
        self.workers: Final = WorkerTables(self)
        # The latest `m.symbol_reads()` row of each (followed read, symbol) still following:
        # what `close()` marks `CLOSED`. A row in a final state leaves it, so it holds no more
        # than the symbols the session's running followed reads follow. Guarded by
        # `registry_lock`.
        self._symbol_reads: dict[tuple[int, str], SymbolReadRow] = {}
        # How many diagnostics views (`diagnostics_view`) users hold.
        self._views = 0
        # Set once by `attach`, right after construction and before any verb can run.
        self._ports: IngestPorts | None = None
        # Shared with the finalizer, which runs the closers of a session collected unclosed.
        self._closers: list[Callable[[], None]] = []
        self._closed = False
        # The update cycle during which a long verb under the update graph's lock was last
        # reported, so each pause of the live tables is reported once; None before any.
        self._paused_cycle_warned: int | None = None
        # The session's once-per-session warnings, such as history()'s unfilled columns.
        self.warnings: Final = BoundedLog(logger, limit=SESSION_WARNINGS, level=logging.WARNING)
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
            follows=self.follows,
            reads=self.reads,
        )
        finalizer.atexit = False

    def dataset_schema(self, spec: DatasetSpec) -> Schema:
        """Return a dataset's schema in this session: `Catalog.dataset_schema` with `RawSymbol`.

        Every result, `columns()` table, and transport plan of the session reads its columns
        here, so they agree on whether the `RawSymbol` column exists: it does when the
        session's `unreadable_symbols` is `LOG` (the default) or `IGNORE` and the dataset reads
        its symbol from rows.

        Args:
            spec: The dataset.

        Returns:
            The cached schema. A bad dataset raises what `Catalog.dataset_schema` raises.
        """
        return self.catalog.dataset_schema(
            spec, raw_symbol=self.config.unreadable_symbols is not OnError.RAISE
        )

    @property
    def closed(self) -> bool:
        """True once `close()` has run.

        The session's one closed flag. The registries read it with `registry_lock` held, so a
        registration and the close's marking of every row never interleave.
        """
        with self.registry_lock:
            return self._closed

    @property
    def views_held(self) -> int:
        """How many diagnostics views (`diagnostics_view`) users hold; `registry_lock` is held.

        A held view keeps the session in the live set (`live_set.sync`), so it stays current.
        """
        return self._views

    def run_verb(self, call: str, body: Callable[[], _T]) -> _T:
        """Run a verb: its guard first, then its body, timed when it holds the UG lock.

        The guard checks the thread first, so a verb on an update-graph thread raises the
        `ValueError` of `refuse_update_graph_thread` whether or not the session is closed, and
        then the session, before the body checks a single argument. A thread that holds the
        update graph's lock, such as a console command's, passes, and the body is timed there:
        while it runs no live table of the server updates, so a verb that ran
        `PAUSED_TABLES_WARNING_SECONDS` or longer, such as a `history()` of many requests on a
        rate-limited key, logs one WARNING saying so, whether it returned or raised: once per
        pause, so one console command of several long verbs warns once, and the next command
        that pauses the tables again warns again. Any exception that escapes the verb, its
        guard's included, is logged at DEBUG with the verb and re-raised, except an unexpected
        one: an `Exception` that is neither a `MassiveError` nor the `TypeError` or
        `ValueError` of a wrong argument is a bug in the library, logged at ERROR with its
        traceback and raised as a `MassiveError` (`ErrorSource.LIBRARY`) chained to it.

        Args:
            call: The verb, as the warning names it, such as
                `"m.stocks.bars('AAPL', dm.Timespan.DAY).history()"`.
            body: The verb's work.

        Returns:
            What `body` returned.

        Raises:
            SessionClosedError: If the session is closed.
            MassiveError: If the verb failed on an unexpected exception: a bug in the library,
                whose message says how to report it, chained to the exception.
        """
        try:
            return self._guarded_verb(call, body)
        except BaseException as error:
            if not _unexpected(error):
                # The caller receives the error; the log keeps which verb raised it, and why.
                logger.debug("%s raised %s: %s", call, type(error).__name__, error)
                raise
            logger.exception("%s failed on an unexpected exception. %s", call, LIBRARY_BUG)
            raise MassiveError(
                library_bug(f"{call} failed: {error!r}"), source=ErrorSource.LIBRARY
            ) from error

    def _guarded_verb(self, call: str, body: Callable[[], _T]) -> _T:
        """Run a verb's guard and body, timing it under the UG lock (`run_verb`).

        Args:
            call: The verb, as the warning names it.
            body: The verb's work.

        Returns:
            What `body` returned.

        Raises:
            SessionClosedError: If the session is closed.
        """
        refuse_update_graph_thread(self.exec_ctx)
        if self.closed:
            raise SessionClosedError(f"{self.label} is closed")
        if not holds_update_graph_lock(self.exec_ctx):
            return body()
        began = time.monotonic()
        try:
            return body()
        finally:
            self._warn_paused_tables(call, time.monotonic() - began)

    def _warn_paused_tables(self, call: str, seconds: float) -> None:
        """Warn, once per pause of the live tables, that a verb held the update graph's lock long.

        A pause lasts while no update cycle runs: a console command holds the lock from its
        first line to its last, so its verbs share one pause, and two commands with no cycle
        between them share one too. The update cycle's number (`update_cycle`) tells them
        apart. A later verb of the same pause logs at DEBUG.

        Args:
            call: The verb, as the warning names it.
            seconds: How long it ran; under `PAUSED_TABLES_WARNING_SECONDS` logs nothing.
        """
        if seconds < PAUSED_TABLES_WARNING_SECONDS:
            return
        cycle = update_cycle(self.exec_ctx)  # a Java call: before the lock
        with self.registry_lock:
            first = self._paused_cycle_warned != cycle
            self._paused_cycle_warned = cycle
        logger.log(
            logging.WARNING if first else logging.DEBUG,
            "%s: %s ran for %.0f s inside a Deephaven console command, which holds the "
            "update graph's lock: live tables do not update until the command ends; leave "
            "out wait=True to let fetch() or history() read in the background",
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

    def read_stop(self) -> threading.Event:
        """Return a new stop token for one background read (`IngestPorts.read_stop`).

        The transports' `fetch` and `history` take it as `stop`; setting it cancels the read.
        Without attached ports this raises the `RuntimeError` of `_attached_ports`.

        Returns:
            The unset token.
        """
        return self._attached_ports().read_stop()

    # --- Closers, and the reports transports make by stream id to either registry. -------

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

    def fail_stream(self, stream_id: int, error: MassiveError) -> str | None:
        """Record that a stream failed and stop reusing it for new verbs.

        Called by the transport just before it fails the stream's table (`publish_failure`),
        so a verb that finds the table failed always finds the failure recorded. The stream's
        row becomes `FAILED` with `error`, and its key is unmapped, so the next
        verb with the same key opens a fresh stream instead of returning the dead table. A
        registered worker table's row (a `poll()` table's or a background read's) becomes
        `FAILED` the same way. A `CLOSED` stream or worker table is left as it is, and no row is
        written: the close came first and is final. The failed
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
            error: The failure; its text fills the `Error` column, and a stream keeps it for a
                later verb on the stream to raise.

        Returns:
            The close's text when the close came first, so the caller fails the table with it
            and the table and its row name the same cause; None when `error` was recorded (or
            only logged).
        """
        with self.registry_lock:
            current = self.streams.row(stream_id)
            if current is None:
                current = self.workers.row(stream_id)
            if current is not None and current.state is StreamState.CLOSED:
                return current.error
            recorded = self.streams.record_failure(stream_id, error) or self.workers.change_row(
                stream_id, functools.partial(replace, state=StreamState.FAILED, error=str(error))
            )
        if not recorded:
            logger.debug(
                "%s: stream %d failed after it left the registry: %s", self.label, stream_id, error
            )
        return None

    def next_stream_id(self) -> int:
        """Return a new process-unique stream id, the `StreamId` of an `m.streams()` row.

        The process's one allocator of stream ids (`stream_ids.next_stream_id`): WebSocket
        streams get theirs when the session registers them, the REST transport calls this for
        each `poll()` table's row, and each static verb (`fetch()`, `history()`) takes one as
        its call id, which its log lines and thread names carry (`call 7`), and a background
        read's row as its `StreamId`, so no two of them ever collide. Thread-safe; takes only a
        leaf lock of its own, so it may be called with or without `registry_lock` held.

        Returns:
            The id, numbered from 1 within the process.
        """
        return next_stream_id()

    def update_streams(self, changes: Mapping[int, StreamUpdate]) -> None:
        """Apply row changes (row counts, deferrals, hosts, request counts) to `m.streams()`.

        The WebSocket transport's flush thread calls this once per flush, after its
        `publisher.add` calls, never under any other lock; at the default `flush_interval` that
        is about 20 times a second per connection. A `poll()` table's poller calls it once per
        cycle; a background read never does (`WorkerTables.finish_read`). One short critical
        section under `registry_lock`: every listed stream still in the registry (opening,
        running, released, failed, or shut down but still held) and every registered worker
        table gets its row updated, and written once the stream has opened (an opening stream's
        row is first written when its open returns, so an open that raises leaves no row
        behind); an id that has left the registry is ignored. No Deephaven call, no wait. It
        never changes `State`.

        Args:
            changes: The update of each stream, by stream id.
        """
        with self.registry_lock:
            for stream_id, update in changes.items():
                if not self.streams.change_row(stream_id, update.apply):
                    self.workers.change_row(stream_id, update.apply)

    def record_stream_drift(self, stream_id: int, transport: Transport, drift: DriftReport) -> None:
        """Add a payload's drift to a stream's `SchemaDrift` and write its row if it changed.

        The REST poller and the WebSocket flush thread call this for each payload a stream receives,
        after reporting the drift's WARNINGs with `DriftRegistry.report` outside every lock; a
        registered worker table is recorded the same way. Recording the drift and writing the row
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
            if stream_id not in self.streams and stream_id not in self.workers:
                return
            text = self.drift.record(stream_id, prefix, drift)
            if text is None:
                return
            change = functools.partial(replace, schema_drift=text)
            if not self.streams.change_row(stream_id, change):
                self.workers.change_row(stream_id, change)

    def write_symbol_reads(self, rows: Sequence[SymbolReadRow]) -> None:
        """Record and queue `m.symbol_reads()` rows, unless the session closed.

        The follow coordinator calls this for every change of a followed read's symbols. The
        row of each unfinished symbol is kept, so `close()` can write it `CLOSED`; a row in a
        final state (`FAILED`, `REMOVED`, `STOPPED`) drops the symbol's kept row, so a stopped
        read keeps none. The close marks the rows and the session closed in one section under
        `registry_lock`, and nothing is written after it, so a read that lands while the
        session closes leaves its row `CLOSED`. No Deephaven call, no wait.

        Args:
            rows: The new rows, each the whole current state of its (read, symbol).
        """
        with self.registry_lock:
            if self._closed:
                return
            for row in rows:
                if row.state in _FINAL_SYMBOL_READS:
                    self._symbol_reads.pop((row.stream_id, row.symbol), None)
                else:
                    self._symbol_reads[row.stream_id, row.symbol] = row
                self.diagnostics.write(row)

    def diagnostics_view(self, table: Table, schema: Schema) -> Table:
        """Return a new view of a diagnostics table, tracked like a live result.

        `m.connections()`, `m.streams()`, `m.rate_limits()`, and `m.symbol_reads()` call this. The
        view is built in its own result scope on the caller thread, inside the session's execution
        context, and counts as a held diagnostics view until it is released or collected, which
        keeps the session in the live set: a view a user holds stays current. After `close()` it
        keeps the diagnostics table, and so its final rows, alive. If building or tracking the view
        raises anything, even `KeyboardInterrupt`, the scope and the count are released first; a
        `DHError` from a `close()` that released the diagnostics tables meanwhile becomes the closed
        session's `MassiveError`, and one from a table that failed because the diagnostics writer
        died (Deephaven's `TableAlreadyFailedException`) a `MassiveError` that says so.

        Args:
            table: `diagnostics.connections`, `diagnostics.streams`,
                `diagnostics.rate_limits`, or `diagnostics.symbol_reads`.
            schema: The table's schema (`CONNECTIONS`, `STREAMS`, `RATE_LIMITS`, or
                `SYMBOL_READS`), whose columns the view lists, in its order.

        Returns:
            The view: the schema's columns of `table`, the same rows.

        Raises:
            SessionClosedError: If the session is closed (`"MassiveSession #1 is closed"`),
                also when the close ran while the view was built.
            MassiveError: If the diagnostics writer died and failed the table
                (`"MassiveSession #1: its diagnostics tables failed when their writer thread died
                (...)"`).
        """
        with self.registry_lock:
            if self._closed:
                raise SessionClosedError(f"{self.label} is closed")
            self._views += 1
            live_set.sync(self)
        scope: LivenessScope | None = None
        try:
            columns = [column.name for column in schema.columns]
            with entered(self.exec_ctx):
                result, scope = make_result(self.exec_ctx, lambda: table.view(columns))
            self.results.track(result, scope, weak_owner_callback(self, SessionCore._drop_view))
        except BaseException as error:  # nothing was tracked: the scope and the count are ours
            try:
                if scope is not None:
                    release_scope(self.exec_ctx, scope)
            finally:
                self._drop_view()
            died = self.diagnostics.died
            if isinstance(error, DHError) and self.closed:
                raise SessionClosedError(f"{self.label} is closed") from error
            if isinstance(error, DHError) and died is not None:
                logger.debug("%s: a diagnostics view of a failed table was refused", self.label)
                raise MassiveError(
                    f"{self.label}: its diagnostics tables failed when their writer thread died "
                    f"({died!r}); see the ERROR logged then",
                    source=source_of(died),
                ) from error
            raise
        return result

    def _drop_view(self) -> None:
        """Count one diagnostics view less, after it was released or failed to build."""
        with self.registry_lock:
            self._views -= 1
            live_set.sync(self)

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
            SessionCore._fail_live(core, thread_died(core.label, what, death))

    def _fail_live(self, error: MassiveError) -> int:
        """Fail every stream and worker table of the session that is still live.

        For a library thread that is dying (`_thread_died`): without it, the tables it served
        would stop without an error. The tables to fail are collected under `registry_lock`
        and failed outside it, each once (a stream's `StreamHandle.fail`, a worker table's
        `fail` hook). A failure that raises is logged at ERROR and the others still fail.

        Args:
            error: The error every table fails with.

        Returns:
            How many tables were failed.
        """
        with self.registry_lock:
            fails = self._live_fails()
        return len(self._fail_all(fails, error))

    def _live_fails(self) -> list[tuple[int, Callable[[MassiveError], bool]]]:
        """Return the stream id and fail hook of every running table; `registry_lock` is held.

        Returns:
            `(stream id, hook)` of each opened stream and each worker table whose table has not
            shut down and has not failed, closed, or, for a read, got its rows.
        """
        return self.streams.live_fails() + self.workers.live_fails()

    def _fail_all(
        self, fails: Sequence[tuple[int, Callable[[MassiveError], bool]]], error: MassiveError
    ) -> list[int]:
        """Fail tables outside every lock, each once; log a hook that raises and go on.

        Args:
            fails: The tables' stream ids and fail hooks, from `_live_fails`.
            error: The error they fail with.

        Returns:
            The stream ids of the tables that are failed now, in the order given: by this call,
            or by a failure that raced it (which then fails the table with this call's error if
            this call recorded it first).
        """
        failed = []
        for stream_id, fail in fails:
            try:
                fail(error)
            except Exception:
                logger.exception(
                    "%s: could not fail the table of stream %d. %s",
                    self.label,
                    stream_id,
                    LIBRARY_BUG,
                )
            else:
                failed.append(stream_id)
        return failed

    def _close_symbol_reads(self) -> None:
        """Mark every unfinished symbol of a followed read `CLOSED` in `m.symbol_reads()`.

        Called by `close` under `registry_lock`; it only queues rows. Every kept row is
        unfinished (`write_symbol_reads`), and none is kept after it.
        """
        now = time.time_ns()
        for read in self._symbol_reads.values():
            self.diagnostics.write(replace(read, state=SymbolReadState.CLOSED, updated_at=now))
        logger.debug("%s: %d followed symbols marked CLOSED", self.label, len(self._symbol_reads))
        self._symbol_reads.clear()

    def close(self) -> None:
        """Close the session: fail every live table, run the closers, release the diagnostics.

        Idempotent. Under `registry_lock` it only collects: it marks the session closed, takes
        the closers, marks every stream and worker table `CLOSED` in `m.streams()` with the error
        text below (a released stream that a derived table or an open panel still keeps alive
        included; a `FAILED` row, or a read's `DONE` row, keeps its state), marks every
        symbol of a followed read that is still queued, loading, retrying, or done `CLOSED` in
        `m.symbol_reads()`, and gathers those whose tables still run. Then,
        outside every lock, verbs waiting for a stream that is still opening wake and raise
        `MassiveError` (the opener releases the stream when its open returns); every gathered
        table fails with `MassiveError("MassiveSession #1 was closed; this table no longer
        updates")`, and with it every table derived from it (a failed table keeps its rows for
        `snapshot()`, `to_pandas`, and `to_arrow`); the closers run (the session's streams leave
        their connections, connections no other session uses close, and the pollers stop; a
        background read's thread stops with its table's failure, and its requests with the session's
        REST client); the final rows are added and the diagnostics scope released, and `close()`
        returns once an update cycle has taken the rows in, so every diagnostics view, and every
        table derived from one, shows them (after `CLOSE_SHOWN_TIMEOUT` seconds it returns
        anyway, after a WARNING). On a thread that holds the update graph's lock, such as a
        console command's, no cycle can run until the lock is released, so `close()` waits for
        neither the writer nor the cycle: the writer adds the rows and releases the scope, and a
        held view shows them, once the caller releases the lock. There the closers join no thread
        either (`WsConnections.close`): while they would wait no live table updates, and the
        threads, daemons that never need the lock, end on their own. Results stay tracked, so
        garbage collection and `release()` still free them. Logs "MassiveSession #1 closed; N
        live tables failed, streams [...]", with the stream ids of the failed tables
        (`bounded_list`): a WARNING when N > 0, INFO otherwise. On an update-graph thread it
        raises the `ValueError` of `refuse_update_graph_thread`, even when the session is
        already closed.
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
            fails = self._live_fails()  # before any row is marked
            close_row = functools.partial(_closed_row, error=text)
            opening = self.streams.close_rows(close_row)
            self.workers.close_rows(close_row)
            self._close_symbol_reads()
        for ready in opening:  # waiting verbs wake and raise MassiveError
            ready.set()
        failed = self._fail_all(fails, SessionClosedError(text))
        for closer in closers:
            try:
                closer()
            except Exception:
                logger.exception("%s: a transport failed to close. %s", self.label, LIBRARY_BUG)
        self.follows.stop()  # the events queued before it are handled; nothing is joined
        self.reads.stop()  # every read's table has failed: its waiting tasks are dropped
        # The writer releases the diagnostics scope under the update graph's shared lock, which
        # it cannot take while this thread holds the lock: waiting for it there would never end.
        self.diagnostics.release(wait=not locked)
        if not locked:
            # The final rows are added, but they reach the tables, and every view of them,
            # only on the next update cycle: wait for it, so a view read right after close()
            # shows them. Wait even when no view is counted as held: a table derived from a
            # view (`m.streams().where(...)`) outlives the view's wrapper, which is collected
            # at once, and must show the final rows too.
            wait_for_update_cycle(self.exec_ctx, CLOSE_SHOWN_TIMEOUT)
        level = logging.WARNING if failed else logging.INFO
        logger.log(
            level,
            "%s closed; %d live tables failed, streams %s",
            self.label,
            len(failed),
            bounded_list(failed),
        )


def _closed_row(row: StreamRow, error: str) -> StreamRow:
    """Return a row marked `CLOSED` with the close's text, unless it has already ended.

    Args:
        row: The current row.
        error: The close's text.

    Returns:
        The new row; a `FAILED` row keeps its state and its own error, and a background read's
        `DONE` row keeps its state, since its table has every row and is not failed.
    """
    if row.state in (StreamState.FAILED, StreamState.DONE):
        return row
    return replace(row, state=StreamState.CLOSED, error=error)
