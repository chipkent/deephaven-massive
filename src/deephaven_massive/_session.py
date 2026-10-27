"""`MassiveSession`, the user's entry point, and `SessionCore`, its internal engine.

`MassiveSession` is what users create: it exposes the dataset namespaces (`m.stocks`, `m.options`,
...), the diagnostics tables, and the lifecycle verbs `release()` and `close()`, and nothing else.
It delegates everything stateful to a `SessionCore`, which queries and namespaces share. The core
owns:

- the session's label (`MassiveSession #1`, a per-process counter), used in its `repr`, the
  same-key WARNING, and error messages;
- the configuration and the captured Deephaven `ExecutionContext`, entered on every library
  thread;
- `registry_lock`, the one leaf lock that guards every session registry: no Deephaven call, network
  call, or blocking wait happens while it is held;
- the stream registry: every stream from just before it opens until its holds are released
  and its blink table has shut down, each with its diagnostics row (which carries its count of
  library-held results), and the key index that maps each `(dataset, channel, symbols)` to the
  one opening or held, unfailed, live stream that serves it;
  every diagnostics row is queued while `registry_lock` is held, so rows reach the tables in
  the order the registry changed;
- the result registry and the reaper thread (`_liveness.py`);
- the diagnostics tables and their writer thread (`_diagnostics.py`);
- one factory hook per transport (`rest_transport`, `stream_transport`, `flat_file_transport`).
  The flat-file transport is implemented: the core creates the session's
  `_massive.FlatFileClient` (the S3 client, closed with the session) on the first flat-file read,
  so a session that reads no flat file never imports boto3, and hands each `history()` call a
  `FlatFileHistory`. The REST and WebSocket hooks are not implemented yet, so they raise
  `NotImplementedError`, and so do the verbs that need them.

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
from typing import TYPE_CHECKING, Final, Self

from deephaven.execution_context import get_exec_ctx
from deephaven.table import Table
from deephaven.update_graph import has_exclusive_lock, has_shared_lock

from deephaven_massive import _massive
from deephaven_massive._config import MassiveConfig, log_json_codec_once, require_positive_int
from deephaven_massive._diagnostics import (
    Diagnostics,
    StreamRow,
    StreamState,
    StreamUpdate,
    describe_symbols,
)
from deephaven_massive._enums import Transport
from deephaven_massive._errors import MassiveError
from deephaven_massive._flat_files import FlatFileHistory
from deephaven_massive._liveness import Reaper, ResultRegistry, entered, make_result
from deephaven_massive._namespaces.base import list_params
from deephaven_massive._namespaces.crypto import _Crypto
from deephaven_massive._namespaces.economy import _Economy
from deephaven_massive._namespaces.forex import _Forex
from deephaven_massive._namespaces.futures import _Futures
from deephaven_massive._namespaces.indicators import _Indicators
from deephaven_massive._namespaces.indices import _Indices
from deephaven_massive._namespaces.options import _Options
from deephaven_massive._namespaces.partners import _Partners
from deephaven_massive._namespaces.reference import _Reference
from deephaven_massive._namespaces.stocks import _Stocks
from deephaven_massive._types import ALL, override

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping
    from types import TracebackType

    from deephaven.execution_context import ExecutionContext
    from deephaven.liveness_scope import LivenessScope

    from deephaven_massive._catalog import DatasetSpec
    from deephaven_massive._enums import Order
    from deephaven_massive._liveness import StreamHandle
    from deephaven_massive._massive import FlatFileClient
    from deephaven_massive._query import (
        FlatFileTransport,
        Query,
        Request,
        RestTransport,
        StreamKey,
        StreamTransport,
    )
    from deephaven_massive._types import FilterValue, Symbols

__all__ = ["MassiveSession", "SessionCore"]

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

_keys_lock = threading.Lock()
_session_numbers = itertools.count(1)
# Stream ids are minted here, not by the transport, so a stream is registered before it opens.
# Its own leaf lock: `_keys_lock` is taken before a session's `registry_lock` (in `_register`),
# and ids are minted under `registry_lock`, so sharing `_keys_lock` would invert that order.
_stream_id_lock = threading.Lock()
_stream_numbers = itertools.count(1)
# Plain weak references without callbacks, pruned only in `_register` under `_keys_lock`. A
# WeakSet removes entries from garbage-collection callbacks, which run on any thread at any
# allocation and could change the set while `_register` iterates it.
_open_sessions: list[weakref.ref[SessionCore]] = []


def _has_execution_context(exec_ctx: ExecutionContext) -> bool:
    """Tell whether an execution context is a real one, not the thread's empty default.

    Args:
        exec_ctx: The context `get_exec_ctx()` returned.

    Returns:
        True if live tables can be created under it.
    """
    j_update_graph = exec_ctx.update_graph.j_update_graph
    return str(j_update_graph.getClass().getSimpleName()) != _POISONED_UPDATE_GRAPH


def _has_script_query_scope(exec_ctx: ExecutionContext) -> bool:
    """Tell whether formulas under an execution context can call the library's Python functions.

    Args:
        exec_ctx: The context `get_exec_ctx()` returned.

    Returns:
        True when the context's query scope is the Python script session's own scope, False
        for a frozen copy (`make_user_exec_ctx(freeze_vars=[...])`).
    """
    j_query_scope = exec_ctx.j_exec_ctx.getQueryScope()
    return str(j_query_scope.getClass().getName()) == _SCRIPT_QUERY_SCOPE


def _on_update_graph(exec_ctx: ExecutionContext) -> bool:
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


_ON_UPDATE_GRAPH: Final = (
    "deephaven_massive calls must not run on a Deephaven update-graph thread "
    "(for example inside a table listener) or while holding the update-graph lock"
)
"""The `ValueError` text for a call on an update-graph thread: verbs create tables and take the
registry lock, which would deadlock against the update cycle there."""


def _next_label() -> str:
    """Return the next session label of the process.

    Returns:
        `"MassiveSession #<n>"`, numbered from 1.
    """
    with _keys_lock:
        return f"MassiveSession #{next(_session_numbers)}"


def _next_stream_id() -> int:
    """Return the next stream id of the process.

    Returns:
        A process-unique id, numbered from 1 (the `StreamId` of `m.streams()`).
    """
    with _stream_id_lock:
        return next(_stream_numbers)


def _register(core: SessionCore) -> None:
    """Record a new open session and warn if another open session uses the same key.

    Individual Massive plans allow one WebSocket connection per cluster per account, so a
    second session with the same key would make the two compete for connection slots. Sessions
    are held weakly; closed and collected ones do not count.

    Args:
        core: The new session's core.
    """
    with _keys_lock:
        alive = [other for other in (ref() for ref in _open_sessions) if other is not None]
        _open_sessions[:] = [weakref.ref(other) for other in alive]
        _open_sessions.append(weakref.ref(core))
        others = sorted(
            other.label
            for other in alive
            if other.config.api_key == core.config.api_key and not other.closed
        )
    if others:
        logger.warning(
            "%s uses the same API key as %s; individual plans allow one WebSocket connection "
            "per cluster per account, so the sessions will compete for connection slots",
            core.label,
            others[0],
        )


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
    active. Thread-safe: every mutable registry is guarded by `registry_lock`.
    """

    def __init__(self, config: MassiveConfig, exec_ctx: ExecutionContext, label: str) -> None:
        """Create the core, its reaper thread, and the diagnostics tables and writer.

        Args:
            config: The validated configuration.
            exec_ctx: The caller's execution context, captured for library threads.
            label: The session's label, such as `"MassiveSession #1"`.
        """
        self.config: Final = config
        self.exec_ctx: Final = exec_ctx
        self.label: Final = label
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
        self._stream_ids: dict[StreamKey, int] = {}
        self._streams: dict[int, _StreamState] = {}
        # Created by the first flat-file read (`flat_file_transport`), under `registry_lock`.
        self._flat_files: FlatFileClient | None = None
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
        if _on_update_graph(self.exec_ctx):
            raise ValueError(_ON_UPDATE_GRAPH)
        if self.closed:
            raise MassiveError(f"{self.label} is closed")

    # --- Transport factory hooks: one per transport, filled by the transport packages. -------

    def rest_transport(self) -> RestTransport:
        """Return the session's REST transport.

        Raises:
            NotImplementedError: Always, until the REST transport is implemented.
        """
        raise NotImplementedError("the REST transport is not implemented yet")

    def stream_transport(self) -> StreamTransport:
        """Return the session's WebSocket transport.

        Raises:
            NotImplementedError: Always, until the WebSocket transport is implemented.
        """
        raise NotImplementedError("the WebSocket transport is not implemented yet")

    def flat_file_transport(self) -> FlatFileTransport:
        """Return a flat-file transport for one `history()` call.

        Each call gets a new, lightweight `FlatFileHistory` bound to this core and to the
        session's `FlatFileClient`. The first call creates that client: it reads
        `_massive.FlatFileClient` outside every lock (the first read imports boto3), creates the
        client under `registry_lock` (the constructor only stores settings), and registers its
        close with `on_close` after releasing the lock, so the client closes with the session,
        or at once if the session closed meanwhile. No call makes a network request: the boto3
        client is created by the first read.

        Returns:
            The transport.

        Raises:
            ValueError: If the configuration has no S3 credentials. `history()` never gets
                here then: its source choice has already required them.
        """
        config = self.config
        access_key_id, secret_access_key = config.s3_access_key_id, config.s3_secret_access_key
        if access_key_id is None or secret_access_key is None:
            raise ValueError(
                "flat files need MassiveConfig s3_access_key_id and s3_secret_access_key"
            )
        factory = _massive.FlatFileClient
        with self.registry_lock:
            client = self._flat_files
            created = client is None
            if client is None:
                # The client holds no reference to the core, so its closer may outlive it.
                client = self._flat_files = factory(
                    access_key_id=access_key_id,
                    secret_access_key=secret_access_key,
                    endpoint_url=config.s3_endpoint_url,
                    bucket=config.s3_bucket,
                    max_pool=config.max_concurrency,
                )
        if created:
            self.on_close(client.close)
        return FlatFileHistory(self, client)

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
        channel: str | None,
        build: Callable[[Table], Table],
        wrap: bool = False,
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
        interrupt while a hold or a scope is being released). A stream that failed while it was
        opening (or between the claim and the build) still serves `blink()`, as a failed table;
        `append_only()`, `ring()` and `last_by()` then raise Deephaven's `DHError` ("Can not listen
        to failed table") while shaping, with the hold released. Either way `m.streams()` shows
        `FAILED`.

        Args:
            spec: The dataset.
            request: The normalized arguments.
            channel: The WebSocket channel.
            build: Turns the stream's blink table into the result.
            wrap: True when `build` returns a new wrapper of the blink table (`blink()`).

        Returns:
            The live result, a distinct Python object per call.
        """
        key: StreamKey = (spec.name, channel, request.symbol_key())
        handle = self._acquire(spec, request, key, channel=channel)
        blink = handle.blink
        scope: LivenessScope | None = None
        try:
            with entered(self.exec_ctx):
                result, scope = make_result(lambda: build(blink), wrap=wrap)
            self.results.track(
                result, scope, functools.partial(self._release_hold, handle.stream_id)
            )
        except BaseException:  # nothing was tracked: the scope and the hold are this call's
            try:
                if scope is not None:
                    with entered(self.exec_ctx):
                        scope.release()
            finally:
                self._release_hold(handle.stream_id)
            raise
        return result

    def _acquire(
        self, spec: DatasetSpec, request: Request, key: StreamKey, *, channel: str | None
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
        self, spec: DatasetSpec, request: Request, *, channel: str | None, stream_id: int
    ) -> StreamHandle:
        """Open the stream of a registered `OPENING` record through the WebSocket transport.

        Runs with no lock held while the transport works. The caller (`_acquire`) sets the
        record's `ready` event afterwards and, if this raises anything, even
        `KeyboardInterrupt`, drops the opener's hold, which deletes a never-opened record and
        unmaps its key; a waiting verb then opens the stream itself. If the interrupt lands
        after the transport returned the stream but before its handle is stored in the
        registry, nothing else knows the stream, so this method releases its scope itself. A
        failure or a shutdown the transport reported during the open has already been
        recorded on the record, and the stream is returned as is: `blink()` returns the failed
        table, and the other shapes raise `DHError` while the caller builds them.

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
        while holds were outstanding) or the stream never opened, the record is deleted now.
        Releasing the stream scope drops the library's reference to the blink table; the table
        dies, and its shutdown callback releases the topics, when nothing else (a derived table,
        an open panel) refers to it. Releasing the scope of a stream that has already shut down
        is a harmless decrement.

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
            else:
                self._streams[stream_id] = replace(state, row=row)
            if opened:
                self.diagnostics.write(row)
        if not held and state.handle is not None:
            with entered(self.exec_ctx):
                state.handle.scope.release()

    def fail_stream(self, stream_id: int, error: str) -> None:
        """Record that a stream failed and stop reusing it for new verbs.

        Called by the transport after it fails the stream's table (`publish_failure`). The
        stream's row becomes `FAILED` with `error`, and its key is unmapped, so the next verb
        with the same key opens a fresh stream instead of returning the dead table. The failed
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
            if state is not None:
                row = replace(state.row, state=StreamState.FAILED, error=error)
                self._streams[stream_id] = replace(state, row=row)
                if self._stream_ids.get(state.key) == stream_id:
                    del self._stream_ids[state.key]
                if state.handle is not None:  # an opening stream's row is written by _open
                    self.diagnostics.write(row)
        if state is None:
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
        """Apply the flush thread's row changes (row counts, deferrals, hosts) to `m.streams()`.

        The WebSocket transport's flush thread calls this once per flush, after its
        `publisher.add` calls, never under any other lock; at the default `flush_interval` that
        is about 20 times a second per connection. One short critical section under
        `registry_lock`: every listed stream still in the registry (opening, running, released,
        failed, or shut down but still held) gets its row updated, and written once the stream
        has opened (an opening stream's row is first written when its open returns, so an open
        that raises leaves no row behind); an id that has left the registry is ignored. No
        Deephaven call, no wait. It never changes `State`.

        Args:
            changes: The update of each stream, by stream id.
        """
        with self.registry_lock:
            for stream_id, update in changes.items():
                state = self._streams.get(stream_id)
                if state is None:
                    continue
                row = update.apply(state.row)
                self._streams[stream_id] = replace(state, row=row)
                if state.handle is not None:  # an opening stream's row is written by _open
                    self.diagnostics.write(row)

    def stream_shut_down(self, stream_id: int) -> None:
        """Record that a stream's blink table has shut down.

        The WebSocket transport calls this first from the blink table's `on_shutdown_callback`,
        before it releases the stream's topics. That callback runs on whichever thread drops
        the last reference to the table: the reaper, an update-graph thread (`publish_failure`
        shuts the table down at once), or a gRPC thread. The stream's key is unmapped, so no
        new verb reuses the dead table. With no holds left the record is deleted; with holds
        outstanding (a failure shut the table down while results are still held) the record is
        marked shut down and deleted when the last hold is released. It takes only
        `registry_lock`, makes no Deephaven call, and writes no row: the stream's last row stays
        in `m.streams()`. An unknown id is ignored.

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

    def close(self) -> None:
        """Close the session: run the closers, mark streams closed, release the diagnostics.

        Idempotent. Publishers are not failed: live tables stop receiving rows and go static,
        and keep their contents. Results stay tracked, so garbage collection and `release()`
        still free them. Every stream still in the registry is marked `CLOSED` in
        `m.streams()`, including a released stream that a derived table or an open panel still
        keeps alive. Verbs waiting for a stream that is still opening wake and raise
        `MassiveError`; the opener releases the stream when its open returns. Logs the number
        of live tables that became static: a WARNING when there are any, INFO otherwise.

        Raises:
            ValueError: On an update-graph thread, even when the session is already closed.
        """
        if _on_update_graph(self.exec_ctx):
            raise ValueError(_ON_UPDATE_GRAPH)
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


class MassiveSession:
    """A connection to Massive for one API key: the namespaces, diagnostics, and lifecycle.

    Create it inside a Deephaven worker (a server console, an application script, or anywhere
    an `ExecutionContext` is active); the library captures that context for its own threads and
    never starts a JVM. Configuration comes only from `MassiveConfig`, and the session does not
    expose it again: keeping the API key out of the session's attributes keeps it one step
    further from console globals that every user of a worker can read.

    Dataset methods hang off the namespaces (`m.stocks.trades(...)`) and return queries; verbs
    on a query return Deephaven tables. Live results are released automatically when garbage
    collected, deterministically with `m.release(table)`, and all at once from the network
    side with `m.close()`. As a context manager the session closes on exit, which suits
    scripts whose results are static.

    The session's label (`MassiveSession #1`, a per-process counter) appears in its `repr`, in
    the WARNING logged when a second open session uses the same key (individual plans allow
    one WebSocket connection per cluster per account), and in error messages. Thread-safe.

    Example:
        ::

            import deephaven_massive as dm

            m = dm.MassiveSession()  # MASSIVE_API_KEY from the environment
            m  # <MassiveSession #1 open>
            m.stocks  # <MassiveSession #1 stocks>, the namespace of stock datasets
            conns = m.connections()  # live diagnostics: connections and host decisions
            streams = m.streams()  # live diagnostics: one row per stream
            m.close()  # live tables go static; verbs now raise MassiveError

            with dm.MassiveSession() as m2:  # closes on exit
                m2.partners.benzinga  # <MassiveSession #2 partners.benzinga>
    """

    __slots__ = (
        "_core",
        "_crypto",
        "_economy",
        "_forex",
        "_futures",
        "_indicators",
        "_indices",
        "_options",
        "_partners",
        "_reference",
        "_stocks",
    )

    def __init__(self, config: MassiveConfig | None = None) -> None:
        """Start a session.

        When `config` is None, `MassiveConfig()` reads the environment and raises its own
        `AuthenticationError` if no API key is configured (it is not repeated under Raises
        because this method does not raise it itself).

        Args:
            config: The configuration; None means `MassiveConfig()`, which reads the API key
                and S3 credentials from the environment.

        Raises:
            TypeError: If `config` is not a `MassiveConfig`.
            MassiveError: If no Deephaven `ExecutionContext` is active on this thread (the
                session must be created inside a Deephaven worker), or if the context's query
                scope is not the Python script session's own, as with
                `make_user_exec_ctx(freeze_vars=[...])`.
            ValueError: If called on an update-graph thread or under the update-graph lock.
        """
        if config is None:
            config = MassiveConfig()
        elif not isinstance(config, MassiveConfig):
            raise TypeError(f"config must be a dm.MassiveConfig, not {type(config).__name__}")
        exec_ctx = get_exec_ctx()
        if not _has_execution_context(exec_ctx):
            raise MassiveError(
                "MassiveSession must be created inside a Deephaven worker: no Deephaven "
                "ExecutionContext is active on this thread. Create it from the Deephaven "
                "console or an application script, or enter a context first "
                "(with deephaven.execution_context.get_exec_ctx(): ...)."
            )
        if _on_update_graph(exec_ctx):
            raise ValueError(_ON_UPDATE_GRAPH)
        if not _has_script_query_scope(exec_ctx):
            raise MassiveError(
                "MassiveSession needs an ExecutionContext whose query scope is the Python script "
                "session's own: the library's formulas call Python functions through it. A "
                "context from make_user_exec_ctx(freeze_vars=[...]) has a frozen copy instead; "
                "create the session from the console or an application script, or use "
                "make_user_exec_ctx() without freeze_vars."
            )
        log_json_codec_once()
        core = SessionCore(config, exec_ctx, _next_label())
        _register(core)
        self._core = core
        self._stocks = _Stocks(core)
        self._options = _Options(core)
        self._futures = _Futures(core)
        self._indices = _Indices(core)
        self._forex = _Forex(core)
        self._crypto = _Crypto(core)
        self._reference = _Reference(core)
        self._indicators = _Indicators(core)
        self._economy = _Economy(core)
        self._partners = _Partners(core)

    @override
    def __repr__(self) -> str:
        """Show the session's label and whether it is open.

        Returns:
            `"<MassiveSession #1 open>"` or `"<MassiveSession #1 closed>"`, or
            `"<MassiveSession (not started)>"` for an object whose `__init__` raised (tracebacks
            show such objects).
        """
        core: SessionCore | None = getattr(self, "_core", None)
        if core is None:  # __init__ raised before the session existed
            return "<MassiveSession (not started)>"
        state = "closed" if core.closed else "open"
        return f"<{core.label} {state}>"

    @property
    def stocks(self) -> _Stocks:
        """US equities: trades, quotes, bars, snapshots, fundamentals, and filings."""
        return self._stocks

    @property
    def options(self) -> _Options:
        """US options: trades, quotes, bars, chains, snapshots, and contracts."""
        return self._options

    @property
    def futures(self) -> _Futures:
        """US futures: trades, quotes, bars, snapshots, and reference data."""
        return self._futures

    @property
    def indices(self) -> _Indices:
        """Indices: values, bars, and snapshots."""
        return self._indices

    @property
    def forex(self) -> _Forex:
        """Foreign exchange: quotes, bars, snapshots, movers, and conversion."""
        return self._forex

    @property
    def crypto(self) -> _Crypto:
        """Crypto pairs: trades, quotes, bars, snapshots, and movers."""
        return self._crypto

    @property
    def reference(self) -> _Reference:
        """Reference data: tickers, news, exchanges, conditions, and market status."""
        return self._reference

    @property
    def indicators(self) -> _Indicators:
        """Technical indicators: SMA, EMA, RSI, and MACD."""
        return self._indicators

    @property
    def economy(self) -> _Economy:
        """US economic data: treasury yields, inflation, labor market, funding conditions."""
        return self._economy

    @property
    def partners(self) -> _Partners:
        """Partner datasets, by vendor: `benzinga`, `etf_global`, `tmx`, `fable`."""
        return self._partners

    def snapshots(
        self,
        symbols: Symbols,
        *,
        sort: str | None = None,
        order: Order | None = None,
        limit: int | None = None,
        filters: Mapping[str, FilterValue] | None = None,
    ) -> Query:
        """Return the unified snapshot (`GET /v3/snapshot`) of tickers of any asset classes.

        Requests carry up to 250 tickers each; rows are the union schema of the unified
        snapshot. `dm.ALL` is not accepted: the endpoint needs tickers, and each class's
        `snapshots(dm.ALL)` covers a whole market. `limit` caps the total number of rows.
        Invalid `sort`, `order`, or `limit` values raise `TypeError` or `ValueError`.

        Args:
            symbols: The tickers, in canonical form with their class prefixes (`AAPL`,
                `O:SPY251219C00600000`, `C:EURUSD`, ...).
            sort: The SDK's sort field, or None.
            order: The sort direction, or None.
            limit: The maximum number of rows, or None for all.
            filters: Further SDK parameters by name, such as `{"type": "stocks"}`.

        Returns:
            A `Query`; call `fetch()` or `poll(interval)` on it.

        Raises:
            ValueError: If `symbols` is `dm.ALL`.
            NotImplementedError: Always, until the unified snapshot dataset is implemented.
        """
        list_params(sort=sort, order=order)
        if limit is not None:
            require_positive_int("limit", limit)
        if symbols is ALL:
            raise ValueError(
                "m.snapshots() needs tickers; use a class's snapshots(dm.ALL), such as "
                "m.stocks.snapshots(dm.ALL), for a whole market"
            )
        del filters  # validated against the dataset's SDK parameters once it is implemented
        raise NotImplementedError("m.snapshots() is not implemented yet")

    def connections(self) -> Table:
        """Return the live diagnostics table of connections and host decisions.

        One row per WebSocket connection plus one `Transport=REST` row per resolved (asset
        class, channel family), with the host, recency, evidence, state, topic counts, and the
        last error. The table is the session's; do not edit it. After `close()` this raises
        `MassiveError` (see `SessionCore.require_open`).

        Returns:
            The `connections` table, keyed by `Transport`, `AssetClass`, and `Family`;
            its columns are `_diagnostics.CONNECTIONS`.
        """
        self._core.require_open()
        return self._core.diagnostics.connections

    def streams(self) -> Table:
        """Return the live diagnostics table of streams and `poll()` tables.

        One row per stream: dataset, symbols, `HeldResults`, deferred option contracts,
        host, rows published, requests per poll cycle, state, and error. After `close()` this
        raises `MassiveError`.

        Returns:
            The `streams` table, keyed by `StreamId`; its columns are
            `_diagnostics.STREAMS`.
        """
        self._core.require_open()
        return self._core.diagnostics.streams

    def release(self, table: Table) -> None:
        """Release one live result now instead of waiting for garbage collection.

        The library's hold on the table is released on the session's reaper thread, right
        away. The table keeps ticking only while something else holds it (a derived table, an
        open UI panel); once nothing does, its stream unsubscribes. Releasing a result twice
        does nothing, and `release()` keeps working after `close()`. Static results
        (`fetch()`, `history()`) are not tracked.

        Args:
            table: A live table returned by a verb of this session.

        Raises:
            TypeError: If `table` is not a Deephaven `Table`.
            ValueError: If `table` is not a live result of this session (checked by identity).
        """
        if not isinstance(table, Table):
            raise TypeError(f"table must be a Deephaven Table, not {type(table).__name__}")
        if not self._core.results.release(table):
            raise ValueError("table is not a live result of this session")

    def close(self) -> None:
        """Close every connection and poller; live tables go static. Idempotent.

        Publishers are not failed, so live tables keep their rows and stop updating;
        `m.streams()` shows every stream `CLOSED` before the diagnostics tables are released.
        "session closed; N live tables are now static" is logged, at WARNING when N > 0. After
        `close()`, verbs, `connections()`, and `streams()` raise
        `MassiveError("MassiveSession #1 is closed")`; building queries, reaching namespaces,
        and `release()` keep working. On an update-graph thread it raises `ValueError`, even
        when the session is already closed.
        """
        self._core.close()

    def __enter__(self) -> Self:
        """Enter a `with` block; the session closes when the block exits.

        Use it for scripts whose results are static: live tables stop updating on exit.

        Returns:
            The session.
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Close the session at the end of a `with` block.

        Args:
            exc_type: The exception type, if the block raised.
            exc: The exception, if the block raised.
            traceback: Its traceback.
        """
        self.close()
