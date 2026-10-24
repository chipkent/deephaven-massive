"""How live results are kept alive and released: result scopes, records, and the reaper.

In a Deephaven console, a table created by a command is managed by the session's query scope
and lives until the session ends; deleting the Python variable does not release it (DESIGN
§ 6.1). The library therefore never lets a live result fall into that scope. Every live result
is created inside its own library-owned *result scope*, a `LivenessScope` that holds exactly
that one table, and is tracked by a `ResultRecord`:

1. `make_result` opens a fresh scope on the caller thread, builds the table inside it, and, for
   a new wrapper of an existing Java table (`blink()`), manages the wrapper explicitly.
2. `ResultRegistry.track` records the result under the session's registry lock, keyed by
   `id(result)`, with a `weakref.finalize` that fires when the Python wrapper is
   garbage-collected.
3. Release happens exactly once per record, by whichever comes first: the finalizer
   (garbage collection), `ResultRegistry.release` (`m.release(table)`). Each flips the
   record's once-only `released` flag under the registry lock and, if it was the first,
   submits a job to the `Reaper`.
4. The `Reaper`, one daemon thread per session, runs the job: it releases the result scope and
   then calls the record's `on_released` callback, which for a stream decrements the stream's
   held-result count (and at zero releases the stream scope) and for a `poll()` table stops
   its poller. A failing job is logged at ERROR and the thread carries on.

Releasing a result scope drops the library's liveness reference only. A table the user derived
from the result, an open UI panel, or a user liveness scope keeps the Java table alive, and with
it the stream: topic reference counts are bound to the blink table's `on_shutdown_callback`,
never to result records.

Threading: `make_result` runs on the caller thread (liveness scopes are thread-local). The
registry's state is guarded by the session's `registry_lock`, a leaf lock under which no
Deephaven call is made. Scope releases happen on the reaper thread inside the session's
`ExecutionContext`.
"""

from __future__ import annotations

import logging
import queue
import threading
import weakref
from dataclasses import dataclass, field
from itertools import count
from typing import TYPE_CHECKING, Final, Generic, TypeVar

from deephaven.execution_context import ExecutionContext
from deephaven.liveness_scope import LivenessScope

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.table import Table

__all__ = [
    "QueueWorker",
    "Reaper",
    "ResultRecord",
    "ResultRegistry",
    "StreamHandle",
    "entered",
    "make_result",
    "new_scope",
]

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

# `LivenessScope.__init__` carries no annotations, so mypy --strict reports every direct call as
# an untyped call. This alias gives the constructor its real signature.
_LIVENESS_SCOPE: Final[Callable[[], LivenessScope]] = LivenessScope


def entered(exec_ctx: ExecutionContext) -> ExecutionContext:
    """Return a fresh wrapper of an execution context, for one `with` block on this thread.

    `deephaven.execution_context.ExecutionContext.__enter__` stores the handle that restores the
    previous context on the wrapper object itself. If two threads enter one shared wrapper at
    once, the first to exit closes the other's handle and installs that thread's previous
    context (usually none) on its own thread, which silently loses its context. Every library
    thread therefore enters the session's context through its own wrapper:
    `with entered(exec_ctx): ...`.

    Args:
        exec_ctx: The context to enter, such as the session's captured one.

    Returns:
        A new wrapper of the same Java context; enter it once, on one thread.
    """
    return ExecutionContext(j_exec_ctx=exec_ctx.j_exec_ctx)


def new_scope() -> LivenessScope:
    """Create a library-owned liveness scope.

    The caller owns the scope and must eventually call `release()` on it exactly once.

    Returns:
        A new, open-for-business `LivenessScope`.
    """
    return _LIVENESS_SCOPE()


def make_result(build: Callable[[], Table], *, wrap: bool = False) -> tuple[Table, LivenessScope]:
    """Build one live result inside its own new result scope.

    Must run on the caller thread: Deephaven's liveness scope stack is thread-local, and the
    tables `build` creates are managed by whichever scope is on top of it. `build` runs with
    the new scope on top, so the result is managed by that scope and not by the console's query
    scope. If `build` raises, even `KeyboardInterrupt`, the scope is released and the exception
    propagates.

    Args:
        build: Creates the result, for example `lambda: blink_to_append_only(stream.blink)`.
        wrap: True when `build` returns a new Python wrapper of an existing Java table
            (`Table(j_table=...)`, as `blink()` does). Creating a wrapper creates no liveness
            reference, so the scope then manages the wrapper explicitly.

    Returns:
        `(result, scope)`; the scope holds the only library reference to the result.
    """
    scope = new_scope()
    try:
        with scope.open():
            result = build()
        if wrap:
            scope.manage(result)
    except BaseException:  # also KeyboardInterrupt: the scope must not leak
        scope.release()
        raise
    return result, scope


@dataclass(frozen=True, slots=True)
class StreamHandle:
    """A live stream as the WebSocket transport hands it to the session.

    The transport creates the stream's `TablePublisher` and blink table inside `scope` (the
    stream scope) and registers the blink table's `on_shutdown_callback`, where the stream's
    registry record is dropped (`SessionCore.stream_shut_down`) and topic reference counts are
    released. The session holds the handle while any result of the stream is held
    and releases `scope` when the last one is released.

    Attributes:
        stream_id: The stream's identifier, unique within the process (the `StreamId` column
            of `m.streams()`).
        blink: The stream's own blink-table wrapper. It is never handed to users: `blink()`
            returns a new wrapper of the same Java table.
        scope: The stream scope that manages `blink` and the publisher.
        host: The WebSocket host serving the stream.
        path: The cluster path, such as `/stocks`.
    """

    stream_id: int
    blink: Table
    scope: LivenessScope
    host: str
    path: str


@dataclass(eq=False, slots=True)
class ResultRecord:
    """The library's record of one live result.

    Created by `ResultRegistry.track`. Its fields are guarded by the session's registry lock;
    `released` changes exactly once, from False to True.

    Attributes:
        record_id: A process-unique identifier, for logs.
        scope: The result scope that holds the result.
        on_released: Runs on the reaper thread after the scope is released: for a stream it
            decrements the stream's held-result count, for a `poll()` table it stops the
            poller.
        finalizer: The collection finalizer on the result's Python wrapper, set by `track`
            right after the record is created; `finalizer.peek()` yields the wrapper while it
            is alive and None once it is collected.
        released: The once-only release flag.
    """

    record_id: int
    scope: LivenessScope
    on_released: Callable[[], None]
    finalizer: weakref.finalize[[ResultRecord, int], Table] = field(init=False)
    released: bool = False


class QueueWorker(Generic[_T]):
    """A daemon thread that handles items from a `queue.SimpleQueue` in batches, in order.

    The shared machinery of the session's background threads (the `Reaper` and the diagnostics
    writer). `put` never blocks and is safe from any thread, including inside garbage
    collection. The thread enters the session's execution context (a fresh wrapper, see
    `entered`) for its whole life. Each time it wakes it takes every item queued so far, up to
    a stop request, and hands them to `handle` as one list; an exception from `handle` is
    logged at ERROR and the thread carries on. The batch is a local of one call, so nothing
    stays referenced while the thread waits for more. After `stop()` the remaining items are
    handled, `on_exit` runs inside the execution context, and the thread ends.

    The thread holds no reference to its owner beyond `handle` and `on_exit`, so owners pass
    callables that do not keep a session alive.
    """

    __slots__ = ("_exec_ctx", "_handle", "_on_exit", "_queue", "thread")

    def __init__(
        self,
        exec_ctx: ExecutionContext,
        name: str,
        *,
        handle: Callable[[list[_T]], None],
        on_exit: Callable[[], None] | None = None,
    ) -> None:
        """Create and start the worker thread.

        Args:
            exec_ctx: The session's captured execution context, entered by the thread.
            name: The thread's name, for thread dumps.
            handle: Handles one batch of items, in queue order.
            on_exit: Runs once after the last batch, inside the execution context.
        """
        self._exec_ctx = exec_ctx
        self._handle = handle
        self._on_exit = on_exit
        self._queue: queue.SimpleQueue[_T | None] = queue.SimpleQueue()
        self.thread: Final = threading.Thread(target=self._run, name=name, daemon=True)
        self.thread.start()

    def put(self, item: _T) -> None:
        """Queue an item; it is handled after every item queued before it. Never blocks.

        Args:
            item: The item.
        """
        self._queue.put(item)

    def stop(self) -> None:
        """Handle what is queued, run `on_exit`, and end the thread. Does not wait; idempotent."""
        self._queue.put(None)

    def _run(self) -> None:
        """Handle batches until `stop()`, then run `on_exit`."""
        with entered(self._exec_ctx):
            while self._next_batch():
                pass
            if self._on_exit is not None:
                self._on_exit()

    def _next_batch(self) -> bool:
        """Wait for an item, take everything queued behind it, and handle the batch.

        Returns:
            False once the stop request was reached, True otherwise.
        """
        batch: list[_T] = []
        item = self._queue.get()
        while item is not None:
            batch.append(item)
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
        if batch:
            try:
                self._handle(batch)
            except Exception:
                logger.exception("%s failed on a batch; continuing", self.thread.name)
        return item is not None


class Reaper:
    """One daemon thread per session that releases result scopes off the caller's thread.

    Jobs are plain callables, run in submission order by a `QueueWorker`; `submit` is safe from
    any thread, including a finalizer running inside garbage collection. Each job runs in its
    own `try`/`except Exception`: a failure is logged at ERROR and the next job still runs.

    The thread holds no reference to the session, so a session that is no longer used can be
    collected; the session then calls `stop()` (through `weakref.finalize`), which lets the
    queued jobs finish and ends the thread.
    """

    __slots__ = ("_worker",)

    def __init__(self, exec_ctx: ExecutionContext, name: str) -> None:
        """Create and start the reaper thread.

        Args:
            exec_ctx: The session's captured execution context, entered by the thread.
            name: The thread's name, for thread dumps.
        """
        self._worker: QueueWorker[Callable[[], None]] = QueueWorker(
            exec_ctx, name, handle=_run_jobs
        )

    @property
    def thread(self) -> threading.Thread:
        """The reaper's thread."""
        return self._worker.thread

    def submit(self, job: Callable[[], None]) -> None:
        """Queue a job; it runs on the reaper thread after every job queued before it.

        Args:
            job: The work to run, typically releasing one result scope.
        """
        self._worker.put(job)

    def stop(self) -> None:
        """Let the queued jobs finish, then end the thread. Does not wait for it."""
        self._worker.stop()


def _run_jobs(jobs: list[Callable[[], None]]) -> None:
    """Run reaper jobs in order, each isolated from the others' failures.

    Args:
        jobs: The batch, in submission order.
    """
    for job in jobs:
        try:
            job()
        except Exception:
            logger.exception("massive reaper job failed; continuing")


class ResultRegistry:
    """The session's live results, by `id(result)`, and their single release path.

    One instance per session. Its dictionary and every record's `released` flag are guarded
    by the session's `registry_lock`, which is a leaf lock: nothing here calls Deephaven while
    holding it. Releases run on the session's `Reaper`.

    The collection finalizer takes no lock at all: it only submits a job to the reaper, which
    takes the lock there. Garbage collection can run a finalizer on any thread at any
    allocation, including one that holds another library lock, so a finalizer that took
    `registry_lock` could deadlock against a thread holding `registry_lock` and waiting for
    that other lock.

    A released record stays in the registry until its wrapper is collected, so a second
    `release()` of the same table finds it and does nothing.
    """

    __slots__ = ("_ids", "_lock", "_reaper", "_results")

    def __init__(self, lock: threading.RLock, reaper: Reaper) -> None:
        """Create an empty registry.

        Args:
            lock: The session's `registry_lock`.
            reaper: The session's reaper.
        """
        self._lock = lock
        self._reaper = reaper
        self._results: dict[int, ResultRecord] = {}
        self._ids = count(1)

    def track(
        self, result: Table, scope: LivenessScope, on_released: Callable[[], None]
    ) -> ResultRecord:
        """Record a live result so that garbage collection or `release` frees its scope.

        Args:
            result: The result, as returned to the user. It must be a distinct Python object
                per result (each `blink()` call returns its own wrapper).
            scope: The result scope `make_result` created for it.
            on_released: The owner's callback, run on the reaper after the scope is released.

        Returns:
            The new record.
        """
        with self._lock:
            key = id(result)
            record = ResultRecord(next(self._ids), scope, on_released)
            self._results[key] = record
            # The finalizer, not the registry, roots the release path: it holds this registry
            # (and through it the reaper) and the record (whose owner callback holds the
            # session core) for exactly as long as the result lives, so a result that outlives
            # every reference to its session is still released when it is collected. Nothing
            # refers to the result strongly, so no cycle keeps anything alive beyond it.
            record.finalizer = weakref.finalize(result, self._on_collected, record, key)
            record.finalizer.atexit = False
        return record

    def release(self, table: Table) -> bool:
        """Release one live result now (`m.release(table)`).

        Deterministic: the result scope is released on the reaper thread right after any
        earlier jobs. The table keeps ticking only while other holders (derived tables, open
        panels) exist. Releasing an already released result does nothing.

        Args:
            table: A table the caller believes is a live result of this session.

        Returns:
            False if `table` is not a live result of this session (checked by identity, so a
            recycled `id` cannot match); True otherwise.
        """
        with self._lock:
            record = self._results.get(id(table))
            alive = None if record is None else record.finalizer.peek()
            if record is None or alive is None or alive[0] is not table:
                return False
            first = not record.released
            record.released = True
        if first:
            self._reaper.submit(lambda: self._reap(record))
        return True

    def held(self) -> int:
        """Count the results that have not been released.

        Returns:
            How many live results the library still holds.
        """
        with self._lock:
            return sum(not record.released for record in tuple(self._results.values()))

    def _on_collected(self, record: ResultRecord, key: int) -> None:
        """Finalizer: the result's wrapper was garbage-collected. Takes no lock.

        Runs on whatever thread collected the wrapper, possibly inside another lock, so it only
        queues `_collected` on the reaper.

        Args:
            record: The collected result's record.
            key: The `id` the result had.
        """
        self._reaper.submit(lambda: self._collected(record, key))

    def _collected(self, record: ResultRecord, key: int) -> None:
        """Reaper job for a collected result: forget it and release it if not yet released.

        Args:
            record: The collected result's record.
            key: The `id` the result had.
        """
        with self._lock:
            if self._results.get(key) is record:
                del self._results[key]
            first = not record.released
            record.released = True
        if first:
            self._reap(record)

    @staticmethod
    def _reap(record: ResultRecord) -> None:
        """Release the result scope, then notify the owner even if the release failed.

        Args:
            record: The record being released.
        """
        try:
            record.scope.release()
        finally:
            record.on_released()
