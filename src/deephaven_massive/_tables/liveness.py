"""How live results are kept alive and released: result scopes, records, and the reaper.

In a Deephaven console, a table created by a command is managed by the session's query scope and
lives until the session ends; deleting the Python variable does not release it. The library
therefore never lets a live result fall into that scope. Every live result is created inside its own
library-owned *result scope*, a `LivenessScope` that holds exactly that one table, and is tracked by
a `ResultRecord`:

1. `make_result` opens a fresh scope on the caller thread and builds the table inside it, so
   the scope manages it. Every result is a new table, `blink()` included (a described copy of
   the stream's blink table).
2. `ResultRegistry.track` records the result under the session's registry lock, keyed by
   `id(result)`, with a `weakref.finalize` that fires when the Python wrapper is
   garbage-collected.
3. Release happens exactly once per record, by whichever comes first: the finalizer
   (garbage collection), `ResultRegistry.release` (`m.release(table)`). Each flips the
   record's once-only `released` flag under the registry lock and, if it was the first,
   submits a job to the `Reaper`.
4. The `Reaper`, one daemon thread per session, runs the job: it releases the result scope
   (under the update graph's shared lock, `release_scope`) and then calls the record's
   `on_released` callback, which for a stream decrements the stream's held-result count (and at
   zero releases the stream scope), for a `poll()` table marks its row released, and for a
   diagnostics view drops the session's count of held views. A failing job is logged at ERROR
   and the thread carries on.

Releasing a result scope drops the library's liveness reference only. A table the user derived
from the result, an open UI panel, or a user liveness scope keeps the Java table alive, and with
it the stream or the poller: topic reference counts and pollers are bound to the blink table's
`on_shutdown_callback`, never to result records. The owner callbacks hold the session core only
weakly, so a result never keeps a session alive through its record; the session's live set does
that (`_tables/session_core.py`).

**Every release holds the update graph's lock.** Releasing a scope can drop the last reference
to a refreshing table, which destroys it. The library releases every scope through
`release_scope`, which holds the update graph's shared lock, so a destroy never overlaps an
update cycle (see `release_scope` for the crash that prevents).

Threading: `make_result` runs on the caller thread (liveness scopes are thread-local). The
registry's state is guarded by the session's `registry_lock`, a leaf lock under which no
Deephaven call is made. Scope releases happen on the reaper thread inside the session's
`ExecutionContext`, under the update graph's shared lock and outside `registry_lock`: the lock
order is the update graph's lock, then `registry_lock`.
"""

from __future__ import annotations

import logging
import queue
import threading
import weakref
from dataclasses import dataclass, field
from itertools import count
from typing import TYPE_CHECKING, Final, Generic, TypeVar, cast

from deephaven.liveness_scope import LivenessScope

from deephaven_massive._errors import LIBRARY_BUG
from deephaven_massive._tables.execution_context import entered

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

__all__ = [
    "QueueWorker",
    "Reaper",
    "ResultRecord",
    "ResultRegistry",
    "make_result",
    "new_scope",
    "release_scope",
]

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

# `LivenessScope.__init__` carries no annotations, so mypy --strict reports every direct call as
# an untyped call. This alias gives the constructor its real signature.
_LIVENESS_SCOPE: Final[Callable[[], LivenessScope]] = LivenessScope


def new_scope() -> LivenessScope:
    """Create a library-owned liveness scope.

    The caller owns the scope and must eventually release it exactly once, with `release_scope`.

    Returns:
        A new, open-for-business `LivenessScope`.
    """
    return _LIVENESS_SCOPE()


def release_scope(exec_ctx: ExecutionContext, scope: LivenessScope) -> None:
    """Release a liveness scope while holding the update graph's shared lock.

    The library's only way to release a scope. Releasing one can drop the last reference to a
    refreshing table, and Deephaven then destroys the table on the releasing thread. While the
    shared lock is held no update cycle runs, so the destroy completes between two cycles and
    never overlaps one. That matters for a blink table whose publisher has a failure queued:
    destroyed by a thread that does not hold the lock, its adapter can run once more after it
    delivered the failure and deliver it a second time, and Deephaven's fatal-error handler then
    exits the process. A destroy runs the table's shutdown callback on this thread, under the
    lock; the library's callbacks only update registries and queue work, so they never wait.

    It blocks while an update cycle runs, and for as long as another thread holds the exclusive
    lock (a console command), but it cannot deadlock: no thread that holds the update graph's
    lock waits for a thread that releases a scope. A thread that already holds either lock, such
    as a console command's, takes it again at once. An asynchronous exception (a
    `KeyboardInterrupt`) that lands just after the lock is taken, before the `try`, would leave
    it held, as with Deephaven's own `shared_lock`; only a caller thread can receive one.

    Never call it on an update-graph thread (in a listener or a shutdown callback, queue the
    release on the reaper instead), under `registry_lock`, or under any other library lock that
    a holder of the update graph's lock may wait for, such as a stream's publish lock: the
    update graph's lock comes first.

    Args:
        exec_ctx: The session's execution context, entered (through a fresh wrapper) for the
            release; its update graph's shared lock is taken.
        scope: The scope to release, once; releasing it again raises Deephaven's `DHError`.
    """
    # The Java lock itself: `deephaven.update_graph.shared_lock` would wrap the release's own
    # error in a second `DHError`.
    lock = exec_ctx.update_graph.j_update_graph.sharedLock()
    with entered(exec_ctx):
        lock.lock()
        try:
            scope.release()
        finally:
            lock.unlock()


def make_result(
    exec_ctx: ExecutionContext, build: Callable[[], Table]
) -> tuple[Table, LivenessScope]:
    """Build one live result inside its own new result scope.

    Must run on the caller thread, inside the session's execution context: Deephaven's liveness
    scope stack is thread-local, and the tables `build` creates are managed by whichever scope
    is on top of it. `build` runs with the new scope on top, so the result is managed by that
    scope and not by the console's query scope. If `build` raises, even `KeyboardInterrupt`,
    the scope is released (`release_scope`) and the exception propagates.

    Args:
        exec_ctx: The session's execution context, whose update graph's lock the release of a
            failed build takes.
        build: Creates the result, a new table, for example
            `lambda: blink_to_append_only(stream.blink)`. A wrapper of an existing Java table
            creates no liveness reference, so `build` never returns one.

    Returns:
        `(result, scope)`; the scope holds the only library reference to the result.
    """
    scope = new_scope()
    try:
        with scope.open():
            result = build()
    except BaseException:  # also KeyboardInterrupt: the scope must not leak
        release_scope(exec_ctx, scope)
        raise
    return result, scope


@dataclass(eq=False, slots=True)
class ResultRecord:
    """The library's record of one live result.

    Created by `ResultRegistry.track`. Its fields are guarded by the session's registry lock;
    `released` changes exactly once, from False to True.

    Attributes:
        record_id: A process-unique identifier, for logs.
        scope: The result scope that holds the result.
        on_released: Runs on the reaper thread after the scope is released: for a stream it
            decrements the stream's held-result count, for a `poll()` table it marks the row
            released, and for a diagnostics view it drops the session's count of held views.
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

    An owner whose state also changes with time alone (the diagnostics writer's rate limits)
    passes `timeout`, the seconds the thread may wait for an item before it calls `on_timeout`
    on its own thread; an exception from `on_timeout` is logged like one from `handle`. No other
    thread or timer is involved, so nothing outlives the worker.

    If something escapes that the thread cannot carry on after (a `KeyboardInterrupt` or
    `SystemExit` raised by `handle`, or an exception from `on_exit`), `on_death` runs with it
    before the thread ends, so the owner can fail what the thread served instead of leaving it
    silently frozen.

    The thread holds no reference to its owner beyond its callables, so owners pass callables
    that do not keep a session alive.
    """

    __slots__ = (
        "_exec_ctx",
        "_handle",
        "_on_death",
        "_on_exit",
        "_on_timeout",
        "_queue",
        "_timeout",
        "thread",
    )

    def __init__(
        self,
        exec_ctx: ExecutionContext,
        name: str,
        *,
        handle: Callable[[list[_T]], None],
        on_exit: Callable[[], None] | None = None,
        timeout: Callable[[], float | None] | None = None,
        on_timeout: Callable[[], None] | None = None,
        on_death: Callable[[BaseException], None] | None = None,
    ) -> None:
        """Create and start the worker thread.

        Args:
            exec_ctx: The session's captured execution context, entered by the thread.
            name: The thread's name, for thread dumps.
            handle: Handles one batch of items, in queue order.
            on_exit: Runs once after the last batch, inside the execution context.
            timeout: Asked, on the thread, before each wait: the seconds to wait for an item
                (0 or less: not at all), or None to wait until one comes. None for no timed
                waits.
            on_timeout: Runs on the thread when a timed wait ends without an item; required
                with `timeout`.
            on_death: Runs on the dying thread, inside the execution context, with what ended
                it, before it is re-raised; an exception it raises is logged and dropped.
        """
        self._exec_ctx = exec_ctx
        self._handle = handle
        self._on_exit = on_exit
        self._timeout = timeout
        self._on_timeout = on_timeout
        self._on_death = on_death
        self._queue: queue.SimpleQueue[_T | None] = queue.SimpleQueue()
        self.thread: Final = threading.Thread(target=self._run, name=name, daemon=True)
        self.thread.start()

    def put(self, item: _T) -> None:
        """Queue an item; it is handled after every item queued before it. Never blocks.

        An item queued after `stop()` is never handled: the thread ends at the stop request,
        and nothing reads the queue again.

        Args:
            item: The item.
        """
        self._queue.put(item)

    def stop(self) -> None:
        """Handle what is queued, run `on_exit`, and end the thread. Does not wait; idempotent.

        Items queued after this call are dropped unhandled (`put`).
        """
        self._queue.put(None)

    def _run(self) -> None:
        """Handle batches until `stop()`, then run `on_exit`.

        A batch's `Exception` is logged and the thread carries on (`_next_batch`). Anything
        else that escapes, such as a `KeyboardInterrupt` or `SystemExit` raised by `handle` or
        an exception from `on_exit`, ends the thread: it is logged at ERROR first, because
        nothing would handle the queue again and the owner (the reaper or the diagnostics
        writer) would otherwise stop silently; then `on_death` runs, and the exception is
        re-raised.
        """
        with entered(self._exec_ctx):
            try:
                while self._next_batch():
                    pass
                if self._on_exit is not None:
                    self._on_exit()
            except BaseException as death:
                logger.exception(
                    "%s died; its queue is no longer handled. %s", self.thread.name, LIBRARY_BUG
                )
                if self._on_death is not None:
                    try:
                        self._on_death(death)
                    except Exception:
                        logger.exception(
                            "%s: handling its death failed. %s", self.thread.name, LIBRARY_BUG
                        )
                raise

    def _next_batch(self) -> bool:
        """Wait for an item, take everything queued behind it, and handle the batch.

        A timed wait (`timeout`) that ends without an item runs `on_timeout` instead.

        Returns:
            False once the stop request was reached, True otherwise.
        """
        batch: list[_T] = []
        wait = None if self._timeout is None else self._timeout()
        try:
            item = self._queue.get(timeout=None if wait is None else max(wait, 0.0))
        except queue.Empty:
            try:
                cast("Callable[[], None]", self._on_timeout)()
            except Exception:
                logger.exception(
                    "%s failed on a timed wake-up; continuing. %s", self.thread.name, LIBRARY_BUG
                )
            return True
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
                logger.exception(
                    "%s failed on a batch; continuing. %s", self.thread.name, LIBRARY_BUG
                )
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

    def __init__(
        self,
        exec_ctx: ExecutionContext,
        name: str,
        *,
        on_death: Callable[[BaseException], None] | None = None,
    ) -> None:
        """Create and start the reaper thread.

        Args:
            exec_ctx: The session's captured execution context, entered by the thread.
            name: The thread's name, for thread dumps.
            on_death: Runs if the thread dies on something it cannot carry on after
                (`QueueWorker`); the session passes a callback that fails its live tables.
        """
        self._worker: QueueWorker[Callable[[], None]] = QueueWorker(
            exec_ctx, name, handle=_run_jobs, on_death=on_death
        )

    @property
    def thread(self) -> threading.Thread:
        """The reaper's thread."""
        return self._worker.thread

    def submit(self, job: Callable[[], None]) -> None:
        """Queue a job; it runs on the reaper thread after every job queued before it.

        A job submitted after `stop()` never runs. The reaper stops only once its session is
        collected and no tracked result remains (`ResultRegistry.retire`), so every tracked
        result's release job runs first.

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
            logger.exception("massive reaper job failed; continuing. %s", LIBRARY_BUG)


class ResultRegistry:
    """The session's live results, by `id(result)`, and their single release path.

    One instance per session. Its dictionary and every record's `released` flag are guarded
    by the session's `registry_lock`, which is a leaf lock: nothing here calls Deephaven while
    holding it. Releases run on the session's `Reaper`, under the update graph's shared lock
    (`release_scope`) and outside `registry_lock`.

    The collection finalizer takes no lock at all: it only submits a job to the reaper, which
    takes the lock there. Garbage collection can run a finalizer on any thread at any
    allocation, including one that holds another library lock, so a finalizer that took
    `registry_lock` could deadlock against a thread holding `registry_lock` and waiting for
    that other lock.

    A released record stays in the registry until its wrapper is collected, so a second
    `release()` of the same table finds it and does nothing.
    """

    __slots__ = ("_exec_ctx", "_ids", "_lock", "_reaper", "_results", "_retired")

    def __init__(self, lock: threading.RLock, reaper: Reaper, exec_ctx: ExecutionContext) -> None:
        """Create an empty registry.

        Args:
            lock: The session's `registry_lock`.
            reaper: The session's reaper.
            exec_ctx: The session's execution context, whose update graph's lock every release
                of a result scope takes.
        """
        self._exec_ctx = exec_ctx
        self._lock = lock
        self._reaper = reaper
        self._results: dict[int, ResultRecord] = {}
        self._ids = count(1)
        self._retired = False

    def track(
        self, result: Table, scope: LivenessScope, on_released: Callable[[], None]
    ) -> ResultRecord:
        """Record a live result so that garbage collection or `release` frees its scope.

        The record is complete (its finalizer exists) before it is registered, so `release`
        never sees a half-built record. If anything raises, even `KeyboardInterrupt` (also one
        landing right after the finalizer is created, or at the lock's exit after the insert),
        the record is marked released and removed from the registry, so neither its finalizer
        nor `release` ever releases it: the caller still owns `scope` and must release it.

        Args:
            result: The result, as returned to the user. It must be a distinct Python object
                per result (each `blink()` call returns its own described copy).
            scope: The result scope `make_result` created for it.
            on_released: The owner's callback, run on the reaper after the scope is released.

        Returns:
            The new record.
        """
        key = id(result)
        with self._lock:
            record = ResultRecord(next(self._ids), scope, on_released)
        # The finalizer, not the registry, roots the release path: it holds this registry (and
        # through it the reaper) and the record (whose owner callback holds the session core)
        # for exactly as long as the result lives, so a result that outlives every reference
        # to its session is still released when it is collected. Nothing refers to the result
        # strongly, so no cycle keeps anything alive beyond it.
        try:
            record.finalizer = weakref.finalize(result, self._on_collected, record, key)
            record.finalizer.atexit = False
            with self._lock:
                self._results[key] = record
        except BaseException:  # the caller releases the scope: no finalizer may release it
            with self._lock:
                record.released = True
                if self._results.get(key) is record:
                    del self._results[key]
            raise
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

    def retire(self) -> None:
        """Stop the reaper once no tracked result remains; called when the session is collected.

        Owner callbacks hold the session core only weakly, so a tracked result can outlive the
        session: one `release()`d while its wrapper is still alive, for example. Stopping the
        reaper with the session would leave such a result unreleased when it is collected, so
        the reaper is stopped only once the registry is empty: now if it already is, otherwise
        by the job that forgets the last collected result. Takes no lock and makes no Java call
        (it runs in a garbage-collection finalizer): it only queues a job on the reaper.
        """
        self._reaper.submit(self._retire)

    def _retire(self) -> None:
        """Reaper job of `retire`: mark the registry retired and stop the reaper if it is empty."""
        with self._lock:
            self._retired = True
            idle = not self._results
        if idle:
            self._reaper.stop()

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
            idle = self._retired and not self._results
        try:
            if first:
                self._reap(record)
        finally:
            if idle:  # the session is gone and this was its last result
                self._reaper.stop()

    def _reap(self, record: ResultRecord) -> None:
        """Release the result scope under the lock, then notify the owner even if that failed.

        The owner's callback runs once the update graph's lock is let go: for a stream it takes
        `registry_lock`, and at zero holds it releases the stream scope, taking the lock again.

        Args:
            record: The record being released.
        """
        try:
            release_scope(self._exec_ctx, record.scope)
        finally:
            record.on_released()
