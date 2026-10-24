"""How live results are kept alive and released: result scopes, records, and the reaper.

In a Deephaven console, a table created by a command is managed by the session's query scope
and lives until the session ends; deleting the Python variable does not release it (DESIGN
§ 6.1). The library therefore never lets a live result fall into that scope. Every live result
is created inside its own library-owned *result scope*, a `LivenessScope` that holds exactly
that one table, and is tracked by a `ResultRecord`:

1. `make_result` opens a fresh scope on the caller thread, builds the table inside it, and, for
   a new wrapper of an existing Java table (`blink()`), manages the wrapper explicitly.
2. `ResultRegistry.track` records the result under the session's registry lock, keyed by
   `id(result)`, with a weak reference whose callback fires when the Python wrapper is
   garbage-collected.
3. Release happens exactly once per record, by whichever comes first: the weakref callback
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
from dataclasses import dataclass
from itertools import count
from typing import TYPE_CHECKING, Final

from deephaven.liveness_scope import LivenessScope

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.execution_context import ExecutionContext
    from deephaven.table import Table

__all__ = [
    "Reaper",
    "ResultRecord",
    "ResultRegistry",
    "StreamHandle",
    "make_result",
    "new_scope",
]

logger = logging.getLogger(__name__)

# `LivenessScope.__init__` carries no annotations, so mypy --strict reports every direct call as
# an untyped call. This alias gives the constructor its real signature.
_LIVENESS_SCOPE: Final[Callable[[], LivenessScope]] = LivenessScope


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
    scope. If `build` raises, the scope is released and the exception propagates.

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
    except Exception:
        scope.release()
        raise
    return result, scope


@dataclass(frozen=True, slots=True)
class StreamHandle:
    """A live stream as the WebSocket transport hands it to the session.

    The transport creates the stream's `TablePublisher` and blink table inside `scope` (the
    stream scope) and registers the blink table's `on_shutdown_callback`, where topic reference
    counts are released. The session holds the handle while any result of the stream is held
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
        ref: A weak reference to the result's Python wrapper; it resolves to None once the
            wrapper is collected.
        released: The once-only release flag.
    """

    record_id: int
    scope: LivenessScope
    on_released: Callable[[], None]
    ref: weakref.ref[Table]
    released: bool = False


class Reaper:
    """One daemon thread per session that releases result scopes off the caller's thread.

    Jobs are plain callables on a `queue.SimpleQueue`, which is safe to feed from weakref
    callbacks running inside garbage collection. The thread enters the session's
    `ExecutionContext` for every job, wraps each in `try`/`except Exception`, logs a failure at
    ERROR, and continues. Jobs run in submission order, so a job submitted after others runs
    after they have finished.

    The thread holds no reference to the session, so a session that is no longer used can be
    collected; the session then calls `stop()` (through `weakref.finalize`), which lets the
    queued jobs finish and ends the thread.
    """

    __slots__ = ("_exec_ctx", "_queue", "_thread")

    def __init__(self, exec_ctx: ExecutionContext, name: str) -> None:
        """Create and start the reaper thread.

        Args:
            exec_ctx: The session's captured execution context, entered for every job.
            name: The thread's name, for thread dumps.
        """
        self._exec_ctx = exec_ctx
        self._queue: queue.SimpleQueue[Callable[[], None] | None] = queue.SimpleQueue()
        self._thread = threading.Thread(target=self._run, name=name, daemon=True)
        self._thread.start()

    def submit(self, job: Callable[[], None]) -> None:
        """Queue a job; it runs on the reaper thread after every job queued before it.

        Safe from any thread, including a weakref callback during garbage collection.

        Args:
            job: The work to run, typically releasing one result scope.
        """
        self._queue.put(job)

    def stop(self) -> None:
        """Let the queued jobs finish, then end the thread. Does not wait for it."""
        self._queue.put(None)

    def _run(self) -> None:
        """Run jobs until `stop()`."""
        while (job := self._queue.get()) is not None:
            try:
                with self._exec_ctx:
                    job()
            except Exception:
                logger.exception("massive reaper job failed; continuing")


class ResultRegistry:
    """The session's live results, by `id(result)`, and their single release path.

    One instance per session. Its dictionary and every record's `released` flag are guarded
    by the session's `registry_lock`, which is a leaf lock: nothing here calls Deephaven while
    holding it. Scope releases are submitted to the session's `Reaper`.

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

            def collected(_ref: weakref.ref[Table]) -> None:
                self._on_collected(record, key)

            record = ResultRecord(
                next(self._ids), scope, on_released, weakref.ref(result, collected)
            )
            self._results[key] = record
        return record

    def release(self, table: Table) -> None:
        """Release one live result now (`m.release(table)`).

        Deterministic: the result scope is released on the reaper thread right after any
        earlier jobs. The table keeps ticking only while other holders (derived tables, open
        panels) exist. Releasing an already released result does nothing.

        Args:
            table: A live result this session returned.

        Raises:
            ValueError: If `table` is not a live result of this session (checked by identity,
                so a recycled `id` cannot match).
        """
        with self._lock:
            record = self._results.get(id(table))
            if record is None or record.ref() is not table:
                raise ValueError("table is not a live result of this session")
            first = not record.released
            record.released = True
        if first:
            self._reaper.submit(lambda: self._reap(record))

    def held(self) -> int:
        """Count the results that have not been released.

        Returns:
            How many live results the library still holds.
        """
        with self._lock:
            return sum(not record.released for record in tuple(self._results.values()))

    def _on_collected(self, record: ResultRecord, key: int) -> None:
        """Weakref callback: the result's wrapper was garbage-collected.

        Runs on whatever thread collected the wrapper. Makes no Java call: under the lock it
        removes the record and flips its flag, then queues the release if it was the first.

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
            self._reaper.submit(lambda: self._reap(record))

    @staticmethod
    def _reap(record: ResultRecord) -> None:
        """Reaper job: release the result scope, then notify the owner.

        Args:
            record: The record being released.
        """
        record.scope.release()
        record.on_released()
