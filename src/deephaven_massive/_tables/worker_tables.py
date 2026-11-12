"""`WorkerTables`, a session's worker tables, from registration until both their clocks run out.

A *worker table* is a table a library thread fills: a `poll()` table's poller, a background
read's task on the session's read pool for a `fetch()` or `history()` without `wait=True`, or the
follow coordinator for a `history()` or `fetch()` of a refreshing symbol table. The registry
keeps the `m.streams()` row of each from its creation until its result is released and its
blink table has shut down, which happen on different clocks, in either order; a background
read's record leaves as soon as it is `DONE` and its result released, since nothing of it needs
the session then. Every diagnostics row is queued while `registry_lock` is held, so rows reach
the tables in the order the registry changed.

The registry belongs to one `SessionCore`, which creates it and alone holds it; it refers to the
core weakly. Every entry point that can change what keeps the session alive (`track`, `release`,
`_shut_down`, `finish_read`) holds the core in a local variable until it returns and updates the
live set (`live_set.sync`) in the same `registry_lock` section as the change. The core reads and
changes the rows through the by-id methods (`row`, `change_row`, `live_fails`, `close_rows`, and
`in`), always under `registry_lock`, when a transport reports a row change, a failure, or drift by
stream id, and when the session closes.
"""

from __future__ import annotations

import functools
import logging
import weakref
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Final, Protocol, TypeVar, cast

from deephaven_massive._errors import MassiveError, SessionClosedError
from deephaven_massive._tables import live_set
from deephaven_massive._tables.diagnostics import FINAL_STATES, StreamRow, StreamState
from deephaven_massive._tables.liveness import release_scope

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable

    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table

    from deephaven_massive._tables.diagnostics import StreamUpdate
    from deephaven_massive._tables.session_core import SessionCore

__all__ = ["WorkerTable", "WorkerTables"]

logger = logging.getLogger(__name__)


class WorkerTable(Protocol):
    """What `WorkerTables.track` needs of a worker table: a `poll()` table or a read."""

    def fail(self, error: MassiveError) -> bool:
        """Stop the thread and fail the table, once (`_WorkerState.fail`)."""
        ...

    def release(self) -> None:
        """Mark the row released; the result record's `on_released` callback."""
        ...

    def abandon(self) -> None:
        """Stop a worker whose thread will never start, and let go of what it joined."""
        ...


_W = TypeVar("_W", bound=WorkerTable)

_ACTIVE: Final = frozenset({StreamState.RUNNING, StreamState.LOADING})
"""The states of a table that is still publishing, or still loading: the ones a release marks
`RELEASED`."""


def _worker_table_shut_down(
    tables_ref: weakref.ref[WorkerTables],
    stream_id: int,
    *,
    record: Callable[[WorkerTables, int], None],
    stop: threading.Event,
) -> None:
    """A worker table's blink-table shutdown callback: stop its thread and tell the session.

    Runs on whichever thread dropped the table's last reference (the reaper, an update-graph
    thread after a failure, a gRPC thread, or Deephaven's liveness-cleanup thread), so it only
    sets a token and updates the registry (`WorkerTables._shut_down`). The session is alive
    unless the record left early: the table's record keeps it in the live set until this has
    run, and before the record exists the opening caller holds it, except that a background
    read's record leaves once the read is `DONE` and its result released, and its session may
    then be freed while a derived table still uses the table. With the session gone there is
    nothing to record.

    Args:
        tables_ref: The session's worker-table registry, weakly.
        record: `WorkerTables._shut_down`, unbound.
        stream_id: The table's `StreamId`.
        stop: The thread's stop token.
    """
    stop.set()
    tables = tables_ref()
    if tables is not None:
        record(tables, stream_id)


@dataclass(frozen=True, slots=True)
class _WorkerState:
    """One worker table of the registry; replaced, never mutated, under `registry_lock`.

    A worker table is a `poll()` table or a background read: a table one library thread of its
    own fills. Like a stream's record, it has two clocks: its one result's hold, and its blink
    table's shutdown. Releasing the result marks the row `RELEASED` and leaves the thread
    running (a derived table may still use the table); the shutdown stops the thread. The
    record leaves the registry once both have happened, or, for a background read, once its
    result is released and its row is `DONE`: its rows are in the table, and it has no thread
    left to stop.

    Attributes:
        row: The table's current diagnostics row.
        fail: Stops the thread and fails the table with the error, once; returns False when
            the table had already failed or, for a read, already got its rows. A Deephaven
            call: never under `registry_lock`.
        held: True until the result is released.
        shut_down: True once the blink table shut down while the result was held.
    """

    row: StreamRow
    fail: Callable[[MassiveError], bool]
    held: bool = True
    shut_down: bool = False


class WorkerTables:
    """The worker-table registry of one session: `poll()` tables, background and followed reads.

    Created by `SessionCore.__init__`, which alone holds it, as `core.workers`. Thread-safe:
    its state is guarded by the core's `registry_lock`.
    """

    def __init__(self, core: SessionCore) -> None:
        """Create the empty registry of a core, which it refers to weakly.

        Args:
            core: The session's core, with its `registry_lock`, result registry, diagnostics,
                and drift registry already created.
        """
        self._core: Final = weakref.ref(core)
        self._lock: Final = core.registry_lock
        self._label: Final = core.label
        self._exec_ctx: Final = core.exec_ctx
        self._results: Final = core.results
        self._diagnostics: Final = core.diagnostics
        self._drift: Final = core.drift
        # The worker tables, by stream id; they have no key and no handle.
        self._workers: dict[int, _WorkerState] = {}

    @property
    def active(self) -> bool:
        """True while any worker table is registered, which keeps the session in the live set.

        Read with `registry_lock` held (`live_set.sync`).
        """
        return bool(self._workers)

    def __contains__(self, stream_id: object) -> bool:
        """Return whether a worker table is registered; `registry_lock` is held.

        Args:
            stream_id: The table's `StreamId`.

        Returns:
            True while the table is in the registry.
        """
        return stream_id in self._workers

    def row(self, stream_id: int) -> StreamRow | None:
        """Return a registered worker table's current row; `registry_lock` is held.

        Args:
            stream_id: The table's `StreamId`.

        Returns:
            The row, or None when the id is not a registered worker table.
        """
        state = self._workers.get(stream_id)
        return None if state is None else state.row

    def track(
        self, result: Table, scope: LivenessScope, make: Callable[[], tuple[_W, StreamRow]]
    ) -> _W:
        """Make a worker table's worker, register its row, and track its result, or undo it all.

        The one way `poll()`, a background read, and a followed read register their table:
        the worker and its first row are made (`make`), the row registered (`_register`),
        and the result tracked (`ResultRegistry.track`), so releasing it marks the row
        released. If any step raises, even `KeyboardInterrupt`, the worker is abandoned, a
        registered row released (`release`), and the result scope released, and the exception
        propagates. `_register` reports the row in `registered` right after storing it, so an
        interrupt that lands later in `_register`, or as it returns, still releases the row. If
        the session closed once the row was registered (`close()` failed the table meanwhile),
        the worker is abandoned and the result released. Run it on the caller thread, under no
        library lock; the caller starts the worker's thread only after it returns.

        Args:
            result: The result, built in `scope` (`make_result`).
            scope: The result scope, which this call owns from now on.
            make: Makes the worker and the table's first row, as `_register` takes
                it; it may also enlist the worker with the session (a poller joins the
                session's pollers), and may raise if the session closed.

        Returns:
            The worker, not yet started.

        Raises:
            SessionClosedError: If the session is closed (`"MassiveSession #1 is closed"`),
                before the row was registered or after.
        """
        worker: _W | None = None
        registered: list[int] = []  # the stream id, as soon as `_register` stores the row
        try:
            core = cast("SessionCore", self._core())  # held until the call returns: live_set.py
            worker, row = make()
            self._register(core, row, fail=worker.fail, registered=registered)
            self._results.track(result, scope, worker.release)
        except BaseException:
            try:
                if worker is not None:
                    worker.abandon()
                for stream_id in registered:
                    self.release(stream_id)
            finally:
                release_scope(self._exec_ctx, scope)
            raise
        if core.closed:  # close() failed the table meanwhile; its thread must never start
            logger.debug("%s: stream %d opened as the session closed", self._label, row.stream_id)
            worker.abandon()
            self._results.release(result)
            raise SessionClosedError(f"{self._label} is closed")
        return worker

    def shutdown_callback(self, stream_id: int, stop: threading.Event) -> Callable[[], None]:
        """Return the shutdown callback of a worker table's blink table.

        It sets the thread's stop token and records the shutdown (`_shut_down`), and holds
        the registry, and so the session, only weakly, so the Java table that keeps it never
        keeps the session alive.

        Args:
            stream_id: The table's `StreamId`.
            stop: The thread's stop token.

        Returns:
            The callback, for `blink_publisher`'s `on_shutdown`.
        """
        return functools.partial(
            _worker_table_shut_down,
            weakref.ref(self),
            stream_id,
            record=WorkerTables._shut_down,
            stop=stop,
        )

    def _register(
        self,
        core: SessionCore,
        row: StreamRow,
        *,
        fail: Callable[[MassiveError], bool],
        registered: list[int],
    ) -> None:
        """Register a new worker table in `m.streams()` and the live set, before its thread starts.

        A worker table, a `poll()` table or a background read, is no shared stream: it has no
        key, is never reused, and its one result is its only hold. Its record stays in the
        registry, keeping the session alive and taking its thread's updates
        (`SessionCore.update_streams`, `SessionCore.fail_stream`,
        `SessionCore.record_stream_drift`, `finish_read`), until its result is released
        (`release`) and its blink table has shut down (`_shut_down`), in either
        order; a background read's leaves as soon as it is `DONE` and its result released,
        since nothing of it needs the session then. One short critical section under
        `registry_lock`; the row is queued for the diagnostics writer.

        Args:
            core: The session's core, which the caller holds.
            row: The table's first row: its `StreamId` (from `next_stream_id`), its `Kind`
                (`POLL` or `READ`), its dataset and symbols, one held result, its transport,
                and a poll's host and path; `RUNNING` for a poll, `LOADING` for a read.
            fail: Stops the thread and fails the table, once (`_WorkerState.fail`); `close()`
                and a dying library thread call it.
            registered: The caller's list of rows to release if it fails; the table's
                `StreamId` is appended right after its record is stored, before the row is
                written or the live set updated, so a `KeyboardInterrupt` in either still
                leaves the caller a row to release.

        Raises:
            SessionClosedError: If the session is closed (`"MassiveSession #1 is closed"`): the
                table must not start its thread then.
        """
        with self._lock:
            if core.closed:
                raise SessionClosedError(f"{self._label} is closed")
            self._workers[row.stream_id] = _WorkerState(row, fail)
            registered.append(row.stream_id)  # before any call: an interrupt still releases it
            self._diagnostics.write(row)
            live_set.sync(core)

    def release(self, stream_id: int) -> None:
        """Record that a worker table's result was released; its thread keeps running.

        The row is written with no held result and `State=RELEASED` when it was `RUNNING` or
        `LOADING`; a failed, closed, or done row keeps its state. A derived table may still use
        the table, so the thread runs on until the table's blink table shuts down
        (`_shut_down`); if it already has, or the row is a background read's `DONE`
        (its rows are in the table, and no thread or close acts on it any more), the record
        leaves the registry now and the drift registry forgets it. Runs on the reaper thread
        when the result is released, or on the caller thread when the verb fails after
        registering the row; each table is released once. No Deephaven call, no wait.

        Args:
            stream_id: The table's `StreamId`.
        """
        with self._lock:
            core = cast("SessionCore", self._core())  # held until the call returns: live_set.py
            state = self._workers[stream_id]
            released = state.row.state in _ACTIVE
            row = replace(
                state.row,
                held_results=0,
                state=StreamState.RELEASED if released else state.row.state,
            )
            self._diagnostics.write(row)
            if state.shut_down or row.state is StreamState.DONE:
                self._forget(stream_id)
            else:
                self._workers[stream_id] = replace(state, row=row, held=False)
            live_set.sync(core)

    def _shut_down(self, stream_id: int) -> None:
        """Record that a worker table's blink table has shut down: nothing uses the table.

        Called from the blink table's `on_shutdown_callback`, after the table's thread was told
        to stop, on whichever thread dropped the table's last reference. With the result
        released the record leaves the registry (its last row stays in `m.streams()`); while the
        result is still held (a failure shut the table down) it is marked, and the release
        deletes it. It takes only `registry_lock`, makes no Deephaven call, and writes no row.
        An unknown id (a table whose registration failed) is ignored, and so is a session that
        has been freed (`_worker_table_shut_down`).

        Args:
            stream_id: The table's `StreamId`.
        """
        with self._lock:
            core = self._core()  # held until the call returns: see live_set.py
            if core is None:
                return
            state = self._workers.get(stream_id)
            if state is None:
                return
            if state.held:
                self._workers[stream_id] = replace(state, shut_down=True)
            else:
                self._forget(stream_id)
            live_set.sync(core)

    def _forget(self, stream_id: int) -> None:
        """Take a worker table's record out of the registry; `registry_lock` is held.

        Its last row stays in `m.streams()`, and the drift registry forgets the table. The
        caller updates the live set (`live_set.sync`).

        Args:
            stream_id: The table's `StreamId`, which must be registered.
        """
        del self._workers[stream_id]
        self._drift.forget(stream_id)

    def finish_read(self, stream_id: int, rows: int) -> None:
        """Record that a background read's rows have landed: its row becomes `DONE`.

        The listener on the read's result calls it in the update cycle that adds the rows, on
        an update-graph thread holding the update graph's lock, so `DONE` reaches `m.streams()`
        only after the rows are in the result; the read's task calls it for an empty result,
        which adds no rows, and a close's hook for a read that has published, outside every
        lock. `DONE` replaces `LOADING`, `RELEASED` (a derived
        table kept the read going after its result was released), and `CLOSED` (a close that
        raced the publish, which then failed nothing: the table has its rows). The read's
        publish lock fails a read only before its rows land, so no row is `FAILED` here. `DONE`
        is final. `RowsPublished` is set and `Error` cleared. A read whose result was released
        already then leaves the registry, as `release` takes a `DONE` one out: a table
        derived from it keeps its rows without the session. One short critical section under
        `registry_lock`; an id that has left the registry is ignored. No Deephaven call, no
        wait.

        Args:
            stream_id: The read's `StreamId`.
            rows: How many rows the result holds, for `RowsPublished`.
        """
        with self._lock:
            core = cast("SessionCore", self._core())  # held until the call returns: live_set.py
            done = functools.partial(
                replace, state=StreamState.DONE, rows_published=rows, error=None
            )
            if self.change_row(stream_id, done) and not self._workers[stream_id].held:
                self._forget(stream_id)
                live_set.sync(core)

    def update_followed_read(self, stream_id: int, update: StreamUpdate, *, loading: bool) -> None:
        """Apply a followed read's progress to its `m.streams()` row, and its state.

        The follow coordinator calls this after each change of the read's symbols. Like
        `SessionCore.update_streams` it sets the fields `update` names; unlike it, it also sets
        `State`:
        `LOADING` while a symbol is queued, loading, or waiting to retry, `RUNNING` once every
        symbol has landed or failed. Only a `LOADING` or `RUNNING` row changes state: a
        released, failed, or closed one keeps its state. Once the session has closed nothing is
        written, as in `SessionCore.write_symbol_reads`: the row keeps what the close wrote. One
        short critical section under `registry_lock`; an id that has left the registry is
        ignored. No Deephaven call, no wait.

        Args:
            stream_id: The followed read's `StreamId`.
            update: The fields to set.
            loading: True while any current symbol is queued, loading, or waiting to retry.
        """
        state = StreamState.LOADING if loading else StreamState.RUNNING

        def change(row: StreamRow) -> StreamRow:
            row = update.apply(row)
            return replace(row, state=state) if row.state in _ACTIVE else row

        with self._lock:
            if cast("SessionCore", self._core()).closed:
                return
            self.change_row(stream_id, change)

    # --- By-id reports and the close: the core calls these with `registry_lock` held. -------

    def change_row(self, stream_id: int, change: Callable[[StreamRow], StreamRow]) -> bool:
        """Change a registered worker table's row and write it; `registry_lock` is held.

        Args:
            stream_id: The table's `StreamId`.
            change: Returns the new row from the current one; called once.

        Returns:
            True if the id is a registered worker table.
        """
        state = self._workers.get(stream_id)
        if state is None:
            return False
        row = change(state.row)
        self._workers[stream_id] = replace(state, row=row)
        self._diagnostics.write(row)
        return True

    def live_fails(self) -> list[tuple[int, Callable[[MassiveError], bool]]]:
        """Return the stream id and fail hook of every running worker table.

        `registry_lock` is held.

        Returns:
            `(stream id, hook)` of each worker table whose table has not shut down and has not
            failed, closed, or, for a read, got its rows (`FINAL_STATES`), in registration
            order.
        """
        return [
            (stream_id, worker.fail)
            for stream_id, worker in self._workers.items()
            if not worker.shut_down and worker.row.state not in FINAL_STATES
        ]

    def close_rows(self, close_row: Callable[[StreamRow], StreamRow]) -> None:
        """Mark every registered worker table's row closed, for `close()`.

        `registry_lock` is held. Each row is replaced by `close_row`'s and written, in
        registration order, even one `close_row` leaves as it was.

        Args:
            close_row: Returns a table's closed row from its current one.
        """
        for stream_id in tuple(self._workers):
            self.change_row(stream_id, close_row)
