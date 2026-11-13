"""Tests for `WorkerTables`, a session's `poll()` tables and background and followed reads.

Most of the registry is exercised through those tables' own tests (`test_poll.py`,
`test_background_read.py`, `test_followed_read.py`); these cover what they cannot reach: a
followed read's progress after the close, a shutdown after the session was freed,
registrations racing the close, and an interrupt that lands after a row is stored.
"""

from __future__ import annotations

import gc
import threading
import weakref
from typing import TYPE_CHECKING, Any

import pytest
from deephaven import empty_table

import deephaven_massive as dm
from deephaven_massive._errors import MassiveError
from deephaven_massive._massive import Transport
from deephaven_massive._tables import live_set
from deephaven_massive._tables import worker_tables as _worker_tables
from deephaven_massive._tables.diagnostics import (
    Diagnostics,
    StreamKind,
    StreamRow,
    StreamState,
    StreamUpdate,
)
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import new_scope

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven_massive._tables.worker_tables import WorkerTables

TIMEOUT = 30.0


def _read_row(call: int) -> StreamRow:
    # A followed read's first row, as `open_followed_read` registers it.
    return StreamRow(
        stream_id=call,
        kind=StreamKind.READ,
        dataset="test.trades",
        query="m.test.trades(<table>)",
        symbol_count=0,
        symbols="",
        held_results=1,
        transport=Transport.REST,
        state=StreamState.LOADING,
        loading=0,
        created_at=1,
    )


def test_a_followed_read_s_progress_after_close_changes_nothing(
    massive_session: dm.MassiveSession,
) -> None:
    # The coordinator may report progress after the close: the row keeps what the close wrote.
    core = massive_session._core
    call = core.next_stream_id()
    core.workers._register(core, _read_row(call), fail=lambda _error: True, registered=[])
    massive_session.close()
    with core.registry_lock:
        closed = core.workers._workers[call].row
    core.workers.update_followed_read(call, StreamUpdate(rows_published=7, loading=1), loading=True)
    with core.registry_lock:
        assert core.workers._workers[call].row == closed
    assert closed.state is StreamState.CLOSED
    core.workers.release(call)  # as releasing the result, then its shutdown, do
    core.workers.shutdown_callback(call, threading.Event())()


def test_a_worker_shutdown_after_its_session_was_freed_only_stops_the_thread() -> None:
    # A finished background read's record leaves the session once its result is released, so
    # its session may be freed while a derived table still uses the read's blink table; the
    # table's shutdown callback then has no session to tell.
    class Gone:
        pass

    gone = Gone()
    tables_ref: weakref.ref[WorkerTables] = weakref.ref(gone)  # type: ignore[arg-type]  # it dies
    del gone
    stop = threading.Event()
    told: list[int] = []
    _worker_tables._worker_table_shut_down(
        tables_ref, 7, record=lambda _, stream_id: told.append(stream_id), stop=stop
    )
    assert stop.is_set()
    assert told == []


@pytest.mark.parametrize("attempt", range(5))
def test_a_registration_racing_close_is_closed_or_refused(
    massive_session: dm.MassiveSession, attempt: int
) -> None:
    # The registry reads the core's closed flag, and the close marks the registry's rows, each
    # under the core's `registry_lock`: every table registered before the close is marked
    # CLOSED and failed by it, and every one after is refused, whatever the interleaving.
    del attempt
    core = massive_session._core
    calls = [core.next_stream_id() for _ in range(8)]
    lock = threading.Lock()
    registered: list[int] = []
    refused: list[int] = []
    failed: list[int] = []
    start = threading.Barrier(len(calls) + 1)

    def register(call: int) -> None:
        def fail(_error: MassiveError) -> bool:
            with lock:
                failed.append(call)
            return True

        start.wait(TIMEOUT)
        try:
            core.workers._register(core, _read_row(call), fail=fail, registered=[])
        except MassiveError:
            with lock:
                refused.append(call)
        else:
            with lock:
                registered.append(call)

    threads = [threading.Thread(target=register, args=(call,)) for call in calls]
    for thread in threads:
        thread.start()
    start.wait(TIMEOUT)
    massive_session.close()
    for thread in threads:
        thread.join(TIMEOUT)
    assert sorted(registered + refused) == calls
    assert sorted(failed) == sorted(registered)
    with core.registry_lock:
        states = {call: core.workers._workers[call].row.state for call in registered}
    assert set(states.values()) <= {StreamState.CLOSED}
    for call in registered:  # as releasing each result, then its shutdown, do
        core.workers.release(call)
        core.workers.shutdown_callback(call, threading.Event())()
    with core.registry_lock:
        assert core.workers._workers == {}
    assert id(core) not in live_set._live


def test_a_shutdown_that_outlives_its_session_records_nothing() -> None:
    # A shutdown callback may take the registry just before the session is freed on another
    # thread; the registry then has no session to tell, and returns.
    m = dm.MassiveSession(dm.MassiveConfig(api_key="outlived-shutdown"))
    tables = m._core.workers
    alive = weakref.ref(m._core)
    del m
    gc.collect()
    assert alive() is None
    tables._shut_down(7)
    with tables._lock:
        assert tables._workers == {}


class _UnstartedWorker:
    # A worker as `WorkerTables.track` sees it; its thread is never started.

    def __init__(self) -> None:
        self.abandoned = False

    def fail(self, error: MassiveError) -> bool:
        del error
        return True

    def release(self) -> None:
        pass

    def abandon(self) -> None:
        self.abandoned = True


@pytest.mark.parametrize("step", ["writing the row", "updating the live set"])
def test_an_interrupt_after_the_row_is_stored_still_releases_it(
    *,
    massive_session: dm.MassiveSession,
    monkeypatch: pytest.MonkeyPatch,
    stream_rows: Callable[[], list[dict[str, Any]]],
    step: str,
) -> None:
    # A KeyboardInterrupt (Control-C in the console) can land in `_register` after the record
    # is stored, while the row is written or the live set updated; the row is released anyway,
    # so only the blink table's shutdown is left to take the record out of the registry.
    core = massive_session._core
    call = core.next_stream_id()
    row = _read_row(call)
    owner, name, arg = (
        (Diagnostics, "write", 1) if step == "writing the row" else (live_set, "sync", 0)
    )
    ours = row if step == "writing the row" else core
    original = getattr(owner, name)
    interrupted: list[bool] = []

    def interrupt_once(*args: object) -> None:
        if args[arg] is ours and not interrupted:
            interrupted.append(True)
            raise KeyboardInterrupt
        original(*args)

    monkeypatch.setattr(owner, name, interrupt_once)
    worker = _UnstartedWorker()
    with entered(core.exec_ctx):
        result = empty_table(0)
    with pytest.raises(KeyboardInterrupt):
        core.workers.track(result, new_scope(), lambda: (worker, row))
    monkeypatch.undo()
    assert interrupted
    assert worker.abandoned
    with core.registry_lock:
        state = core.workers._workers[call]
    assert (state.held, state.row.state) == (False, StreamState.RELEASED)
    core.workers.shutdown_callback(call, threading.Event())()  # as the blink table's shutdown
    with core.registry_lock:
        assert core.workers._workers == {}
    assert id(core) not in live_set._live
    (shown,) = stream_rows()  # holding m.streams() puts the session back in the live set
    assert (shown["State"], shown["HeldResults"]) == ("RELEASED", 0)
