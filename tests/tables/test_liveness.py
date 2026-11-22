"""Tests for result scopes, records, and the reaper, in the embedded JVM.

The stream tests use a `FakeStreamTransport` whose streams are `blink_publisher` blink tables
without a network, so they observe exactly when Deephaven runs a stream's `on_shutdown_callback`.
"""

import gc
import logging
import threading
import time
import weakref
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Protocol

import pytest
from deephaven import empty_table, time_table
from deephaven.dherror import DHError
from deephaven.execution_context import ExecutionContext
from deephaven.liveness_scope import LivenessScope
from deephaven.table import Table
from deephaven.update_graph import shared_lock
from typing_extensions import override

import deephaven_massive as dm
from deephaven_massive._datasets.catalog import DatasetSpec, KeySpec, SourceKeys
from deephaven_massive._enums import AssetClass, NotBars
from deephaven_massive._errors import LIBRARY_BUG
from deephaven_massive._massive import Transport
from deephaven_massive._tables.execution_context import entered, holds_update_graph_lock
from deephaven_massive._tables.liveness import (
    QueueWorker,
    Reaper,
    ResultRecord,
    ResultRegistry,
    make_result,
    new_scope,
    release_scope,
    weak_owner_callback,
)
from deephaven_massive._tables.transports import StreamHandle
from deephaven_massive.namespaces._query_building import build_query
from deephaven_massive.queries import StreamQuery

if TYPE_CHECKING:
    from deephaven_massive._tables.session_core import SessionCore
    from tests._fakes.ws import FakeMassiveServer, TradeFeed
    from tests._fakes.ws_session import WsSession


class FakeStreams(Protocol):
    # What the tests read from the `fake_streams` fixture (tests/conftest.py).
    opened: list[tuple[str, str | None]]
    shutdown: dict[int, threading.Event]
    shutdown_calls: dict[int, int]


TIMEOUT = 30.0

SPEC = DatasetSpec(
    name="test.trades",
    summary="A dataset for the tests.",
    asset_class=AssetClass.STOCKS,
    channels={NotBars.ANY: "T"},
    keys={(Transport.WS, "stocks/T"): SourceKeys(KeySpec("Ticker"), KeySpec("SipTimestamp"))},
)


def query(session: dm.MassiveSession, symbols: list[str] | None = None) -> StreamQuery:
    return build_query(session._core, SPEC, query_type=StreamQuery, symbols=symbols or ["AAPL"])


# --- Components ------------------------------------------------------------------------------


def test_new_scope_is_a_liveness_scope() -> None:
    scope = new_scope()
    assert isinstance(scope, LivenessScope)
    scope.release()


class _RecordingScope:
    # Stands in for a scope: records whether its release ran under the update graph's lock.
    def __init__(self, exec_ctx: ExecutionContext, *, fail: bool = False) -> None:
        self._exec_ctx = exec_ctx
        self._fail = fail
        self.locked: list[bool] = []

    def release(self) -> None:
        self.locked.append(holds_update_graph_lock(self._exec_ctx))
        if self._fail:
            raise RuntimeError("released twice")


class _Owner:
    def __init__(self) -> None:
        self.released: list[int] = []

    def release(self, stream_id: int) -> None:
        self.released.append(stream_id)


def test_an_owner_callback_runs_the_method_and_holds_its_owner_weakly() -> None:
    owner = _Owner()
    callback = weak_owner_callback(owner, _Owner.release, 7)
    alive = weakref.ref(owner)
    callback()
    assert owner.released == [7]
    del owner
    assert alive() is None  # the callback kept no strong reference


def test_release_scope_holds_the_update_graph_lock(dh_exec_ctx: ExecutionContext) -> None:
    # A release can destroy a refreshing table; under the lock, no update cycle runs meanwhile.
    scope = _RecordingScope(dh_exec_ctx)
    recorded: LivenessScope = scope  # type: ignore[assignment]  # a fake that records the lock
    release_scope(dh_exec_ctx, recorded)
    assert scope.locked == [True]
    assert not holds_update_graph_lock(dh_exec_ctx)  # let go after the release
    failing = _RecordingScope(dh_exec_ctx, fail=True)
    broken: LivenessScope = failing  # type: ignore[assignment]  # a fake whose release fails
    with pytest.raises(RuntimeError, match=r"^released twice$"):  # its own error, not wrapped
        release_scope(dh_exec_ctx, broken)
    assert failing.locked == [True]
    assert not holds_update_graph_lock(dh_exec_ctx)  # let go after a failed release too


def test_release_scope_destroys_a_refreshing_table(dh_exec_ctx: ExecutionContext) -> None:
    scope = new_scope()
    with scope.open():
        ticking = time_table("PT1S")
    release_scope(dh_exec_ctx, scope)
    assert not _alive(ticking)


def test_release_scope_in_a_console_command_takes_the_lock_again(
    dh_exec_ctx: ExecutionContext, console_command: Callable[[Callable[[], None]], None]
) -> None:
    # A console command holds the exclusive lock; it takes the shared lock again at once.
    scope = _RecordingScope(dh_exec_ctx)
    recorded: LivenessScope = scope  # type: ignore[assignment]  # a fake that records the lock
    console_command(lambda: release_scope(dh_exec_ctx, recorded))
    assert scope.locked == [True]


def test_a_reaper_release_waits_for_a_console_command(
    dh_exec_ctx: ExecutionContext, console_command: Callable[[Callable[[], None]], None]
) -> None:
    # The reaper takes the shared lock, so a release queued while a console command holds the
    # exclusive lock runs once the command ends; no lock holder waits for the reaper.
    reaper = Reaper(dh_exec_ctx, "test-reaper")
    registry = ResultRegistry(threading.RLock(), reaper, dh_exec_ctx)
    released = threading.Event()
    table, scope = make_result(dh_exec_ctx, lambda: time_table("PT1S").where("true"))
    registry.track(table, scope, released.set)
    during: list[bool] = []

    def command() -> None:
        assert registry.release(table)
        during.append(released.is_set())

    console_command(command)
    assert during == [False]
    assert released.wait(TIMEOUT)
    assert not _alive(table)
    reaper.stop()


def test_make_result_manages_the_result(dh_exec_ctx: ExecutionContext) -> None:
    result, scope = make_result(dh_exec_ctx, lambda: empty_table(1).update("X = i"))
    assert result.size == 1
    scope.release()


def _alive(table: Table) -> bool:
    # Whether the Java table can still be retained, that is, has not been released for good.
    if table.j_table.tryRetainReference():
        table.j_table.dropReference()
        return True
    return False


def test_a_result_outlives_the_scope_of_its_source(dh_exec_ctx: ExecutionContext) -> None:
    user = new_scope()
    with user.open():
        source = time_table("PT1S")  # refreshing, so its liveness is observable
    result, scope = make_result(dh_exec_ctx, lambda: source.where("true"))
    assert result != source  # a new table, managed by the result scope
    user.release()
    assert _alive(result)  # the result scope now holds the only reference
    scope.release()
    assert not _alive(result)


def test_make_result_releases_the_scope_on_failure(
    dh_exec_ctx: ExecutionContext, monkeypatch: pytest.MonkeyPatch
) -> None:
    scopes: list[LivenessScope] = []
    real = new_scope

    def tracking() -> LivenessScope:
        scope = real()
        scopes.append(scope)
        return scope

    monkeypatch.setattr("deephaven_massive._tables.liveness.new_scope", tracking)

    def fail() -> Table:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        make_result(dh_exec_ctx, fail)
    assert scopes[0].j_scope is None  # released

    def interrupted() -> Table:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        make_result(dh_exec_ctx, interrupted)
    assert scopes[1].j_scope is None  # released on an interrupt too


def test_reaper_runs_jobs_in_order_and_survives_failures(
    dh_exec_ctx: ExecutionContext, caplog: pytest.LogCaptureFixture
) -> None:
    reaper = Reaper(dh_exec_ctx, "test-reaper")
    seen: list[int] = []
    done = threading.Event()

    def fail() -> None:
        raise RuntimeError("job failed")

    with caplog.at_level(logging.ERROR, logger="deephaven_massive._tables.liveness"):
        reaper.submit(lambda: seen.append(1))
        reaper.submit(fail)
        reaper.submit(lambda: seen.append(2))
        reaper.submit(done.set)
        assert done.wait(TIMEOUT)
    assert seen == [1, 2]
    assert [(r.threadName, r.getMessage()) for r in caplog.records] == [
        (
            "test-reaper",
            "Releasing a live table or its resources failed. The library goes on. " + LIBRARY_BUG,
        )
    ]
    reaper.stop()
    reaper.thread.join(TIMEOUT)
    assert not reaper.thread.is_alive()


def test_registry_release_checks_identity(dh_exec_ctx: ExecutionContext) -> None:
    reaper = Reaper(dh_exec_ctx, "test-reaper")
    registry = ResultRegistry(threading.RLock(), reaper, dh_exec_ctx)
    released: list[str] = []
    table, scope = make_result(dh_exec_ctx, lambda: empty_table(1))
    registry.track(table, scope, lambda: released.append("released"))
    assert registry.held() == 1
    other = empty_table(1)
    assert not registry.release(other)
    # A record whose id now belongs to another object (a recycled id) does not match.
    registry._results[id(other)] = registry._results[id(table)]
    assert not registry.release(other)
    assert registry.release(table)
    assert registry.release(table)  # a second release is a no-op
    # Collecting a wrapper whose id was re-registered leaves the newer record in place.
    collected, collected_scope = make_result(dh_exec_ctx, lambda: empty_table(1))
    registry.track(collected, collected_scope, lambda: released.append("collected"))
    newer = registry._results[id(table)]
    registry._results[id(collected)] = newer
    del collected
    gc.collect()
    done = threading.Event()
    reaper.submit(done.set)
    assert done.wait(TIMEOUT)
    assert released == ["released", "collected"]
    assert registry.held() == 0
    reaper.stop()


class _Interrupting(dict[int, ResultRecord]):
    # A registry dictionary whose insertion is interrupted, like a KeyboardInterrupt delivered
    # while `track` registers a record.
    @override
    def __setitem__(self, key: int, value: ResultRecord) -> None:
        raise KeyboardInterrupt


def test_an_interrupted_track_registers_nothing_and_never_releases(
    dh_exec_ctx: ExecutionContext,
) -> None:
    reaper = Reaper(dh_exec_ctx, "test-reaper")
    registry = ResultRegistry(threading.RLock(), reaper, dh_exec_ctx)
    registry._results = _Interrupting()
    released: list[str] = []
    table, scope = make_result(dh_exec_ctx, lambda: empty_table(1))
    with pytest.raises(KeyboardInterrupt):
        registry.track(table, scope, lambda: released.append("released"))
    assert registry.held() == 0
    del table
    gc.collect()  # the detached finalizer queues nothing
    done = threading.Event()
    reaper.submit(done.set)
    assert done.wait(TIMEOUT)
    assert released == []  # the caller still owns the scope
    scope.release()
    reaper.stop()


# --- Streams through a session ---------------------------------------------------------------


def test_shutdown_runs_after_the_last_result_is_released(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    drain: Callable[[], None],
) -> None:
    q = query(massive_session)
    a = q.append_only()
    b = q.last_by()
    assert len(fake_streams.opened) == 1
    massive_session.release(a)
    drain()
    assert not fake_streams.shutdown[1].is_set()
    massive_session.release(b)
    assert fake_streams.shutdown[1].wait(TIMEOUT)


def test_a_derived_table_keeps_the_stream_alive(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    drain: Callable[[], None],
) -> None:
    result = query(massive_session).append_only()
    user_scope = new_scope()
    with user_scope.open():
        derived = result.tail(100)  # not a formula, which would capture these locals
    massive_session.release(result)
    drain()
    assert not fake_streams.shutdown[1].is_set()
    assert derived.is_refreshing
    user_scope.release()
    assert fake_streams.shutdown[1].wait(TIMEOUT)


def test_blink_returns_distinct_tables_and_records(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    drain: Callable[[], None],
) -> None:
    q = query(massive_session)
    a = q.blink()
    b = q.blink()
    assert a is not b
    assert a != b  # two described copies of the stream's one blink table
    assert a.is_blink
    assert b.is_blink
    records = massive_session._core.results._results
    assert records[id(a)] is not records[id(b)]
    record_b = records[id(b)]
    del a
    gc.collect()
    drain()
    assert not record_b.released
    assert not fake_streams.shutdown[1].is_set()
    assert b.is_refreshing
    massive_session.release(b)
    assert fake_streams.shutdown[1].wait(TIMEOUT)


def test_garbage_collection_releases(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    drain: Callable[[], None],
) -> None:
    result = query(massive_session).ring(10)
    assert massive_session._core.results.held() == 1
    del result
    gc.collect()
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    drain()
    assert massive_session._core.results.held() == 0


def test_double_release_runs_one_shutdown(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    drain: Callable[[], None],
) -> None:
    result = query(massive_session).append_only()
    massive_session.release(result)
    massive_session.release(result)
    del result
    gc.collect()
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    drain()
    assert fake_streams.shutdown_calls == {1: 1}


def wait_for(condition: Callable[[], bool]) -> None:
    # A bounded wait for background work (the reaper, an update-graph cycle) to land.
    pause = threading.Event()  # never set; only paces the loop
    for _ in range(int(TIMEOUT / 0.02)):
        if condition():
            return
        pause.wait(0.02)
    assert condition()


def test_close_fails_results_and_keeps_diagnostics_views(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    result = query(massive_session).append_only()
    streams = massive_session.streams()
    massive_session.close()
    with pytest.raises(dm.MassiveError, match="is closed"):
        massive_session.streams()
    assert streams.snapshot().size == 1  # a view taken before the close keeps its rows
    # The live table failed with the close, keeps its rows, and refuses new operations. The
    # blink table's shutdown callback runs inside the update cycle that delivers the failure,
    # before the cycle passes it on to the result derived from the blink table: the shared lock
    # waits for that cycle to end.
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    with shared_lock(result):
        failed = bool(result.j_table.isFailed())  # pytest cannot describe a Java method
        assert failed
    assert result.snapshot().size == 0
    with pytest.raises(DHError, match="failed table"):
        result.tail(1)
    # Garbage collection still releases results after close().
    core = massive_session._core
    del result
    gc.collect()
    wait_for(lambda: core.results.held() == 1)  # only the diagnostics view is left
    del streams
    gc.collect()
    wait_for(lambda: core.results.held() == 0)


def test_a_failed_build_drops_its_hold(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    core = massive_session._core
    q = query(massive_session)

    def broken(_handle: StreamHandle) -> Table:
        raise RuntimeError("cannot shape")

    with pytest.raises(RuntimeError, match="cannot shape"):
        core.streams.result(
            SPEC, q._request, channel="T", query="<Query test>", verb="blink", build=broken
        )
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    assert core.streams._stream_ids == {}


def test_an_interrupted_build_drops_its_hold(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    core = massive_session._core
    q = query(massive_session)

    def interrupted(_handle: StreamHandle) -> Table:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        core.streams.result(
            SPEC, q._request, channel="T", query="<Query test>", verb="blink", build=interrupted
        )
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    assert core.streams._stream_ids == {}


def test_streams_are_shared_then_reopened(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    q = query(massive_session, ["MSFT", "AAPL"])
    first = q.append_only()
    second = query(massive_session, ["AAPL", "MSFT"]).last_by()
    other = query(massive_session, ["NVDA"]).blink()
    assert fake_streams.opened == [("test.trades", "T"), ("test.trades", "T")]
    for table in (first, second, other):
        massive_session.release(table)
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    assert fake_streams.shutdown[2].wait(TIMEOUT)
    again = q.append_only()
    assert len(fake_streams.opened) == 3
    massive_session.release(again)
    assert fake_streams.shutdown[3].wait(TIMEOUT)


def test_concurrent_verbs_share_one_stream(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    *,
    drain: Callable[[], None],
    stream_rows: Callable[[], list[dict[str, Any]]],
) -> None:
    q = query(massive_session)
    results: list[Table] = []
    lock = threading.Lock()
    start = threading.Barrier(16)

    def worker() -> None:
        start.wait(TIMEOUT)
        with entered(massive_session._core.exec_ctx):
            table = q.blink()
        with lock:
            results.append(table)

    threads = [threading.Thread(target=worker) for _ in range(16)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(TIMEOUT)
    assert len(results) == 16
    # One open per key: the racing verbs wait for the first open and share its stream.
    assert len(fake_streams.opened) == 1
    ((winner, held),) = [(r["StreamId"], r["HeldResults"]) for r in stream_rows()]
    assert held == 16
    workers = [threading.Thread(target=massive_session.release, args=(t,)) for t in results]
    for thread in workers:
        thread.start()
    for thread in workers:
        thread.join(TIMEOUT)
    assert fake_streams.shutdown[winner].wait(TIMEOUT)
    drain()
    assert fake_streams.shutdown_calls == dict.fromkeys(fake_streams.shutdown, 1)


def test_collection_takes_no_lock(dh_exec_ctx: ExecutionContext) -> None:
    # Garbage collection can run the finalizer on a thread that holds another library lock, so
    # it must not take registry_lock; it only queues the release on the reaper.
    reaper = Reaper(dh_exec_ctx, "test-reaper")
    lock = threading.RLock()
    registry = ResultRegistry(lock, reaper, dh_exec_ctx)
    released = threading.Event()
    table, scope = make_result(dh_exec_ctx, lambda: empty_table(1))
    registry.track(table, scope, released.set)
    holding, done = threading.Event(), threading.Event()

    def hold() -> None:
        with lock:
            holding.set()
            done.wait(TIMEOUT)

    holder = threading.Thread(target=hold)
    holder.start()
    assert holding.wait(TIMEOUT)
    del table
    gc.collect()  # would block here if the finalizer took the held lock
    assert not released.is_set()  # the reaper job is waiting for the lock
    done.set()
    holder.join(TIMEOUT)
    assert released.wait(TIMEOUT)
    reaper.stop()


def test_a_failing_scope_release_still_notifies_the_owner(dh_exec_ctx: ExecutionContext) -> None:
    class BrokenScope:
        def release(self) -> None:
            raise RuntimeError("cannot release")

    notified: list[str] = []
    broken: LivenessScope = BrokenScope()  # type: ignore[assignment]  # a fake whose release fails
    record = ResultRecord(broken, lambda: notified.append("released"))
    reaper = Reaper(dh_exec_ctx, "unused-reaper")  # the reap under test never uses it
    with pytest.raises(RuntimeError, match="cannot release"):
        ResultRegistry(threading.RLock(), reaper, dh_exec_ctx)._reap(record)
    assert notified == ["released"]
    reaper.stop()


class _Fatal(BaseException):
    # A BaseException, like KeyboardInterrupt, that a worker's batch handler does not catch.
    pass


def test_queue_worker_logs_a_fatal_exit_before_dying(
    dh_exec_ctx: ExecutionContext,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    escaped: list[threading.ExceptHookArgs] = []
    # The thread's exception must still escape; collect it instead of pytest's warning.
    monkeypatch.setattr(threading, "excepthook", escaped.append)

    def fatal(_batch: list[int]) -> None:
        raise _Fatal

    with caplog.at_level(logging.ERROR, logger="deephaven_massive._tables.liveness"):
        worker: QueueWorker[int] = QueueWorker(dh_exec_ctx, "fatal-worker", handle=fatal)
        worker.put(1)
        worker.thread.join(TIMEOUT)
    assert not worker.thread.is_alive()
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.ERROR, f"A library thread died, so its session's live tables fail. {LIBRARY_BUG}")
    ]
    assert [r.threadName for r in caplog.records] == ["fatal-worker"]
    assert [args.exc_type for args in escaped] == [_Fatal]


def test_queue_worker_hands_its_death_to_its_owner(
    dh_exec_ctx: ExecutionContext,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    escaped: list[threading.ExceptHookArgs] = []
    monkeypatch.setattr(threading, "excepthook", escaped.append)
    deaths: list[BaseException] = []

    def fatal(_batch: list[int]) -> None:
        raise _Fatal

    def broken(death: BaseException) -> None:
        deaths.append(death)
        raise RuntimeError("cannot handle it")

    for name, on_death in (("told-worker", deaths.append), ("broken-worker", broken)):
        with caplog.at_level(logging.ERROR, logger="deephaven_massive._tables.liveness"):
            worker: QueueWorker[int] = QueueWorker(
                dh_exec_ctx, name, handle=fatal, on_death=on_death
            )
            worker.put(1)
            worker.thread.join(TIMEOUT)
        assert not worker.thread.is_alive()
    assert [type(death) for death in deaths] == [_Fatal, _Fatal]
    died = f"A library thread died, so its session's live tables fail. {LIBRARY_BUG}"
    assert [(r.threadName, r.getMessage()) for r in caplog.records] == [
        ("told-worker", died),
        ("broken-worker", died),
        (
            "broken-worker",
            "Failing the live tables after a library thread died raised an error. " + LIBRARY_BUG,
        ),
    ]
    assert [args.exc_type for args in escaped] == [_Fatal, _Fatal]  # still re-raised


def test_queue_worker_runs_on_exit_and_logs_batch_failures(
    dh_exec_ctx: ExecutionContext, caplog: pytest.LogCaptureFixture
) -> None:
    exited = threading.Event()

    def fail(batch: list[int]) -> None:
        raise RuntimeError(f"bad batch {batch}")

    with caplog.at_level(logging.ERROR, logger="deephaven_massive._tables.liveness"):
        worker: QueueWorker[int] = QueueWorker(
            dh_exec_ctx, "test-worker", handle=fail, on_exit=exited.set
        )
        worker.put(1)
        worker.stop()
        worker.thread.join(TIMEOUT)
    assert exited.is_set()
    assert [(r.threadName, r.getMessage()) for r in caplog.records] == [
        ("test-worker", f"A batch of queued work failed. The library goes on. {LIBRARY_BUG}")
    ]


def test_queue_worker_wakes_on_its_timeout_and_logs_a_failing_wake_up(
    dh_exec_ctx: ExecutionContext, caplog: pytest.LogCaptureFixture
) -> None:
    waits = [0.0, 0.0, None]  # wake at once twice, then wait for an item
    woken = threading.Semaphore(0)

    def timeout() -> float | None:
        return waits.pop(0) if waits else None

    def wake_up() -> None:
        woken.release()
        if len(waits) == 1:  # the first wake-up fails, and the worker carries on
            raise RuntimeError("wake-up broke")

    handled: list[int] = []
    with caplog.at_level(logging.ERROR, logger="deephaven_massive._tables.liveness"):
        worker: QueueWorker[int] = QueueWorker(
            dh_exec_ctx, "timed-worker", handle=handled.extend, timeout=timeout, on_timeout=wake_up
        )
        assert woken.acquire(timeout=TIMEOUT)
        assert woken.acquire(timeout=TIMEOUT)
        worker.put(7)
        worker.stop()
        worker.thread.join(TIMEOUT)
    assert handled == [7]
    assert [(r.threadName, r.getMessage()) for r in caplog.records] == [
        ("timed-worker", f"A timed wake-up failed. The library goes on. {LIBRARY_BUG}")
    ]


def test_collection_removes_the_record_and_keeps_a_newer_one(
    dh_exec_ctx: ExecutionContext,
) -> None:
    reaper = Reaper(dh_exec_ctx, "test-reaper")
    registry = ResultRegistry(threading.RLock(), reaper, dh_exec_ctx)
    table, scope = make_result(dh_exec_ctx, lambda: empty_table(1))
    key = id(table)
    registry.track(table, scope, lambda: None)
    del table
    gc.collect()
    done = threading.Event()
    reaper.submit(done.set)
    assert done.wait(TIMEOUT)
    assert key not in registry._results  # a collected result leaves no record behind
    # A recycled id: the collected wrapper's key now belongs to a newer record.
    old, old_scope = make_result(dh_exec_ctx, lambda: empty_table(1))
    registry.track(old, old_scope, lambda: None)
    newer, newer_scope = make_result(dh_exec_ctx, lambda: empty_table(1))
    newer_record = registry.track(newer, newer_scope, lambda: None)
    recycled = id(old)
    registry._results[recycled] = newer_record
    del old
    gc.collect()
    done = threading.Event()
    reaper.submit(done.set)
    assert done.wait(TIMEOUT)
    assert registry._results.get(recycled) is newer_record  # the newer record stays
    reaper.stop()


class _InterruptingExit:
    # Stands in for the registry's lock: raises KeyboardInterrupt at `__exit__` once a record
    # has been stored, the checkpoint between `track`'s insert and its return.
    def __init__(self, real: threading.RLock, registry: ResultRegistry) -> None:
        self.real = real
        self.registry = registry
        self.armed = True

    def __enter__(self) -> bool:
        return self.real.__enter__()

    def __exit__(self, *exc: object) -> None:
        self.real.__exit__(None, None, None)
        if self.armed and self.registry._results:
            self.armed = False
            raise KeyboardInterrupt


def test_an_interrupt_after_the_record_is_stored_leaves_no_stale_record(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    registry = massive_session._core.results
    real = registry._lock
    monkeypatch.setattr(registry, "_lock", _InterruptingExit(real, registry))
    with pytest.raises(KeyboardInterrupt):
        query(massive_session).blink()
    monkeypatch.setattr(registry, "_lock", real)
    assert registry._results == {}  # the caller released the scope: nothing stays registered
    assert registry.held() == 0
    assert fake_streams.shutdown[1].wait(TIMEOUT)


def test_an_interrupt_right_after_the_finalizer_is_created_never_releases_twice(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The interrupt lands once the finalizer is attached but before `track` can manage it:
    # the verb releases the result itself, so the finalizer must not release it again when
    # the dead result is collected.
    core = massive_session._core
    first = query(massive_session).blink()
    real = weakref.finalize
    armed = [True]

    def creating(obj: object, func: Callable[..., object], /, *args: object) -> object:
        made = real(obj, func, *args)
        if armed:
            armed.clear()
            raise KeyboardInterrupt
        return made

    monkeypatch.setattr(weakref, "finalize", creating)  # as liveness sees it
    with pytest.raises(KeyboardInterrupt):
        query(massive_session).append_only()
    monkeypatch.setattr(weakref, "finalize", real)
    gc.collect()
    done = threading.Event()
    core._reaper.submit(done.set)
    assert done.wait(TIMEOUT)
    assert core.streams._streams[1].row.held_results == 1  # `first` still holds the stream
    massive_session.release(first)
    assert fake_streams.shutdown[1].wait(TIMEOUT)


# --- Soak test: a long run under load (marker `soak`, left out of a plain run) ---------------


class Soak(Protocol):
    # What the soak test uses of the `soak` fixture (tests/conftest.py).
    seconds: float
    running: threading.Event

    def rounds(self) -> Iterator[int]: ...

    def note(self, **figures: object) -> None: ...


SOAK_SYMBOLS = [f"S{i:02d}" for i in range(40)]
"""The symbols the soak feed trades."""

SOAK_HELD = 64
"""How many results and streams the session may hold at the end of a soak round: the eight
threads hold one each, and the rest are on their way out (a release waits for the reaper, and a
stream's record goes when its blink table shuts down, on the next update cycle). A result or
stream left behind per step grows past it within seconds."""


def soak_settle(core: "SessionCore", condition: Callable[[], bool], what: str) -> None:
    # Wait for the reaper's queued releases, then until `condition` holds; fail after a while.
    done = threading.Event()
    core._reaper.submit(done.set)
    assert done.wait(TIMEOUT), "the reaper did not catch up"
    pause = threading.Event()  # never set; only paces the polling
    deadline = time.monotonic() + 6 * TIMEOUT
    while not condition():
        assert time.monotonic() < deadline, f"waited for {what}"
        pause.wait(0.1)


def open_and_let_go(harness: "WsSession", worker: int, step: int) -> None:
    # One step of the release soak: open a result of one of the four shapes for one symbol, hold
    # it for a moment, then let it go, by `m.release` or, every other step, by dropping it for
    # the garbage collector and the reaper.
    q = query(harness.session, [SOAK_SYMBOLS[(worker * 5 + step * 3) % 40]])
    shape = step % 4
    result = (q.blink, q.append_only, q.last_by, lambda: q.ring(10))[shape]()
    threading.Event().wait((0.02, 0.1, 0.05)[step % 3])  # never set; only paces the step
    if (worker + step) % 2:
        harness.session.release(result)


@pytest.mark.soak
def test_soak_released_and_collected_results_free_their_streams(
    make_ws_session: "Callable[..., WsSession]",
    *,
    make_trade_feed: "Callable[..., TradeFeed]",
    fake_ws_server: "FakeMassiveServer",
    soak: Soak,
) -> None:
    # Eight threads keep opening results of every shape (blink, append-only, last-by, ring)
    # under a fast feed and letting them go, half through `m.release` and half by dropping them
    # for the garbage collector and the reaper. After each round the session may hold only a
    # handful of results and streams; at the end it holds none, and the connection no topic.
    fake_ws_server.keep_frames = False
    harness = make_ws_session(flush_interval=0.05)
    core = harness.session._core
    feed = make_trade_feed(SOAK_SYMBOLS)
    feed.start()
    steps = [0] * 8

    def churn(worker: int) -> None:
        while soak.running.is_set():
            steps[worker] += 1
            open_and_let_go(harness, worker, steps[worker])

    with ThreadPoolExecutor(8, thread_name_prefix="soak release") as pool:
        workers = [pool.submit(churn, worker) for worker in range(8)]
        try:
            for _ in soak.rounds():
                soak_settle(
                    core,
                    lambda: (
                        core.results.held() <= SOAK_HELD and len(core.streams._streams) <= SOAK_HELD
                    ),
                    f"at most {SOAK_HELD} results and streams",
                )
                soak.note(results_opened=sum(steps), rows=sum(feed.sent().values()))
        finally:
            soak.running.clear()  # a failed round stops the workers too
            for worker in workers:
                worker.result(6 * TIMEOUT)
    feed.stop()
    soak_settle(
        core,
        lambda: core.results.held() == 0 and not core.streams._streams,
        "every result and stream to go",
    )
    soak_settle(core, lambda: not harness.connections.group.streams, "the routes to empty")

    def topics() -> set[str]:
        with fake_ws_server._lock:
            return set(fake_ws_server.connections[-1].topics)

    soak_settle(core, lambda: not topics(), "every topic to be unsubscribed")
