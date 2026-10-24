"""Tests for result scopes, records, and the reaper (DESIGN § 6.1), in the embedded JVM.

The stream tests use a `FakeStreamTransport` whose streams are bare `table_publisher` blink
tables, so they observe exactly when Deephaven runs a stream's `on_shutdown_callback`.
"""

import gc
import logging
import threading
from collections.abc import Callable
from typing import Any, Protocol, cast

import pytest
from deephaven import empty_table
from deephaven.dherror import DHError
from deephaven.execution_context import ExecutionContext, get_exec_ctx
from deephaven.liveness_scope import LivenessScope
from deephaven.table import Table

import deephaven_massive as dm
from deephaven_massive._catalog import DatasetSpec
from deephaven_massive._domains.base import build_query
from deephaven_massive._enums import AssetClass
from deephaven_massive._liveness import (
    QueueWorker,
    Reaper,
    ResultRecord,
    ResultRegistry,
    entered,
    make_result,
    new_scope,
)
from deephaven_massive._query import StreamQuery
from deephaven_massive._schema import TRADES
from deephaven_massive._types import override


class FakeStreams(Protocol):
    # What the tests read from the `fake_streams` fixture (tests/conftest.py).
    opened: list[tuple[str, str | None]]
    shutdown: dict[int, threading.Event]
    shutdown_calls: dict[int, int]


TIMEOUT = 30.0

SPEC = DatasetSpec(
    name="test.trades",
    query_class=StreamQuery,
    asset_class=AssetClass.STOCKS,
    schema=TRADES.schema,
    channels={None: "T"},
)


def query(session: dm.MassiveSession, symbols: list[str] | None = None) -> StreamQuery:
    q = build_query(session._core, SPEC, symbols=symbols or ["AAPL"])
    assert isinstance(q, StreamQuery)
    return q


# --- Components ------------------------------------------------------------------------------


def test_new_scope_is_a_liveness_scope() -> None:
    scope = new_scope()
    assert isinstance(scope, LivenessScope)
    scope.release()


def test_make_result_manages_the_result(dh_exec_ctx: object) -> None:
    result, scope = make_result(lambda: empty_table(1).update("X = i"))
    assert result.size == 1
    scope.release()


def test_make_result_wraps_and_manages(dh_exec_ctx: object) -> None:
    source = empty_table(1)
    result, scope = make_result(lambda: Table(j_table=source.j_table), wrap=True)
    assert result is not source
    assert result == source
    scope.release()


def test_make_result_releases_the_scope_on_failure(
    dh_exec_ctx: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    scopes: list[LivenessScope] = []
    real = new_scope

    def tracking() -> LivenessScope:
        scope = real()
        scopes.append(scope)
        return scope

    monkeypatch.setattr("deephaven_massive._liveness.new_scope", tracking)

    def fail() -> Table:
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError, match="boom"):
        make_result(fail)
    assert scopes[0].j_scope is None  # released

    def interrupted() -> Table:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        make_result(interrupted)
    assert scopes[1].j_scope is None  # released on an interrupt too


def test_reaper_runs_jobs_in_order_and_survives_failures(
    dh_exec_ctx: ExecutionContext, caplog: pytest.LogCaptureFixture
) -> None:
    reaper = Reaper(dh_exec_ctx, "test-reaper")
    seen: list[int] = []
    done = threading.Event()

    def fail() -> None:
        raise RuntimeError("job failed")

    with caplog.at_level(logging.ERROR, logger="deephaven_massive._liveness"):
        reaper.submit(lambda: seen.append(1))
        reaper.submit(fail)
        reaper.submit(lambda: seen.append(2))
        reaper.submit(done.set)
        assert done.wait(TIMEOUT)
    assert seen == [1, 2]
    assert "massive reaper job failed" in caplog.text
    reaper.stop()
    reaper.thread.join(TIMEOUT)
    assert not reaper.thread.is_alive()


def test_registry_release_checks_identity(dh_exec_ctx: ExecutionContext) -> None:
    reaper = Reaper(dh_exec_ctx, "test-reaper")
    registry = ResultRegistry(threading.RLock(), reaper)
    released: list[str] = []
    table, scope = make_result(lambda: empty_table(1))
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
    collected, collected_scope = make_result(lambda: empty_table(1))
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
    registry = ResultRegistry(threading.RLock(), reaper)
    registry._results = _Interrupting()
    released: list[str] = []
    table, scope = make_result(lambda: empty_table(1))
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
        derived = result.where("Price > 0")
    massive_session.release(result)
    drain()
    assert not fake_streams.shutdown[1].is_set()
    assert derived.is_refreshing
    user_scope.release()
    assert fake_streams.shutdown[1].wait(TIMEOUT)


def test_blink_returns_distinct_wrappers_and_records(
    massive_session: dm.MassiveSession,
    fake_streams: FakeStreams,
    drain: Callable[[], None],
) -> None:
    q = query(massive_session)
    a = q.blink()
    b = q.blink()
    assert a is not b
    assert a == b  # one Java table
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


def test_close_releases_the_diagnostics_and_keeps_results_usable(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    result = query(massive_session).append_only()
    streams = massive_session.streams()
    massive_session.close()
    with pytest.raises(dm.MassiveError, match="is closed"):
        massive_session.streams()
    with pytest.raises(DHError, match="LivenessStateException"):
        streams.tail(1)
    # Live tables went static but remain usable. (A formula operation such as where() would
    # capture this frame's locals in the query scope and keep `result` alive, so use tail().)
    user_scope = new_scope()
    with user_scope.open():
        tail = result.tail(1)
    rows = tail.size
    assert rows == 0
    del tail
    user_scope.release()
    # Garbage collection still releases results after close().
    del result
    gc.collect()
    assert fake_streams.shutdown[1].wait(TIMEOUT)


def test_a_failed_build_drops_its_hold(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    core = massive_session._core
    q = query(massive_session)

    def broken(_blink: Table) -> Table:
        raise RuntimeError("cannot shape")

    with pytest.raises(RuntimeError, match="cannot shape"):
        core.stream_result(SPEC, q._request, channel="T", build=broken)
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    assert core._stream_ids == {}


def test_an_interrupted_build_drops_its_hold(
    massive_session: dm.MassiveSession, fake_streams: FakeStreams
) -> None:
    core = massive_session._core
    q = query(massive_session)

    def interrupted(_blink: Table) -> Table:
        raise KeyboardInterrupt

    with pytest.raises(KeyboardInterrupt):
        core.stream_result(SPEC, q._request, channel="T", build=interrupted)
    assert fake_streams.shutdown[1].wait(TIMEOUT)
    assert core._stream_ids == {}


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
    # Opens are optimistic: racing verbs may each open a stream, but exactly one is registered
    # and holds all sixteen results; every other one is released at once.
    ((winner, held),) = [(r["StreamId"], r["HeldResults"]) for r in stream_rows()]
    assert held == 16
    losers = set(fake_streams.shutdown) - {winner}
    for loser in losers:
        assert fake_streams.shutdown[loser].wait(TIMEOUT)
    workers = [threading.Thread(target=massive_session.release, args=(t,)) for t in results]
    for thread in workers:
        thread.start()
    for thread in workers:
        thread.join(TIMEOUT)
    assert fake_streams.shutdown[winner].wait(TIMEOUT)
    drain()
    assert fake_streams.shutdown_calls == dict.fromkeys(fake_streams.shutdown, 1)


def test_entered_contexts_survive_overlapping_threads(dh_exec_ctx: ExecutionContext) -> None:
    # Regression: one shared ExecutionContext wrapper entered on two threads at once made the
    # first thread to exit install the other thread's previous (empty) context on itself.
    poisoned = "PoisonedUpdateGraph"
    worker_in = threading.Event()
    main_out = threading.Event()
    seen: list[str] = []

    def worker() -> None:
        with entered(dh_exec_ctx):
            worker_in.set()
            main_out.wait(TIMEOUT)
            seen.append(_update_graph_class())

    thread = threading.Thread(target=worker)
    with entered(dh_exec_ctx):
        thread.start()
        assert worker_in.wait(TIMEOUT)
    main_out.set()
    thread.join(TIMEOUT)
    assert _update_graph_class() != poisoned
    assert seen != [poisoned]
    assert len(seen) == 1


def _update_graph_class() -> str:
    j_update_graph = get_exec_ctx().update_graph.j_update_graph
    return str(j_update_graph.getClass().getSimpleName())


def test_collection_takes_no_lock(dh_exec_ctx: ExecutionContext) -> None:
    # Garbage collection can run the finalizer on a thread that holds another library lock, so
    # it must not take registry_lock; it only queues the release on the reaper.
    reaper = Reaper(dh_exec_ctx, "test-reaper")
    lock = threading.RLock()
    registry = ResultRegistry(lock, reaper)
    released = threading.Event()
    table, scope = make_result(lambda: empty_table(1))
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


def test_a_failing_scope_release_still_notifies_the_owner() -> None:
    class BrokenScope:
        def release(self) -> None:
            raise RuntimeError("cannot release")

    notified: list[str] = []
    record = ResultRecord(
        1, cast("LivenessScope", BrokenScope()), lambda: notified.append("released")
    )
    with pytest.raises(RuntimeError, match="cannot release"):
        ResultRegistry._reap(record)
    assert notified == ["released"]


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

    with caplog.at_level(logging.CRITICAL, logger="deephaven_massive._liveness"):
        worker: QueueWorker[int] = QueueWorker(dh_exec_ctx, "fatal-worker", handle=fatal)
        worker.put(1)
        worker.thread.join(TIMEOUT)
    assert not worker.thread.is_alive()
    assert [(r.levelno, r.getMessage()) for r in caplog.records] == [
        (logging.CRITICAL, "fatal-worker died; its queue is no longer handled")
    ]
    assert [args.exc_type for args in escaped] == [_Fatal]


def test_queue_worker_runs_on_exit_and_logs_batch_failures(
    dh_exec_ctx: ExecutionContext, caplog: pytest.LogCaptureFixture
) -> None:
    exited = threading.Event()

    def fail(batch: list[int]) -> None:
        raise RuntimeError(f"bad batch {batch}")

    with caplog.at_level(logging.ERROR, logger="deephaven_massive._liveness"):
        worker: QueueWorker[int] = QueueWorker(
            dh_exec_ctx, "test-worker", handle=fail, on_exit=exited.set
        )
        worker.put(1)
        worker.stop()
        worker.thread.join(TIMEOUT)
    assert exited.is_set()
    assert "test-worker failed on a batch" in caplog.text
