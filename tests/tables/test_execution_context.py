"""Tests for entering the session's execution context and checking where a call runs."""

from __future__ import annotations

import logging
import re
import threading
import types
from typing import TYPE_CHECKING

import pytest
from deephaven import time_table
from deephaven.execution_context import ExecutionContext, get_exec_ctx
from deephaven.table_listener import TableUpdate, listen
from deephaven.update_graph import UpdateGraph, exclusive_lock, shared_lock

from deephaven_massive._tables import execution_context
from deephaven_massive._tables.execution_context import (
    entered,
    holds_update_graph_lock,
    refuse_update_graph_thread,
    update_cycle,
    wait_for_update_cycle,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from contextlib import AbstractContextManager

TIMEOUT = 30.0


def test_the_update_graph_check_refuses_only_update_graph_threads(
    dh_exec_ctx: ExecutionContext,
) -> None:
    # The check itself, run on this thread with an update graph that says this is one of its
    # threads (coverage tools on Python 3.11 do not see lines run on Deephaven's own threads).
    graph = types.SimpleNamespace(currentThreadProcessesUpdates=lambda: True)
    update_thread: ExecutionContext = types.SimpleNamespace(  # type: ignore[assignment]  # a fake
        update_graph=types.SimpleNamespace(j_update_graph=graph)
    )
    # The refusal names the way to do a listener's usual job without one.
    refusal = (
        "A deephaven_massive call cannot run on a Deephaven update-graph thread, such as inside "
        "a table listener. To follow a table's changes, pass the refreshing table as symbols, "
        "or call poll(), from outside the listener."
    )
    with pytest.raises(ValueError, match=f"^{re.escape(refusal)}$"):
        refuse_update_graph_thread(update_thread)
    with shared_lock(dh_exec_ctx.update_graph):  # holding the lock is not running the graph
        refuse_update_graph_thread(dh_exec_ctx)
    refuse_update_graph_thread(dh_exec_ctx)


def test_the_lock_check_sees_either_lock(dh_exec_ctx: ExecutionContext) -> None:
    update_graph = dh_exec_ctx.update_graph
    assert not holds_update_graph_lock(dh_exec_ctx)
    with shared_lock(update_graph):
        assert holds_update_graph_lock(dh_exec_ctx)
    with exclusive_lock(update_graph):
        assert holds_update_graph_lock(dh_exec_ctx)
    assert not holds_update_graph_lock(dh_exec_ctx)


def test_the_update_cycle_stands_still_while_the_lock_is_held(
    dh_exec_ctx: ExecutionContext, next_update_cycle: Callable[[], None]
) -> None:
    with exclusive_lock(dh_exec_ctx.update_graph):
        held = update_cycle(dh_exec_ctx)
        dh_exec_ctx.update_graph.j_update_graph.requestRefresh()
        assert update_cycle(dh_exec_ctx) == held  # no cycle can run under the lock
    next_update_cycle()
    assert update_cycle(dh_exec_ctx) > held


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


def test_waiting_for_an_update_cycle_returns_once_a_later_one_has_ended(
    dh_exec_ctx: ExecutionContext,
) -> None:
    first = update_cycle(dh_exec_ctx)
    assert wait_for_update_cycle(dh_exec_ctx, TIMEOUT)
    with shared_lock(dh_exec_ctx.update_graph):  # no cycle runs: the number is a finished one
        assert update_cycle(dh_exec_ctx) > first


def _scripted(
    steps: list[int], waits: list[int], refreshes: list[int], *, locks: list[bool] | None = None
) -> ExecutionContext:
    # An update graph whose cycle numbers, signal waits, and lock attempts follow a script, so
    # each turn of the wait's loop can be told apart (with no `locks`, the lock is always
    # free); each refresh request is counted in `refreshes`. Its methods carry the Java names.
    graph = types.SimpleNamespace(
        currentThreadProcessesUpdates=lambda: False,
        currentStep=lambda: steps.pop(0),
        requestRefresh=lambda: refreshes.append(1),
        requestSignal=lambda _signal: None,
        tryLock=lambda _nanoseconds, _unit: locks.pop(0) if locks is not None else True,
        unlock=lambda: None,
        awaitNanos=lambda _nanoseconds: waits.pop(0),
    )
    graph.clock = lambda: graph
    graph.exclusiveLock = lambda: graph
    graph.newCondition = lambda: graph
    scripted: ExecutionContext = types.SimpleNamespace(  # type: ignore[assignment]  # a fake
        update_graph=types.SimpleNamespace(j_update_graph=graph)
    )
    return scripted


def test_a_signal_from_a_cycle_already_running_makes_the_wait_ask_again(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The first signal came from the cycle that was running at the call (the number has not
    # grown); the wait asks for another cycle, and the next one's end is seen.
    monkeypatch.setattr(execution_context, "holds_update_graph_lock", lambda _ctx: False)
    refreshes: list[int] = []
    assert wait_for_update_cycle(_scripted([5, 5, 6], [1_000], refreshes), TIMEOUT)
    assert len(refreshes) == 2


def test_a_wait_whose_signal_never_comes_gives_up(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(execution_context, "holds_update_graph_lock", lambda _ctx: False)
    with caplog.at_level(logging.WARNING, logger=execution_context.__name__):
        assert not wait_for_update_cycle(_scripted([5, 5], [0], []), 0.5)
    assert [r.getMessage() for r in caplog.records] == [
        (
            "No update cycle ended within 0.5 s of the wait, after cycle 5. The usual cause is "
            "another thread holding the update graph's lock."
        )
    ]


def test_a_wait_gives_up_when_another_thread_takes_the_lock_before_the_cycle(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    # The first number was read; then another thread held the lock until the deadline.
    monkeypatch.setattr(execution_context, "holds_update_graph_lock", lambda _ctx: False)
    with caplog.at_level(logging.WARNING, logger=execution_context.__name__):
        assert not wait_for_update_cycle(_scripted([5], [], [], locks=[True, False]), 0.5)
    assert [r.getMessage() for r in caplog.records] == [
        (
            "No update cycle ended within 0.5 s of the wait, after cycle 5. The usual cause is "
            "another thread holding the update graph's lock."
        )
    ]


def test_a_wait_gives_up_while_another_thread_holds_the_lock(
    dh_exec_ctx: ExecutionContext, caplog: pytest.LogCaptureFixture
) -> None:
    held, done = threading.Event(), threading.Event()

    def hold() -> None:
        with exclusive_lock(dh_exec_ctx.update_graph):  # as another thread's console command
            held.set()
            done.wait(TIMEOUT)

    thread = threading.Thread(target=hold)
    thread.start()
    try:
        assert held.wait(TIMEOUT)
        with caplog.at_level(logging.WARNING, logger=execution_context.__name__):
            assert not wait_for_update_cycle(dh_exec_ctx, 0.2)
    finally:
        done.set()
        thread.join(TIMEOUT)
    assert [r.getMessage() for r in caplog.records] == [
        (
            "No update cycle ended within 0.2 s of the wait, which never got the update graph's "
            "lock. The usual cause is another thread holding the lock."
        )
    ]


@pytest.mark.parametrize("lock", [shared_lock, exclusive_lock], ids=["shared", "exclusive"])
def test_a_wait_for_an_update_cycle_is_refused_under_the_lock(
    dh_exec_ctx: ExecutionContext, lock: Callable[[UpdateGraph], AbstractContextManager[None]]
) -> None:
    refusal = (
        "No update cycle can end while this thread holds the update graph's lock, so the wait "
        "would never end. Wait without holding the lock."
    )
    with (
        lock(dh_exec_ctx.update_graph),
        pytest.raises(RuntimeError, match=f"^{re.escape(refusal)}$"),
    ):
        wait_for_update_cycle(dh_exec_ctx, TIMEOUT)


def test_a_wait_for_an_update_cycle_is_refused_on_an_update_graph_thread(
    dh_exec_ctx: ExecutionContext,
) -> None:
    refused: list[BaseException] = []
    heard = threading.Event()

    def on_update(update: TableUpdate, is_replay: bool) -> None:
        try:
            wait_for_update_cycle(dh_exec_ctx, TIMEOUT)
        except ValueError as error:
            refused.append(error)
        heard.set()

    handle = listen(time_table("PT0.01S"), on_update)
    try:
        assert heard.wait(TIMEOUT)
    finally:
        handle.stop()
    assert str(refused[0]).startswith(
        "A deephaven_massive call cannot run on a Deephaven update-graph thread"
    )
