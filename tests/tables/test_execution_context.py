"""Tests for entering the session's execution context and checking where a call runs."""

import threading
import types
from typing import cast

import pytest
from deephaven.execution_context import ExecutionContext, get_exec_ctx
from deephaven.update_graph import exclusive_lock, shared_lock

from deephaven_massive._tables.execution_context import (
    entered,
    holds_update_graph_lock,
    refuse_update_graph_thread,
)

TIMEOUT = 30.0


def test_the_update_graph_check_refuses_only_update_graph_threads(
    dh_exec_ctx: ExecutionContext,
) -> None:
    # The check itself, run on this thread with an update graph that says this is one of its
    # threads (coverage tools on Python 3.11 do not see lines run on Deephaven's own threads).
    graph = types.SimpleNamespace(currentThreadProcessesUpdates=lambda: True)
    update_thread = types.SimpleNamespace(update_graph=types.SimpleNamespace(j_update_graph=graph))
    # The refusal names the recipe that does a listener's usual job without one.
    with pytest.raises(ValueError, match=r"update-graph thread.*docs/guides/schedules\.md$"):
        refuse_update_graph_thread(cast("ExecutionContext", update_thread))
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
