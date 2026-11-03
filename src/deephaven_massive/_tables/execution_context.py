"""Where a library call runs: entering the session's execution context, and checking it.

Deephaven creates a live table only on a thread that has entered an `ExecutionContext`, and a
library call that creates tables and waits on library threads would stall the update cycle if
it ran on one of the update graph's own threads. This module holds the one way the library
enters a context, `entered`, and the probes that tell whether a call can work where it runs:

- `has_execution_context`: the context is a real one, not the thread's empty default;
- `has_script_query_scope`: formulas under the context can call the library's Python functions;
- `refuse_update_graph_thread`: refuses a call on one of the update graph's own threads, such
  as inside a table listener;
- `holds_update_graph_lock`: the calling thread holds the update graph's lock, as every
  Deephaven console command does, so it must wait for nothing that needs the lock;
- `update_cycle`: the number of the update graph's current cycle, which stays the same for as
  long as a thread holds the lock, so it tells one pause of the live tables from the next;
- `wait_for_update_cycle`: waits until an update cycle that started after the call has ended,
  so rows handed to a publisher before it are in the publisher's table and every table
  derived from it.

`MassiveSession()` runs the first two when it captures the caller's context, the one place a
context is captured. `refuse_update_graph_thread` runs at every entry point (`MassiveSession()`,
every verb through `SessionCore.run_verb`, and `close()`). Every library thread, and the caller
thread whenever the library creates a table, enters the session's context through `entered`.
Nothing here keeps state, so every function is safe on any thread. Only
`wait_for_update_cycle` takes a lock, the update graph's own, and it refuses to run where no
cycle could run. Every function calls Java, so none of them runs under `registry_lock`.
"""

from __future__ import annotations

import importlib
import logging
import time
from typing import Final

from deephaven.execution_context import ExecutionContext
from deephaven.update_graph import has_exclusive_lock, has_shared_lock

__all__ = [
    "entered",
    "has_execution_context",
    "has_script_query_scope",
    "holds_update_graph_lock",
    "refuse_update_graph_thread",
    "update_cycle",
    "wait_for_update_cycle",
]

logger = logging.getLogger(__name__)

# jpy, Deephaven's bridge to Java, ships no type stubs; imported by name it is untyped, as
# every other Java object the library touches is.
_NANOSECONDS: Final = (
    importlib.import_module("jpy").get_type("java.util.concurrent.TimeUnit").NANOSECONDS
)
"""The unit of the timed lock wait in `wait_for_update_cycle`."""


_SYMBOLS_GUIDE: Final = (
    "https://github.com/chipkent/deephaven-massive/blob/main/docs/guide/watchlists.md#schedules"
)
"""The user guide's section on schedules, symbol tables that follow the trading day, which the
update-graph refusal names as the way to change a stream without a table listener."""


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


def has_execution_context(exec_ctx: ExecutionContext) -> bool:
    """Tell whether an execution context is a real one, not the thread's empty default.

    Args:
        exec_ctx: The context `get_exec_ctx()` returned.

    Returns:
        True if live tables can be created under it.
    """
    j_update_graph = exec_ctx.update_graph.j_update_graph
    return str(j_update_graph.getClass().getSimpleName()) != _POISONED_UPDATE_GRAPH


def has_script_query_scope(exec_ctx: ExecutionContext) -> bool:
    """Tell whether formulas under an execution context can call the library's Python functions.

    Args:
        exec_ctx: The context `get_exec_ctx()` returned.

    Returns:
        True when the context's query scope is the Python script session's own scope, False
        for a frozen copy (`make_user_exec_ctx(freeze_vars=[...])`).
    """
    j_query_scope = exec_ctx.j_exec_ctx.getQueryScope()
    return str(j_query_scope.getClass().getName()) == _SCRIPT_QUERY_SCOPE


def refuse_update_graph_thread(exec_ctx: ExecutionContext) -> None:
    """Refuse a call on one of the update graph's own threads, such as inside a table listener.

    An update-graph thread runs the graph's cycle: a library call there would create tables and
    wait on library threads while the cycle, which those threads may need, cannot go on. A
    thread that merely holds the update graph's lock, as the Deephaven console does for every
    command, is not refused: the library never waits there for an update cycle or on a thread
    that may need the lock.

    Args:
        exec_ctx: The session's execution context, or the one a new session captures.

    Raises:
        ValueError: On an update-graph thread.
    """
    if exec_ctx.update_graph.j_update_graph.currentThreadProcessesUpdates():
        raise ValueError(
            "deephaven_massive calls must not run on a Deephaven update-graph thread "
            "(for example inside a table listener). To change what a live table follows, pass "
            "a refreshing symbol table as symbols instead: the stream follows its rows as they "
            "change, and the user guide shows one that follows the trading day: "
            f"{_SYMBOLS_GUIDE}"
        )


def holds_update_graph_lock(exec_ctx: ExecutionContext) -> bool:
    """Tell whether the calling thread holds the update graph's exclusive or shared lock.

    The Deephaven console runs every command holding the exclusive lock. While a thread holds
    either lock no update cycle runs, so it must not wait for one, nor on a thread that may
    need the lock. A Java call: never make it under `registry_lock`.

    Args:
        exec_ctx: The session's execution context.

    Returns:
        True while the thread holds either lock.
    """
    update_graph = exec_ctx.update_graph
    return has_exclusive_lock(update_graph) or has_shared_lock(update_graph)


def update_cycle(exec_ctx: ExecutionContext) -> int:
    """Return the number of the update graph's current cycle (its logical clock's step).

    The number grows by one with each update cycle and never changes while a thread holds the
    update graph's lock, so two calls that see one number ran within one pause of the live
    tables. A Java call that never waits, so a table listener may make it; never make it under
    `registry_lock`.

    Args:
        exec_ctx: The session's execution context.

    Returns:
        The cycle's number.
    """
    return int(exec_ctx.update_graph.j_update_graph.clock().currentStep())


def wait_for_update_cycle(exec_ctx: ExecutionContext, timeout: float) -> bool:
    """Wait until an update cycle that started after this call has ended.

    For a caller that handed rows to a publisher (`add` returns before the rows are in the
    table) and must see them in the table and in every table derived from it: the next cycle
    takes the rows in, and its listeners update the derived tables, before it ends. It asks
    the update graph for a cycle at once (`requestRefresh`), rather than waiting for the next
    periodic one.

    How it waits: it reads the number of the last finished cycle under the exclusive lock
    (which no running cycle holds). Then, until the timeout, it asks for a cycle and for a
    signal at the end of one, with the lock let go, takes the lock again, and returns once the
    number has grown; otherwise it waits for the signal, which lets the lock go meanwhile.
    Under the lock no cycle is running, so a grown number means a cycle that started after
    the call has ended. Reading the first number under the lock is what makes the signal
    safe: every cycle that can deliver it started after that read, so a signal delivered
    before the wait began leaves a grown number behind instead of a wait that never ends.

    Args:
        exec_ctx: The session's execution context, whose update graph is waited on.
        timeout: The most seconds to wait.

    Returns:
        True once such a cycle has ended; False, after a WARNING, if none ended within
        `timeout` (another thread held the update graph's lock all that time, for example).

    Raises:
        ValueError: On an update-graph thread (`refuse_update_graph_thread`).
        RuntimeError: While the calling thread holds the update graph's shared or exclusive
            lock, under which no cycle can run.
    """
    refuse_update_graph_thread(exec_ctx)
    if holds_update_graph_lock(exec_ctx):
        raise RuntimeError(
            "waiting for an update cycle while holding the update graph's lock would never end"
        )
    j_update_graph = exec_ctx.update_graph.j_update_graph
    lock = j_update_graph.exclusiveLock()
    deadline = time.monotonic() + timeout
    first = None
    if lock.tryLock(_nanoseconds_until(deadline), _NANOSECONDS):
        try:
            first = update_cycle(exec_ctx)
        finally:
            lock.unlock()
    while first is not None:
        signal = lock.newCondition()
        j_update_graph.requestRefresh()
        j_update_graph.requestSignal(signal)  # not under the lock, so a cycle signals it
        if not lock.tryLock(_nanoseconds_until(deadline), _NANOSECONDS):
            break  # another thread held the lock until the deadline
        try:
            if update_cycle(exec_ctx) > first:
                return True
            if signal.awaitNanos(_nanoseconds_until(deadline)) <= 0:
                break  # no signal before the deadline
        finally:
            lock.unlock()
    logger.warning(
        "no update cycle ended within %.1f s of the wait (the last finished cycle seen: %s); "
        "another thread may hold the update graph's lock",
        timeout,
        first,
    )
    return False


def _nanoseconds_until(deadline: float) -> int:
    """Return the whole nanoseconds from now until a `time.monotonic()` deadline, at least 0.

    Args:
        deadline: The deadline.

    Returns:
        The nanoseconds left; 0 once the deadline has passed.
    """
    return max(int((deadline - time.monotonic()) * 1e9), 0)
