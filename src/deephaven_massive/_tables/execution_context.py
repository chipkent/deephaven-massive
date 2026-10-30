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
  Deephaven console command does, so it must wait for nothing that needs the lock.

`MassiveSession()` runs the first two when it captures the caller's context, the one place a
context is captured. `refuse_update_graph_thread` runs at every entry point (`MassiveSession()`,
every verb through `SessionCore.run_verb`, and `close()`). Every library thread, and the caller
thread whenever the library creates a table, enters the session's context through `entered`.
Nothing here keeps state or takes a lock, so every function is safe on any thread; the probes
call Java, so none of them runs under `registry_lock`.
"""

from __future__ import annotations

from typing import Final

from deephaven.execution_context import ExecutionContext
from deephaven.update_graph import has_exclusive_lock, has_shared_lock

__all__ = [
    "entered",
    "has_execution_context",
    "has_script_query_scope",
    "holds_update_graph_lock",
    "refuse_update_graph_thread",
]


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
            "(for example inside a table listener)"
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
