"""The process's live set: the sessions their own live tables keep alive.

A session is kept alive by its user's references and by its own live tables, and by nothing else.
The live set holds every `SessionCore` whose stream registry (`SharedStreams`), worker-table
registry (`WorkerTables`), or count of held diagnostics views is non-empty; every method that
changes one of them calls `sync` in the same `registry_lock` section, so the set never lags its
registries. Everything else in the library refers to a core weakly (connections, stream sinks,
pollers, the clients, the registries themselves, and the owner callbacks of stream results and
diagnostics views), so an unused session is freed by reference counting, at once, and a session
whose stream a derived table keeps running stays alive as long as that table.

A core that leaves the set is handed to the reaper as a job argument (`_let_go`), so its last
reference is not dropped under a lock; the thread that took it out may still drop it last,
which is safe because the core's finalizer only queues work on the reaper. Every caller of
`sync` holds the core in a local variable until it returns, after it has let go of
`registry_lock`, so leaving the set never drops the core's last reference while that lock is
held. On a free-threaded build, a reference
dropped on another thread reaches the thread that created the core only when that thread next
runs Python code (or a cyclic collection runs), so the core is freed then.

Lock order: a core's `registry_lock`, then the set's own leaf lock, under which nothing else
is taken.
"""

from __future__ import annotations

import functools
import threading
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from deephaven_massive._tables.session_core import SessionCore

__all__ = ["sync"]

# The live set, by `id(core)`. A core in it is alive, so no other object can share its id.
_live_lock = threading.Lock()
_live: dict[int, SessionCore] = {}


def _let_go(core: SessionCore) -> None:
    """Reaper job that holds a core leaving the live set, so its last reference usually drops there.

    Args:
        core: The core; the job's reference to it ends when the job returns.
    """


def sync(core: SessionCore) -> None:
    """Put a core in the live set, or take it out, from what its registries hold now.

    Called with the core's `registry_lock` held, by every method that changes its stream
    registry, its worker-table registry, or its count of held diagnostics views, after the
    change. The core is live while any of the three holds anything. A core leaving the set is
    handed to the reaper (`SessionCore.defer`), so its last reference is never dropped under a
    lock. Takes only the set's leaf lock; no Deephaven call, no wait.

    Args:
        core: The core, held by the caller until it returns.
    """
    live = bool(core.streams.active or core.workers.active or core.views_held)
    with _live_lock:
        if live is (id(core) in _live):
            return
        if live:
            _live[id(core)] = core
            return
        del _live[id(core)]
    core.defer(functools.partial(_let_go, core))
