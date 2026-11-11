"""The publisher of every blink table the library feeds, with a failure delivered exactly once.

A live table is fed by Deephaven's `TablePublisher`: it adds rows to a blink table and can fail
the table, which fails every table derived from it too. Every blink table the library writes is
created with `blink_publisher`, which returns the blink table and a `BlinkPublisher`, never with
Deephaven's `table_publisher` directly: a WebSocket stream's (`_tables/stream.py`), a `poll()`
table's (`_tables/poll.py`), a background read's (`_tables/background_read.py`), and each
diagnostics table's (`_tables/diagnostics.py`).

**Created under the update graph's lock.** `blink_publisher` creates the table while it holds
the update graph's shared lock, so no update cycle runs meanwhile. Deephaven stamps a new table
with the step of the current cycle, and the creation stamp counts as the table's notification
for that step: a table registered with the update graph while a cycle runs can be refreshed in
that same cycle, and a first change delivered in the same step is fatal (Deephaven's
fatal-error handler ends the process). Keyed input tables were seen to fail this way; a
publisher's blink table is registered with the update graph the same way, so it is created
under the lock too, as a precaution. A cycle holds the exclusive lock for its whole run, so no
cycle runs while a table is created under the shared lock, and the table is never one of a
running cycle's sources.

**Why the wrapper exists.** `TablePublisher.publish_failure` only queues the failure: on its next
update cycle, Deephaven's `StreamToBlinkTableAdapter` fails the blink table and then closes
itself, and the close takes the adapter off the update graph. The adapter never forgets a queued
failure, so it relies on that close to stop it running. A blink table's `destroy()` closes the
adapter too, and only the first closer does the work. When a thread that does not hold the update
graph's lock destroys the table while the failure is queued, and is preempted before it takes the
adapter off the update graph, the adapter runs on the next two cycles and delivers the same
failure twice. Deephaven asserts that a table fails only once, and its fatal-error handler then
calls `System.exit(1)`: in a user's Deephaven server, that ends the server.

The library releases every scope under the update graph's shared lock (`release_scope`), so its
own releases never destroy a table that way. Deephaven's own releases do not take the lock: a
gRPC thread drops a released export's reference when a user closes a panel in the Deephaven IDE, and
the liveness-cleanup thread drops the references of a table the JVM collected. A user's thread
may release a derived table the same way. Any of them can drop the last reference to the blink
table while its failure is queued, so the library pins the table as well.

**The pin.** `BlinkPublisher.publish_failure` first makes a new liveness scope manage the table,
so nothing can destroy it while the failure is queued; then it publishes. The failure's delivery
closes the adapter on the update-graph thread, inside the cycle, so no second run can follow.
That close runs the table's shutdown callback, which hands the pin to `release_later` (the
session's reaper, which releases it under the update graph's lock) before it runs the owner's
callback.

**Threads and locks.** `add` and `publish_failure` run on any thread with an execution context
except an update-graph thread. The owner serializes them under a lock of its own, which a console
command may take while it holds the update graph's lock, so neither may wait for that lock: every
release goes to `release_later`. The pin's state is shared with the shutdown callback, which may
run on any thread, under one leaf lock that is never held across a Deephaven call.
"""

from __future__ import annotations

import functools
import logging
import threading
from typing import TYPE_CHECKING, Final

from deephaven.execution_context import get_exec_ctx
from deephaven.stream.table_publisher import table_publisher

from deephaven_massive._tables.liveness import new_scope

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.liveness_scope import LivenessScope
    from deephaven.stream.table_publisher import TablePublisher
    from deephaven.table import Table, TableDefinitionLike

__all__ = ["BlinkPublisher", "blink_publisher"]

logger = logging.getLogger(__name__)


class _FailurePin:
    """The liveness hold a failing blink table keeps until its failure has been delivered.

    Shared by a `BlinkPublisher` and its blink table's shutdown callback, and guarded by its own
    leaf lock: `BlinkPublisher.publish_failure` sets it, and the shutdown callback hands it on
    for release and marks the table shut down. Java holds the shutdown callback for as long as
    the publisher lives, so the pin refers to no Python wrapper of the table: the scope it holds
    tracks the table the way every Deephaven scope does, without keeping it reachable.
    """

    __slots__ = ("_lock", "_scope", "_shut_down", "name")

    def __init__(self, name: str) -> None:
        """Start with no hold, before the table has shut down.

        Args:
            name: The publisher's name, such as `massive.trades#7`, for the log.
        """
        self.name: Final = name
        self._lock = threading.Lock()
        self._scope: LivenessScope | None = None
        self._shut_down = False

    def hold(self, scope: LivenessScope) -> bool:
        """Keep a scope until `release` hands it on; the caller then makes it manage the table.

        Args:
            scope: A new scope that manages nothing yet.

        Returns:
            False, keeping nothing, when a failure already holds a scope or the table has shut
            down: either way a failure was already published or can no longer be delivered, so
            the caller publishes none.
        """
        with self._lock:
            if self._shut_down or self._scope is not None:
                return False
            self._scope = scope
            return True

    def release(
        self, release_later: Callable[[LivenessScope], None], *, shut_down: bool = False
    ) -> None:
        """Hand the held scope to `release_later`, once; do nothing when no scope is held.

        The pin is free at once, so a later `hold` succeeds unless the table has shut down. The
        hand-over is logged at DEBUG.

        Args:
            release_later: Releases the scope on a library thread, under the update graph's
                lock (`SessionCore.release_later`); never blocks.
            shut_down: True from the shutdown callback: the table has shut down, and no later
                failure is held or published.
        """
        with self._lock:
            self._shut_down |= shut_down
            scope, self._scope = self._scope, None
        if scope is not None:
            logger.debug("%s: its failure pin is handed to the reaper for release", self.name)
            release_later(scope)


class BlinkPublisher:
    """A blink table's publisher whose failure Deephaven delivers exactly once.

    It wraps Deephaven's `TablePublisher`: `add` keeps its meaning, and `publish_failure` pins
    the table until the failure has been delivered (see the module), so the process cannot end on
    a second delivery whatever else releases the table meanwhile.

    Created by `blink_publisher` on the caller thread, inside the scope that is to manage the
    blink table. Thread-safe: `add` and `publish_failure` may run on any thread that has an
    execution context except an update-graph thread, as with `TablePublisher`. The owner
    serializes them (its publish lock) and fails the table at most once.

    Example:
        blink, publisher = blink_publisher(
            "massive.trades#7",
            {"Price": dtypes.double},
            on_shutdown=stopped,
            release_later=core.release_later,
        )
        publisher.add(rows)  # a static table with the blink table's columns
        publisher.publish_failure(MassiveError("the stream failed"))

    Attributes:
        table: The blink table. The publisher refers to it strongly, so the Java table stays
            reachable while the publisher's owner (a stream's sink, a poller) lives.
    """

    __slots__ = ("_pin", "_publisher", "_release_later", "table")

    def __init__(
        self,
        table: Table,
        publisher: TablePublisher,
        *,
        pin: _FailurePin,
        release_later: Callable[[LivenessScope], None],
    ) -> None:
        """Wrap a publisher; use `blink_publisher`, which also wires the shutdown callback.

        Args:
            table: The publisher's blink table.
            publisher: Deephaven's publisher.
            pin: The pin the blink table's shutdown callback releases.
            release_later: Where this publisher's scopes are released (`blink_publisher`).
        """
        self.table: Final = table
        self._publisher: Final = publisher
        self._pin: Final = pin
        self._release_later: Final = release_later

    def add(self, table: Table) -> None:
        """Add a snapshot of a table's rows to the blink table's next update cycle.

        Args:
            table: The rows: at least the blink table's columns, in any order; other columns
                are ignored.
        """
        self._publisher.add(table)

    def publish_failure(self, error: Exception) -> None:
        """Fail the blink table, and every table derived from it, with an error.

        The table is pinned first, so the failure is delivered on the next update cycle and
        exactly once, whatever else releases the table meanwhile. Nothing is published, and
        nothing pinned, for a table that has already shut down, whose earlier failure is still
        pinned, or that is being destroyed (its shutdown callback, which the destroy runs,
        releases the pin's empty scope). Whatever `TablePublisher.publish_failure` raises (such
        as a `KeyboardInterrupt`) propagates after the pin is handed to `release_later`, so the
        table can still be destroyed and the call may be repeated at once. An interrupt that
        lands after Java has queued the failure therefore unpins the table early, and the table
        is unprotected again until the failure is delivered.

        It never releases a scope itself: the owner calls it under a lock that a holder of the
        update graph's lock may wait for, so every release goes to `release_later`. A refused
        failure, and each hand-over to `release_later`, is logged at DEBUG with the table's name.

        Args:
            error: The failure; the table fails with a `RuntimeException` carrying its text.
        """
        pin = new_scope()
        # The Java scope, kept before the pin is reserved: a destroy racing this call may run
        # the shutdown callback, and the reaper release the reserved scope (which clears the
        # wrapper's `j_scope`), before the scope manages the table. A released scope manages
        # nothing (`tryManage` returns False), so the failure is then declined.
        j_scope = pin.j_scope
        if not self._pin.hold(pin):  # shut down, or already failing: publish nothing
            logger.debug(
                "%s: failure not published, the table has shut down or an earlier failure is "
                "still pinned (%s); its unused scope is handed to the reaper for release",
                self._pin.name,
                error,
            )
            self._release_later(pin)  # it manages nothing
            return
        if not j_scope.tryManage(self.table.j_table):
            logger.debug(
                "%s: failure not published, the table is being destroyed (%s)",
                self._pin.name,
                error,
            )
            return  # its shutdown callback releases the pin, or already has
        try:
            self._publisher.publish_failure(error)
        except BaseException:
            self._pin.release(self._release_later)
            raise


def blink_publisher(
    name: str,
    columns: TableDefinitionLike,
    *,
    on_shutdown: Callable[[], None],
    release_later: Callable[[LivenessScope], None],
) -> tuple[Table, BlinkPublisher]:
    """Create a blink table and its `BlinkPublisher`, as `table_publisher` does.

    Run it on the caller thread, inside the scope that is to manage the blink table and inside
    the session's execution context, but under no library lock that a holder of the update
    graph's lock may wait for. The table is created under the update graph's shared lock (see
    the module), so the call waits for a running update cycle to end; a thread that already
    holds either lock, as a console command does, takes it again at once. The table lives as
    long as a liveness scope or a derived
    table refers to it. When it shuts down (it was destroyed, or its failure was delivered), the
    shutdown callback first hands a failure's pin to `release_later`, then runs `on_shutdown`,
    on whichever thread shut it down.

    Args:
        name: The publisher's name, for Deephaven's logs, such as `massive.trades#7`.
        columns: The blink table's columns (`col_defs`, or a `TableDefinition`).
        on_shutdown: The table's shutdown callback. It must follow the rules of a Deephaven
            shutdown callback: no Deephaven call, no blocking, no exception.
        release_later: Releases a scope later on a library thread, under the update graph's
            shared lock, without blocking the caller (`SessionCore.release_later`, which runs
            it on the session's reaper). It receives each failure's pin once, and the unused
            scope of each refused failure.

    Returns:
        The blink table and its publisher.
    """
    pin = _FailurePin(name)
    # No update cycle runs under the lock, so none can refresh the table in the step that
    # stamped it (see the module). The Java lock itself: `deephaven.update_graph.shared_lock`
    # would wrap a failed creation's own error in a second `DHError`.
    lock = get_exec_ctx().update_graph.j_update_graph.sharedLock()
    lock.lock()
    try:
        table, publisher = table_publisher(
            name,
            columns,
            on_shutdown_callback=functools.partial(_shut_down, pin, on_shutdown, release_later),
        )
    finally:
        lock.unlock()
    return table, BlinkPublisher(table, publisher, pin=pin, release_later=release_later)


def _shut_down(
    pin: _FailurePin,
    on_shutdown: Callable[[], None],
    release_later: Callable[[LivenessScope], None],
) -> None:
    """A `blink_publisher` table's shutdown callback: release a failure's pin, then the owner's.

    The pin's release is a Deephaven call, which a shutdown callback must not make (it may run
    on an update-graph thread inside the update cycle), so it goes to `release_later`. It is
    handed on before `on_shutdown` runs, while the owner's record still keeps the session, and
    with it the reaper, alive.

    Args:
        pin: The table's failure pin.
        on_shutdown: The owner's shutdown callback.
        release_later: Where the pin is released.
    """
    pin.release(release_later, shut_down=True)
    on_shutdown()
