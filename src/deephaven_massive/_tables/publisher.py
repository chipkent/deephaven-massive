"""The publisher of every blink table the library feeds, with a failure delivered exactly once.

A live table is fed by Deephaven's `TablePublisher`: it adds rows to a blink table and can fail
the table, which fails every table derived from it too. The library's WebSocket streams
(`_tables/stream.py`) and `poll()` tables (`_ingest/poll.py`) create theirs with
`blink_publisher`, which returns the blink table and a `BlinkPublisher`, never with Deephaven's
`table_publisher` directly.

**Why the wrapper exists.** `TablePublisher.publish_failure` only queues the failure: on its next
update cycle, Deephaven's `StreamToBlinkTableAdapter` fails the blink table and then closes
itself, and the close takes the adapter off the update graph. The adapter never forgets a queued
failure, so it relies on that close to stop it running. A blink table's `destroy()` closes the
adapter too, and only the first closer does the work. When another thread destroys the table
while the failure is queued (a released result, a session's close, a user's derived table going
away) and is preempted before it takes the adapter off the update graph, the adapter runs on the
next two cycles and delivers the same failure twice. Deephaven asserts that a table fails only
once, and its fatal-error handler then calls `System.exit(1)`: in a user's Deephaven server, that
ends the server.

**The pin.** `BlinkPublisher.publish_failure` first makes a new liveness scope manage the table,
so nothing can destroy it while the failure is queued; then it publishes. The failure's delivery
closes the adapter on the update-graph thread, inside the cycle, so no second run can follow.
That close runs the table's shutdown callback, which hands the pin's release to `defer` (the
session's reaper) before it runs the owner's callback.

**Threads and locks.** `add` and `publish_failure` run on any thread with an execution context
except an update-graph thread; the owner serializes them. The pin's state is shared with the
shutdown callback, which may run on any thread, under one leaf lock that is never held across a
Deephaven call.
"""

from __future__ import annotations

import functools
import operator
import threading
from typing import TYPE_CHECKING, Final

from deephaven.stream.table_publisher import table_publisher

from deephaven_massive._tables.liveness import new_scope

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven import dtypes
    from deephaven.liveness_scope import LivenessScope
    from deephaven.stream.table_publisher import TablePublisher
    from deephaven.table import Table

__all__ = ["BlinkPublisher", "blink_publisher"]


class _FailurePin:
    """The liveness hold a failing blink table keeps until its failure has been delivered.

    Shared by a `BlinkPublisher` and its blink table's shutdown callback, and guarded by its own
    leaf lock: `BlinkPublisher.publish_failure` sets it, and the shutdown callback hands its
    release to a library thread and marks the table shut down. Java holds the shutdown callback
    for as long as the publisher lives, so the pin refers to no Python wrapper of the table: the
    scope it holds tracks the table the way every Deephaven scope does, without keeping it
    reachable.
    """

    __slots__ = ("_lock", "_scope", "_shut_down")

    def __init__(self) -> None:
        """Start with no hold, before the table has shut down."""
        self._lock = threading.Lock()
        self._scope: LivenessScope | None = None
        self._shut_down = False

    def hold(self, scope: LivenessScope) -> bool:
        """Keep a scope that manages the blink table until `release` hands it on.

        Args:
            scope: A new scope that manages the table.

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
        self, run: Callable[[Callable[[], None]], None], *, shut_down: bool = False
    ) -> None:
        """Hand the held scope's release to `run`, once; do nothing when no scope is held.

        Args:
            run: Runs the release, a Deephaven call: now (`operator.call`) on a thread that may
                make one, or later on a library thread (`defer`).
            shut_down: True from the shutdown callback: the table has shut down, and no later
                failure is held or published.
        """
        with self._lock:
            self._shut_down |= shut_down
            scope, self._scope = self._scope, None
        if scope is not None:
            run(scope.release)


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
            "massive.trades#7", {"Price": dtypes.double}, on_shutdown=stopped, defer=reaper.submit
        )
        publisher.add(rows)  # a static table with the blink table's columns
        publisher.publish_failure(MassiveError("the stream failed"))

    Attributes:
        table: The blink table. The publisher refers to it strongly, so the Java table stays
            reachable while the publisher's owner (a stream's sink, a poller) lives.
    """

    __slots__ = ("_pin", "_publisher", "table")

    def __init__(self, table: Table, publisher: TablePublisher, pin: _FailurePin) -> None:
        """Wrap a publisher; use `blink_publisher`, which also wires the shutdown callback.

        Args:
            table: The publisher's blink table.
            publisher: Deephaven's publisher.
            pin: The pin the blink table's shutdown callback releases.
        """
        self.table: Final = table
        self._publisher: Final = publisher
        self._pin: Final = pin

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
        nothing pinned, for a table that is already being destroyed, that has already shut down,
        or whose earlier failure is still pinned. Whatever `TablePublisher.publish_failure`
        raises (such as a `KeyboardInterrupt`) propagates after the pin is released, so the
        table can still be destroyed and the call may be repeated. An interrupt that lands after
        Java has queued the failure therefore releases the pin early, and the table is
        unprotected again until the failure is delivered.

        Args:
            error: The failure; the table fails with a `RuntimeException` carrying its text.
        """
        pin = new_scope()
        if not pin.j_scope.tryManage(self.table.j_table) or not self._pin.hold(pin):
            pin.release()  # being destroyed, shut down, or already failing: publish nothing
            return
        try:
            self._publisher.publish_failure(error)
        except BaseException:
            self._pin.release(operator.call)
            raise


def blink_publisher(
    name: str,
    columns: dict[str, dtypes.DType],
    *,
    on_shutdown: Callable[[], None],
    defer: Callable[[Callable[[], None]], None],
) -> tuple[Table, BlinkPublisher]:
    """Create a blink table and its `BlinkPublisher`, as `table_publisher` does.

    Run it on the caller thread, inside the scope that is to manage the blink table and inside
    the session's execution context. The table lives as long as a liveness scope or a derived
    table refers to it. When it shuts down (it was destroyed, or its failure was delivered), the
    shutdown callback first queues the release of a failure's pin on `defer`, then runs
    `on_shutdown`, on whichever thread shut it down.

    Args:
        name: The publisher's name, for Deephaven's logs, such as `massive.trades#7`.
        columns: The blink table's columns (`col_defs`).
        on_shutdown: The table's shutdown callback. It must follow the rules of a Deephaven
            shutdown callback: no Deephaven call, no blocking, no exception.
        defer: Runs a job later on a library thread that has entered the session's execution
            context, without blocking the caller (the session's reaper, `SessionCore.defer`).
            It receives one job per failed table: the release of its pin.

    Returns:
        The blink table and its publisher.
    """
    pin = _FailurePin()
    table, publisher = table_publisher(
        name,
        columns,
        on_shutdown_callback=functools.partial(_shut_down, pin, on_shutdown, defer),
    )
    return table, BlinkPublisher(table, publisher, pin)


def _shut_down(
    pin: _FailurePin,
    on_shutdown: Callable[[], None],
    defer: Callable[[Callable[[], None]], None],
) -> None:
    """A `blink_publisher` table's shutdown callback: release a failure's pin, then the owner's.

    The pin's release is a Deephaven call, which a shutdown callback must not make (it may run
    on an update-graph thread inside the update cycle), so it is queued on `defer`. It is queued
    before `on_shutdown` runs, while the owner's record still keeps the session, and with it
    the `defer` thread, alive.

    Args:
        pin: The table's failure pin.
        on_shutdown: The owner's shutdown callback.
        defer: Where the pin's release runs.
    """
    pin.release(defer, shut_down=True)
    on_shutdown()
