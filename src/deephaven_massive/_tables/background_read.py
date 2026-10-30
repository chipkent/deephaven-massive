"""Background reads: `fetch(wait=False)` and `history(..., wait=False)`, one thread each.

A static verb called with `wait=False` returns at once a refreshing table with the dataset's
columns and no rows. A library thread of its own, the *read*, runs the same transport call
`wait=True` makes, so the whole static result is built off the update graph's lock; the
complete result then appears in that table in one update cycle, and the table never changes
again. It is never half loaded.

**How the rows land without a copy.** The result is the merge of a partitioned table whose
constituents are Deephaven tables held in a one-column blink table (`CONSTITUENT`), made
append-only, all created on the caller thread inside the result's own liveness scope
(`make_result`)::

    blink, publisher = blink_publisher(name, {"Constituent": TABLE}, ...)
    parts = PartitionedTable.from_partitioned_table(
        blink_to_append_only(blink), key_cols=[], unique_keys=False,
        constituent_column="Constituent", constituent_table_columns=<the dataset's columns>,
        constituent_changes_permitted=True)
    result = describe(parts.merge(), spec, schema, note=...)   # refreshing, no rows

When the transport call returns, the read publishes one row that holds the static table. On
the next cycle the append-only table gains that row and the merge adds the static table as a
constituent, which only redirects row keys: no row is copied inside the cycle, so a result of a
billion rows lands as fast as one of ten, and the cycle that adds it freezes nothing.

**Its row in `m.streams()`.** `Kind=read`, keyed by the call's id: `LOADING` from the call,
then `DONE` with `RowsPublished` once the rows have landed (`SessionCore.finish_read`), or
`FAILED` with the error. The read is a *worker table* of the session core
(`SessionCore.register_worker`), as a `poll()` table is, so its record keeps the session alive
while the result is held or still in use, and `m.close()` fails it while it is loading.

**Lifetime.** Releasing the result (`m.release`, garbage collection) marks the row `RELEASED`
and leaves the read running, since a table derived from the result may still want the rows.
The read stops when the blink table shuts down, once nothing uses the result: the shutdown
callback sets the read's stop token (`IngestPorts.read_stop`), which every request and file
read of the transport call listens to, so a request waiting for the rate governor stops at once
and one on the wire is the last; a stopped read publishes nothing. A read released while
loading becomes `DONE` if a derived table kept it going until its rows landed.

**Failures.** A transport call that raises fails the table through `publish_failure` (the
normal failed-table state, which reaches the merged result through the append-only and
partitioned tables), after marking the row `FAILED` and logging `call 7 failed after 3.2 s:
...` at ERROR. An error that is not a `MassiveError` is logged with its traceback, and the table
fails with a `MassiveError` that names it. A close or a failure that comes after the rows have
landed changes nothing: the table has its complete result.

**Threads and locks.** `open_read` runs on the caller thread, which may hold the update graph's
lock (a console command): it creates tables, registers the row, and starts the thread, and
waits for nothing. The read's thread enters the session's execution context; it holds no lock
while the transport call runs, so its rate-limit patience is the full `READ_PATIENCE`, not the
console's. Its publish lock serializes the one `add` with a failure, so the table either gets
its rows or fails, never both; only `registry_lock` is taken below it. Every other method is
safe from any thread: `release` and `fail` only set the token, update registries, and publish a
failure.
"""

from __future__ import annotations

import functools
import logging
import threading
import time
import weakref
from typing import TYPE_CHECKING, Final, cast

from deephaven import dtypes, new_table
from deephaven.column import InputColumn
from deephaven.stream import blink_to_append_only
from deephaven.table import PartitionedTable

from deephaven_massive._errors import MassiveError
from deephaven_massive._tables.columns import col_defs, describe
from deephaven_massive._tables.diagnostics import (
    StreamKind,
    StreamRow,
    StreamState,
    describe_symbols,
    shown_query,
)
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import make_result, release_scope
from deephaven_massive._tables.publisher import BlinkPublisher, blink_publisher

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._massive import Transport
    from deephaven_massive._tables.session_core import SessionCore

__all__ = ["BACKGROUND_NOTE", "BackgroundRead", "open_read"]

logger = logging.getLogger(__name__)

CONSTITUENT: Final = "Constituent"
"""The column of the read's blink table that holds the static result, as a Deephaven table."""

TABLE: Final = dtypes.DType(j_name="io.deephaven.engine.table.Table")
"""The Deephaven type of `CONSTITUENT`: a column whose cells are tables."""

BACKGROUND_NOTE: Final = (
    "Read in the background (wait=False): no rows until the download completes, then the "
    "complete result in one update, which never changes; m.streams() shows LOADING, then DONE "
    "or FAILED."
)
"""The sentence a background read's table description ends with. Deephaven fixes a table's
description when the table is created, so it states the behaviour; `m.streams()` shows the
progress."""


class BackgroundRead:
    """One background read: its thread, the publisher of its one row, and its stop token.

    Created by `open_read` on the caller thread, which builds the table, registers the read
    with the session, and then starts its thread (`start`). The thread enters the session's
    execution context, runs the transport call, and publishes the static result as one row of
    the blink table, or fails the table. `release` and `fail` are the session's hooks and are
    safe from any thread. The publish lock makes the one `add` and a failure exclusive, and
    each happens at most once.
    """

    __slots__ = (
        "__weakref__",
        "_call",
        "_columns",
        "_core",
        "_exec_ctx",
        "_failed",
        "_landed",
        "_name",
        "_publish_lock",
        "_publisher",
        "_run",
        "_started",
        "_stop",
        "_thread",
    )

    def __init__(
        self,
        core: SessionCore,
        *,
        name: str,
        call: int,
        columns: tuple[str, ...],
        publisher: BlinkPublisher,
        stop: threading.Event,
        run: Callable[[threading.Event], Table],
    ) -> None:
        """Prepare the read; nothing is requested, and the thread is not started yet.

        Args:
            core: The session's core, held weakly.
            name: The dataset's name, such as `"stocks.trades"`, for the failure messages.
            call: The call's id: the read's `StreamId`, its thread's name, and its log lines.
            columns: The dataset's columns in schema order, the definition of the merged
                result, which the static result is viewed as before it is published.
            publisher: The blink table's publisher (`blink_publisher`).
            stop: The read's stop token; the blink table's shutdown and `fail` set it.
            run: Runs the transport call with the stop token and returns its static table in
                the dataset's schema; it raises the transport's errors.
        """
        self._core = weakref.ref(core)
        self._exec_ctx = core.exec_ctx
        self._name = name
        self._call = call
        self._columns = columns
        self._publisher = publisher
        self._stop = stop
        self._run = run
        self._publish_lock = threading.Lock()
        self._failed = False
        self._landed = False
        self._started = time.monotonic()
        self._thread = threading.Thread(
            target=self._read, name=f"{core.thread_prefix} read-{call}", daemon=True
        )

    @property
    def thread(self) -> threading.Thread:
        """The read's thread."""
        return self._thread

    @property
    def stopped(self) -> bool:
        """True once the read has been asked to stop."""
        return self._stop.is_set()

    def start(self) -> None:
        """Start the thread, which begins the transport call at once."""
        self._thread.start()

    def release(self) -> None:
        """The result was released: mark the row `RELEASED`; the read keeps running.

        Runs on the reaper as the result record's `on_released` callback. A table derived from
        the result may still want its rows; the read stops when the blink table shuts down.
        The session is alive: the read's record keeps it in the live set until this has run.
        """
        cast("SessionCore", self._core()).release_worker(self._call)

    def fail(self, error: MassiveError) -> bool:
        """Stop the read and fail its table, once: the session closed, or a library thread died.

        The session's hook for the read (`SessionCore.register_worker`), called outside every
        lock while the read is registered, so the session is alive.

        Args:
            error: The error the table fails with.

        Returns:
            False when the table had already failed or already has its rows.
        """
        self._stop.set()
        return self._fail_once(cast("SessionCore", self._core()), error)

    def _read(self) -> None:
        """The thread: run the transport call, then publish its result or fail the table.

        A read stopped meanwhile (its result released and unused, or the session closed)
        publishes nothing and logs only at DEBUG, whatever the call returned or raised. A
        `MassiveError` fails the table as it is; anything else is a defect, logged with its
        traceback, and fails the table with a `MassiveError` that names it.
        """
        core = cast("SessionCore", self._core())  # registered before the thread started
        with entered(self._exec_ctx):
            try:
                self._publish(core, self._run(self._stop))
            except MassiveError as error:
                if not self._stopped_by(f"{type(error).__name__}: {error}"):
                    self._failed_with(core, error)
            except BaseException as error:
                if not self._stopped_by(f"{type(error).__name__}: {error}"):
                    logger.exception("call %d failed after %.3f s", self._call, self._took)
                    self._fail_once(
                        core,
                        MassiveError(
                            f"{self._name}: the background read failed: "
                            f"{type(error).__name__}: {error}"
                        ),
                    )

    def _failed_with(self, core: SessionCore, error: MassiveError) -> None:
        """Log a `MassiveError` that ended the read at ERROR, by its text, and fail the table.

        Its text names the status, the path, and the `request_id`; its traceback adds nothing a
        user can act on.

        Args:
            core: The session's core.
            error: The error.
        """
        logger.error("call %d failed after %.3f s: %s", self._call, self._took, error)
        self._fail_once(core, error)

    @property
    def _took(self) -> float:
        """Seconds since the call started."""
        return time.monotonic() - self._started

    def _stopped_by(self, outcome: str) -> bool:
        """Tell whether the read was stopped, so `outcome` is only how the stop ended the call.

        A stopped read publishes and fails nothing: its table was released and is unused, or
        the session closed and failed it already. The outcome is logged at DEBUG.

        Args:
            outcome: What the transport call ended with, such as `"12 rows"` or the error.

        Returns:
            True if the read's stop token is set.
        """
        if not self._stop.is_set():
            return False
        logger.debug(
            "call %d stopped after %.3f s (its table was released or closed): %s",
            self._call,
            self._took,
            outcome,
        )
        return True

    def _publish(self, core: SessionCore, table: Table) -> None:
        """Publish the static result as the blink table's one row, and mark the row `DONE`.

        The result is viewed as the dataset's columns, in schema order (a view copies no
        data), so it matches the merged table's definition. A read stopped meanwhile
        publishes nothing.

        Args:
            core: The session's core.
            table: The transport's static table.
        """
        constituent = table.view(list(self._columns))
        cell = InputColumn(name=CONSTITUENT, data_type=TABLE, input_data=[constituent.j_table])
        row = new_table([cell])
        rows = constituent.size
        with self._publish_lock:
            # Checked under the lock: a close sets the token before it fails the table here.
            if self._stopped_by(f"{rows} rows"):
                return
            self._publisher.add(row)
            self._landed = True
        core.finish_read(self._call, rows)
        logger.debug("call %d done: %d rows in %.3f s", self._call, rows, self._took)

    def _fail_once(self, core: SessionCore, error: MassiveError) -> bool:
        """Mark the row `FAILED`, then fail the table, once; never after the rows landed.

        The failure is recorded first, as a poll's is, so the row is on its way to
        `m.streams()` before anyone can see the table failed. When the session's close came
        first, the table fails with the close's error instead, so the table and its `CLOSED`
        row name the same cause. The table counts as failed only once `publish_failure` has
        returned, so a failure that raises can be retried.

        Args:
            core: The session's core.
            error: The failure.

        Returns:
            False when the table had already failed or already has its rows.
        """
        with self._publish_lock:
            if self._failed or self._landed:
                return False
            closed = core.fail_stream(self._call, str(error))
            self._publisher.publish_failure(
                error if closed in (None, str(error)) else MassiveError(closed)
            )
            self._failed = True
            return True


def _shut_down(core_ref: weakref.ref[SessionCore], call: int, stop: threading.Event) -> None:
    """A read's blink-table shutdown callback: stop the read and tell the session.

    Runs on whichever thread dropped the table's last reference, so it only sets a token and
    updates the registry (`SessionCore.worker_shut_down`). The session is alive: the read's
    record keeps it in the live set until this has run, and before the record exists the
    opening caller holds it.

    Args:
        core_ref: The session's core, weakly.
        call: The read's `StreamId`.
        stop: The read's stop token.
    """
    stop.set()
    cast("SessionCore", core_ref()).worker_shut_down(call)


def _build(
    core: SessionCore, spec: DatasetSpec, *, call: int, stop: threading.Event, note: str | None
) -> tuple[tuple[Table, BlinkPublisher], LivenessScope]:
    """Create a read's refreshing, empty result in a new result scope (`make_result`).

    Args:
        core: The session's core, for the dataset's schema.
        spec: The dataset.
        call: The read's `StreamId`.
        stop: The read's stop token, set when the blink table shuts down.
        note: The sentence the description adds after the dataset's summary (a `history()`
            table's source), or None; `BACKGROUND_NOTE` follows it.

    Returns:
        The result and its publisher, and the result scope. The result is the merged
        partitioned table, described last (`describe`).
    """
    built: list[BlinkPublisher] = []
    schema = core.dataset_schema(spec)
    text = BACKGROUND_NOTE if note is None else f"{note} {BACKGROUND_NOTE}"

    def shape() -> Table:
        blink, publisher = blink_publisher(
            f"massive.{spec.name}#{call}",
            {CONSTITUENT: TABLE},
            on_shutdown=functools.partial(_shut_down, weakref.ref(core), call, stop),
            release_later=core.release_later,
        )
        built.append(publisher)
        parts = PartitionedTable.from_partitioned_table(
            blink_to_append_only(blink),
            key_cols=[],
            unique_keys=False,
            constituent_column=CONSTITUENT,
            constituent_table_columns=col_defs(schema),
            constituent_changes_permitted=True,
        )
        return describe(parts.merge(), spec, schema, note=text)

    result, scope = make_result(core.exec_ctx, shape)
    return (result, built[0]), scope


def open_read(
    core: SessionCore,
    *,
    spec: DatasetSpec,
    call: int,
    query: str,
    symbols: NormalizedSymbols | None,
    transport: Transport,
    note: str | None,
    run: Callable[[threading.Event], Table],
) -> Table:
    """Create a background read's table and start its thread.

    On the caller thread: the result is created inside its own liveness scope (in the
    session's execution context); the read's row is registered in `m.streams()` as `LOADING`,
    which keeps the session alive while the read is in use; and the result is tracked, so
    that releasing it marks the row released. Only then does the thread start, unless the
    session closed meanwhile: the result is then released and the closed session's
    `MassiveError` raised. If any step raises, even `KeyboardInterrupt`, every step already
    taken is undone and nothing keeps running. Nothing here waits, so it works on a thread
    that holds the update graph's lock.

    Args:
        core: The session's core.
        spec: The dataset.
        call: The call's id (`SessionCore.next_stream_id`), which becomes the row's
            `StreamId`.
        query: The `repr` of the query, for the row's `Query`.
        symbols: The call's static symbols, for the row's `SymbolCount` and `Symbols`; None
            for a dataset without symbols.
        transport: The source the read reads, `REST` or `FLAT_FILES`, for the row's
            `Transport`.
        note: The sentence the table description adds after the dataset's summary (a
            `history()` table's source), or None.
        run: Runs the transport call with the read's stop token, on the read's thread, and
            returns its static table in the dataset's schema.

    Returns:
        The refreshing result: every column of the dataset and no rows, until the read lands.

    Raises:
        MassiveError: If the session closed after the read was registered but before its
            thread started (`"MassiveSession #1 is closed"`). One closed before the read
            could be registered raises the `MassiveError` of `SessionCore.register_worker`.
    """
    stop = core.read_stop()
    with entered(core.exec_ctx):
        (result, publisher), scope = _build(core, spec, call=call, stop=stop, note=note)
    count, text = (None, "") if symbols is None else describe_symbols(symbols)
    registered = False
    try:
        read = BackgroundRead(
            core,
            name=spec.name,
            call=call,
            columns=core.dataset_schema(spec).column_names,
            publisher=publisher,
            stop=stop,
            run=run,
        )
        core.register_worker(
            StreamRow(
                stream_id=call,
                kind=StreamKind.READ,
                dataset=spec.name,
                query=shown_query(query),
                symbol_count=count,
                symbols=text,
                held_results=1,
                transport=transport,
                state=StreamState.LOADING,
                created_at=time.time_ns(),
            ),
            fail=read.fail,
        )
        registered = True
        core.results.track(result, scope, read.release)
    except BaseException:
        try:
            stop.set()
            if registered:
                core.release_worker(call)
        finally:
            release_scope(core.exec_ctx, scope)
        raise
    if core.closed:  # close() failed the table meanwhile; never start its thread
        core.results.release(result)
        raise MassiveError(f"{core.label} is closed")
    read.start()
    return result
