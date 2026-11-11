"""Background reads: `fetch(wait=False)` and `history(..., wait=False)`, one thread each.

A static verb called with `wait=False` returns at once a refreshing table with the dataset's
columns and no rows. A library thread of its own, the *read*, runs the same transport call
`wait=True` makes, so the whole static result is built off the update graph's lock; the
complete result then appears in that table in one update cycle, and the table never changes
again. It is never half loaded.

**How the rows land without a copy.** The result ungroups an append-only blink table whose
columns are the dataset's columns as vectors, the definition `group_by()` gives a table of the
dataset's columns (`_grouped_columns`), all created on the caller thread inside the result's
own liveness scope (`make_result`)::

    blink, publisher = blink_publisher(name, <each dataset column as a vector>, ...)
    rows = blink_to_append_only(blink).ungroup(<the dataset's columns>)
    result = describe(rows, spec, schema, note=...)   # refreshing, no rows

When the transport call returns, the read groups the static table into one row per
`GROUP_ROWS` rows (`group_by()` with no keys, whose vectors view the static table's columns
without copying them) and publishes them in one `add`. On the next cycle the append-only table
gains those rows and `ungroup` expands them into the result's rows, in order, which only maps
row keys onto the vectors: no row is copied,
so a result of a billion rows lands as fast as one of ten, and the cycle that adds it freezes
nothing. The static table stays in memory, viewed by the vectors, for the life of the result.

Not a merged `PartitionedTable` holding the static table as its one constituent: while a
partitioned table may gain constituents, Deephaven counts its merge's columns as stateful, and
its `view` and `update_view` refuse stateful columns, so a user could not call them on the
result. `ungroup`'s columns are stateless.

**Its row in `m.streams()`.** `Kind=read`, keyed by the call's id: `LOADING` from the call,
then `DONE` with `RowsPublished` once the rows have landed (`SessionCore.finish_read`), or
`FAILED` with the error. The rows land on the update cycle after the publish, so `DONE` is
recorded by a listener on the ungrouped table (`_RowsLanded`), in the cycle that adds the rows:
the row reaches `m.streams()` on a later cycle, so whoever sees `DONE` there sees the rows in the
result. An empty result adds no rows and has nothing to wait for; the read records it `DONE`
as soon as it is published. The read is a *worker table* of the session core
(`SessionCore.track_worker`), as a `poll()` table is, so its record keeps the session alive
while the result is held, or still in use while it loads, and `m.close()` fails it while it is
loading. Once the read is `DONE` and its result released, the record leaves the session: a
table derived from the result keeps its rows without it.

**Lifetime.** Releasing the result (`m.release`, garbage collection) marks the row `RELEASED`
and leaves the read running, since a table derived from the result may still want the rows.
The read stops when the blink table shuts down, once nothing uses the result: the shutdown
callback sets the read's stop token (`IngestPorts.read_stop`), which every request and file
read of the transport call listens to, so a request waiting for the rate governor stops at once
and one on the wire is the last; a stopped read publishes nothing. A read released while
loading becomes `DONE` if a derived table kept it going until its rows landed.

**Failures.** A transport call that raises fails the table through `publish_failure` (the
normal failed-table state, which reaches the result through the append-only and ungrouped
tables), after marking the row `FAILED` and logging `call 7 failed after 3.2 s:
...` once, at WARNING for an expected failure and at ERROR for a defect, naming whose. An error
that is not a `MassiveError` is a defect of the library, logged with its traceback and how to
report it, and the table fails with a `MassiveError` that names it. A close or a failure that
comes after the read published its rows fails nothing: the rows land on the next cycle, and
the row is marked `DONE` at once (`BackgroundRead.fail`), while the session still shows it.

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

import logging
import threading
import time
import weakref
from typing import TYPE_CHECKING, Final, cast

from deephaven import merge, new_table
from deephaven.column import InputColumn
from deephaven.stream import blink_to_append_only
from deephaven.table import TableDefinition
from deephaven.table_listener import TableListener, TableListenerHandle

from deephaven_massive._errors import LIBRARY_BUG, ErrorCause, MassiveError, library_bug
from deephaven_massive._log_limits import log_failure, mark_failure_logged
from deephaven_massive._tables.columns import col_defs, describe
from deephaven_massive._tables.diagnostics import (
    StreamKind,
    StreamRow,
    StreamState,
    describe_symbols,
    shown_query,
)
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import make_result
from deephaven_massive._tables.publisher import BlinkPublisher, blink_publisher
from deephaven_massive._types import override

if TYPE_CHECKING:
    from collections.abc import Callable

    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table
    from deephaven.table_listener import TableUpdate

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.schema import Schema
    from deephaven_massive._datasets.symbols import NormalizedSymbols
    from deephaven_massive._massive import Transport
    from deephaven_massive._tables.session_core import SessionCore

__all__ = ["BACKGROUND_NOTE", "GROUP_ROWS", "BackgroundRead", "open_read"]

logger = logging.getLogger(__name__)

GROUP_ROWS: Final = 1 << 30
"""The most rows one published row's vectors hold. Deephaven's `ungroup` reads every value of a
vector longer than 2^31 wrongly (Deephaven 42.8), so a larger result is grouped in slices of
this size, all published in the one `add`."""

BACKGROUND_NOTE: Final = (
    "Read in the background (wait=False): no rows until the download completes, then the "
    "complete result in one update, which never changes; m.streams() shows LOADING, then DONE "
    "or FAILED."
)
"""The sentence a background read's table description ends with. Deephaven fixes a table's
description when the table is created, so it states the behaviour; `m.streams()` shows the
progress."""


class BackgroundRead:
    """One background read: its thread, the publisher of its one `add`, and its stop token.

    Created by `open_read` on the caller thread, which builds the table, registers the read
    with the session, and then starts its thread (`start`). The thread enters the session's
    execution context, runs the transport call, and publishes the static result as rows of
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
        "_name",
        "_publish_lock",
        "_published",
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
            columns: The dataset's columns in schema order, the result's definition, which the
                static result is viewed as before it is grouped and published.
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
        self._published: int | None = None
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

    def abandon(self) -> None:
        """Stop a read whose thread will never start (`SessionCore.track_worker`)."""
        self._stop.set()

    def release(self) -> None:
        """The result was released: mark the row `RELEASED`; the read keeps running.

        Runs on the reaper as the result record's `on_released` callback. A table derived from
        the result may still want its rows; the read stops when the blink table shuts down.
        The session is alive: the read's record keeps it in the live set until this has run.
        """
        cast("SessionCore", self._core()).release_worker(self._call)

    def fail(self, error: MassiveError) -> bool:
        """Stop the read and fail its table, once: the session closed, or a library thread died.

        The session's hook for the read (`SessionCore.track_worker`), called outside every
        lock while the session is alive (a read that is `DONE` may have left the registry
        meanwhile; it fails nothing).

        Args:
            error: The error the table fails with.

        A read that has published its rows is not failed: they land on the next update cycle
        whatever happens now. It is marked `DONE` at once instead, while a closing session
        still shows its rows. A close inside a console command holds the update graph's lock,
        so the cycle that adds the rows, where the result's listener would mark it, runs only
        after the session's diagnostics are released, and the read would stay `CLOSED`. The
        `DONE` row reaches `m.streams()` no earlier than the rows reach the result: both were
        handed to Deephaven before that cycle, the rows first.

        Args:
            error: The error the table fails with.

        Returns:
            False when the table had already failed or already has its rows.
        """
        self._stop.set()
        core = cast("SessionCore", self._core())
        if self._fail_once(core, error):
            return True
        with self._publish_lock:
            published = self._published
        if published is not None:
            logger.debug("call %d done at the close: its %d rows land next", self._call, published)
            core.finish_read(self._call, published)
        return False

    def _read(self) -> None:
        """The thread: run the transport call, then publish its result or fail the table.

        A read stopped meanwhile (its result released and unused, or the session closed)
        publishes nothing and logs only at DEBUG, whatever the call returned or raised. A
        `MassiveError` fails the table as it is; anything else is a defect of the library, logged
        at ERROR with its traceback and how to report it, and fails the table with a
        `MassiveError` that names it (`ErrorCause.LIBRARY_DEFECT`).
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
                    failure = MassiveError(
                        library_bug(
                            f"{self._name}: the background read failed: "
                            f"{type(error).__name__}: {error}"
                        ),
                        cause=ErrorCause.LIBRARY_DEFECT,
                    )
                    mark_failure_logged(failure)  # logged here, with its trace
                    logger.exception(
                        "call %d failed after %.3f s. %s", self._call, self._took, LIBRARY_BUG
                    )
                    self._fail_once(core, failure)

    def _failed_with(self, core: SessionCore, error: MassiveError) -> None:
        """Log a `MassiveError` that ended the read, by its text, and fail the table.

        It logs once at the level its cause gives (`log_failure`): WARNING for an expected
        failure, ERROR for a defect, naming whose. Its text names the status, the path, and the
        `request_id`; its traceback adds nothing a user can act on.

        Args:
            core: The session's core.
            error: The error.
        """
        log_failure(logger, error, "call %d failed after %.3f s: %s", self._call, self._took)
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
        """Publish the static result as rows of the blink table, in one `add`.

        The result is viewed as the dataset's columns, in schema order, and grouped into one
        row of vectors per `GROUP_ROWS` rows (`group_by()` with no keys, on each slice);
        none of it copies data, and the rows match the blink table's definition. A read
        stopped meanwhile publishes nothing. The rows land on the next update cycle, where the
        result's listener (`_RowsLanded`) marks the row `DONE`; an empty result publishes no
        row and adds no rows, so its row is marked `DONE` here.

        Args:
            core: The session's core.
            table: The transport's static table.
        """
        columns = table.view(list(self._columns))
        rows = columns.size
        grouped = (
            merge(
                [
                    columns.slice(start, min(rows, start + GROUP_ROWS)).group_by()
                    for start in range(0, rows, GROUP_ROWS)
                ]
            )
            if rows
            else None
        )
        with self._publish_lock:
            # Checked under the lock: a close sets the token before it fails the table here.
            if self._stopped_by(f"{rows} rows"):
                return
            if grouped is not None:
                self._publisher.add(grouped)
            self._published = rows
        logger.debug("call %d published: %d rows in %.3f s", self._call, rows, self._took)
        if not rows:  # no rows to land: the empty result is already what the table shows
            core.finish_read(self._call, 0)

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
            if self._failed or self._published is not None:
                return False
            closed = core.fail_stream(self._call, str(error))
            self._publisher.publish_failure(
                error if closed in (None, str(error)) else MassiveError(closed)
            )
            self._failed = True
            return True


class _RowsLanded(TableListener):
    """The listener on a background read's result that marks its row `DONE` once rows land.

    Created by `_build`, listening to the ungrouped table; the result keeps it alive. That
    table changes once, in the cycle that adds the read's complete result, and never again,
    so its one update marks the row `DONE` with the number of rows it added. An empty result
    never changes, and the read marks it `DONE` itself. It runs on an update-graph thread,
    holding the update graph's lock: it only reads the update's row count and records the
    state under `registry_lock` (`SessionCore.finish_read`), which never blocks.
    """

    def __init__(self, core_ref: weakref.ref[SessionCore], call: int) -> None:
        """Wait for the read's rows.

        Args:
            core_ref: The session's core, weakly. The read's record keeps the session in the
                live set while the read loads; a close marks a read that has published `DONE`
                before its rows land, and once its result is released too the session may be
                freed before the listener runs.
            call: The read's `StreamId`.
        """
        self._core_ref = core_ref
        self._call = call

    @override
    def on_update(self, update: TableUpdate, is_replay: object) -> None:
        """Mark the row `DONE` with the number of rows the update added, if the session lives.

        A session freed meanwhile has nothing to mark: its close marked the read `DONE`.

        Args:
            update: The ungrouped table's one update; only its count of added rows is read.
            is_replay: Always False: the listener starts without a replay.
        """
        del is_replay  # never a replay
        rows = update.j_table_update.added().size()
        logger.debug("call %d done: its %d rows landed in the result", self._call, rows)
        core = self._core_ref()
        if core is not None:
            core.finish_read(self._call, rows)

    @override
    def on_error(self, e: Exception) -> None:
        """Do nothing more for a failed result: the read marked its row `FAILED` already.

        Args:
            e: The failure, logged at DEBUG.
        """
        logger.debug("call %d: its result failed: %s", self._call, e)


def _grouped_columns(schema: Schema) -> TableDefinition:
    """Return the read's blink-table columns: each dataset column as the vector `group_by()` makes.

    Taken from `group_by()` with no keys on an empty static table of the dataset's columns, so
    the definition, element types included, is exactly that of the row `_publish` groups, and
    `ungroup` turns it back into the dataset's columns. An aggregation of a static table:
    nothing is compiled or waited for, so it runs on a thread that holds the update graph's
    lock.

    Args:
        schema: The dataset's schema.

    Returns:
        The definition, column for column in schema order.
    """
    empty = new_table([InputColumn(name, kind) for name, kind in col_defs(schema).items()])
    return TableDefinition(empty.group_by().j_table.getDefinition())


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
        The result and its publisher, and the result scope. The result is the ungrouped
        append-only table, described last (`describe`).
    """
    built: list[BlinkPublisher] = []
    schema = core.dataset_schema(spec)
    text = BACKGROUND_NOTE if note is None else f"{note} {BACKGROUND_NOTE}"

    def shape() -> Table:
        blink, publisher = blink_publisher(
            f"massive.{spec.name}#{call}",
            _grouped_columns(schema),
            on_shutdown=core.worker_shutdown_callback(call, stop),
            release_later=core.release_later,
        )
        built.append(publisher)
        rows = blink_to_append_only(blink).ungroup(list(schema.column_names))
        described = describe(rows, spec, schema, note=text)
        # The listener hears the ungrouped table and the result keeps it, so it lives exactly
        # as long as the result: a table derived from a released result still sees `DONE`. It
        # cannot listen to the result itself, which would then keep itself alive.
        landed = TableListenerHandle(
            rows,
            _RowsLanded(weakref.ref(core), call),
            description=f"deephaven_massive read {call} landed",
        )
        landed.start()
        described.j_table.manage(landed.j_object)
        return described

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
            could be registered raises the `MassiveError` of `SessionCore.track_worker`.
    """
    stop = core.read_stop()
    with entered(core.exec_ctx):
        (result, publisher), scope = _build(core, spec, call=call, stop=stop, note=note)

    def make() -> tuple[BackgroundRead, StreamRow]:
        count, text = (None, "") if symbols is None else describe_symbols(symbols)
        row = StreamRow(
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
        )
        read = BackgroundRead(
            core,
            name=spec.name,
            call=call,
            columns=core.dataset_schema(spec).column_names,
            publisher=publisher,
            stop=stop,
            run=run,
        )
        return read, row

    read = core.track_worker(result, scope, make)
    read.start()
    return result
