"""Followed reads: a `history()` result that follows a refreshing symbol table.

When `symbols` is a refreshing table, `history()` follows it: each symbol in the table is read
over the call's fixed window and its rows land in the result whole, in one update cycle; each
symbol removed from the table leaves the result in the cycle its row leaves the table. This
module builds that result and holds what one result owns; the session's follow coordinator
(`_tables/follow_coordinator.py`) decides what is read when.

**How the rows land without a copy.** Everything is created on the caller thread inside the
result's own liveness scope (`open_followed_read`)::

    blink, publisher = blink_publisher(name, {"Symbol": str, "Seq": long, "Constituent": TABLE})
    latest = blink.last_by("Symbol")                  # each symbol's latest cell
    held = latest                                     # wait=False; with wait=True, the seed:
    held = merge([seed.natural_join(latest, "Symbol", "Over=Seq").where("isNull(Over)"),
                  latest])                            # the seed's cell until a newer one
    shown = watched_rows(held.where("!isNull(Constituent)"), symbol_table)
    parts = PartitionedTable.from_partitioned_table(shown, key_cols=["Symbol"],
                unique_keys=True, constituent_column="Constituent", ...)
    result = describe(parts.merge(), spec, schema, note=...)

A *cell* is one row of the blink table: a symbol, a sequence number, and a static table of
that symbol's complete rows, built off the update graph's lock by the read. Publishing a cell
(`FollowedRead.publish`) replaces the symbol's constituent, so a symbol lands, is replaced, or
leaves as one constituent, and no row is copied in the update cycle. A cell with no table (a
*tombstone*) frees a symbol's rows. With `wait=True` the call-time symbols' cells form a static
seed, so their rows are in the result when the call returns, even inside a console command,
which holds the update graph's lock; a later cell of the same symbol masks its seed cell.
`watched_rows` joins the user's table, so a symbol whose row leaves the table leaves the result
in that same cycle. A blink symbol table is followed as its append-only form, so every symbol
it has delivered stays. There is no `input_table` and no `add_async`: the publisher's `add`
never waits for an update cycle and runs on no shared Java pool.

**Its row in `m.streams()`.** `Kind=read`, keyed by the call's id, `LOADING` while a symbol is
queued, loading, or waiting to retry, `RUNNING` once every symbol has landed or failed; the
coordinator keeps it current (`SessionCore.update_followed_read`), and each symbol's own row
is in `m.symbol_reads()`. The result is a worker table of the session core
(`SessionCore.register_worker`), so its record keeps the session alive while the result is held
or still in use, and `m.close()` fails it.

**Lifetime.** Releasing the result (`m.release`, garbage collection) marks its row `RELEASED`;
it keeps following while a derived table uses it. Once nothing does, the blink table shuts
down: its callback sets the result's stop token, which every read in flight listens to, tells
the coordinator, queues the watch's stop on the reaper, and tells the session, making no
Deephaven call itself.

**Failures.** A failure of the whole result (`fail_table`) is recorded first
(`SessionCore.fail_stream`) and then published through the blink table's `BlinkPublisher`,
under the result's publish lock, which also guards every cell's publish: a failed result never
gets another cell. The coordinator fails a result; `m.close()` and a dying session thread fail
it through `fail`, the session's hook.

**Threads.** `open_followed_read` runs on the caller thread, which may hold the update graph's
lock (a console command): it creates tables, registers the row, and starts the watch, and waits
for nothing. The watch's listener only queues events for the coordinator (`_WatchRouter`).
`publish` and `fail_table` run on the coordinator thread, `fail` on a closing or dying thread,
and the shutdown callback on whichever thread shut the blink table down.
"""

from __future__ import annotations

import functools
import logging
import threading
import time
import weakref
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, cast

from deephaven import dtypes, merge, new_table
from deephaven.column import InputColumn, long_col, string_col
from deephaven.stream import blink_to_append_only
from deephaven.table import PartitionedTable

from deephaven_massive._errors import MassiveError
from deephaven_massive._tables.columns import col_defs, describe
from deephaven_massive._tables.diagnostics import StreamKind, StreamRow, StreamState, shown_query
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import make_result, release_scope
from deephaven_massive._tables.publisher import BlinkPublisher, blink_publisher
from deephaven_massive._tables.transports import SymbolRows
from deephaven_massive._tables.watchlist import SymbolWatch, skipped_rows_text, watched_rows

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._enums import Reread
    from deephaven_massive._massive import Transport
    from deephaven_massive._tables.follow_coordinator import FirstRead, FollowCoordinator
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._tables.transports import SymbolOutcome
    from deephaven_massive._tables.watchlist import SymbolRowRule

__all__ = ["CONSTITUENT", "SEQ", "SYMBOL", "FollowPlan", "FollowedRead", "open_followed_read"]

logger = logging.getLogger(__name__)

SYMBOL: Final = "Symbol"
"""The cell column that names the symbol, the partition key."""

SEQ: Final = "Seq"
"""The cell column that numbers a result's cells in the order they were published."""

CONSTITUENT: Final = "Constituent"
"""The cell column that holds the symbol's static rows, or null for a tombstone."""

TABLE: Final = dtypes.DType(j_name="io.deephaven.engine.table.Table")
"""The Deephaven type of `CONSTITUENT`: a column whose cells are tables."""

_OVER: Final = "__MassiveOver"
"""The hidden column that marks a seed cell a newer cell masks; dataset columns are PascalCase
wire names, so none can take this name."""


@dataclass(frozen=True, slots=True)
class FollowPlan:
    """How a followed read reads its symbols: its source, and the read itself.

    Made by `history()` on the caller thread from its source choice; immutable.

    Attributes:
        transport: The source every read uses, `Transport.REST` or `Transport.FLAT_FILES`.
        one_scan: True for flat files: the symbols waiting for a read are read together, in one
            scan, and one scan of a result runs at a time. False for REST: each symbol is read
            on its own, in parallel.
        read: Reads symbols over the call's window and returns each one's outcome
            (`RestTransport.history_by_symbol` or `FlatFileTransport.history_by_symbol`),
            given the symbols and the result's stop token. It raises what fails every symbol
            alike (a rejected key, a closed session).
    """

    transport: Transport
    one_scan: bool
    read: Callable[[tuple[str, ...], threading.Event], Mapping[str, SymbolOutcome]]


class FollowedRead:
    """One followed read's result: its publisher, its publish lock, and its stop token.

    Created by `open_followed_read` on the caller thread, before its tables. The coordinator
    holds its per-symbol state elsewhere; this object holds only what other threads share, and
    every method is safe from any thread. It refers to the session's core weakly; the core is
    alive whenever a method runs, since the result's record keeps it in the live set until the
    blink table has shut down.
    """

    __slots__ = (
        "__weakref__",
        "_columns",
        "_core",
        "_failed",
        "_publish_lock",
        "_publisher",
        "_watch",
        "call",
        "dataset",
        "plan",
        "reread",
        "shut_down_event",
        "stop_token",
    )

    def __init__(
        self,
        core: SessionCore,
        *,
        call: int,
        dataset: str,
        reread: Reread,
        plan: FollowPlan,
        columns: tuple[str, ...],
        stop: threading.Event,
    ) -> None:
        """Prepare the result's shared state; nothing is created yet.

        Args:
            core: The session's core, held weakly.
            call: The call's id: the row's `StreamId` and the log's `call 7`.
            dataset: The dataset's name, such as `"stocks.bars"`.
            reread: Whether a returning symbol is read again.
            plan: How the symbols are read.
            columns: The dataset's columns in schema order, the definition of the merged
                result, which each symbol's rows are viewed as before they are published.
            stop: The result's stop token; every read listens to it.
        """
        self._core = weakref.ref(core)
        self.call: Final = call
        self.dataset: Final = dataset
        self.reread: Final = reread
        self.plan: Final = plan
        self.stop_token: Final = stop
        self.shut_down_event: Final = threading.Event()
        self._columns = columns
        self._publish_lock = threading.Lock()
        self._failed = False
        self._publisher: BlinkPublisher | None = None
        self._watch: SymbolWatch | None = None

    @property
    def core(self) -> SessionCore:
        """The session's core."""
        return cast("SessionCore", self._core())

    @property
    def shut_down(self) -> bool:
        """True once the result's blink table has shut down: nothing uses the result."""
        return self.shut_down_event.is_set()

    def attach(self, publisher: BlinkPublisher, watch: SymbolWatch) -> None:
        """Keep the result's publisher and watch, once both exist.

        Args:
            publisher: The blink table's publisher.
            watch: The watch of the user's symbol table.
        """
        self._publisher = publisher
        self._watch = watch

    def publish(self, symbol: str, seq: int, table: Table | None) -> None:
        """Publish a symbol's cell: its rows, or a tombstone; nothing once the result stopped.

        Runs on the coordinator thread, in the session's execution context. The rows are viewed
        as the dataset's columns, in schema order, so they match the merged result. The cell
        lands on the next update cycle; `add` waits for none.

        Args:
            symbol: The symbol.
            seq: The cell's number, larger than every earlier cell of the result.
            table: The symbol's static rows, or None to free them.
        """
        rows = None if table is None else _viewed(table, self._columns).j_table
        cell = new_table(
            [
                string_col(SYMBOL, [symbol]),
                long_col(SEQ, [seq]),
                InputColumn(name=CONSTITUENT, data_type=TABLE, input_data=[rows]),
            ]
        )
        with self._publish_lock:
            # Checked under the lock: a failure sets the token before it publishes.
            if self._failed or self.stop_token.is_set():
                logger.debug("call %d %s: stopped, so its cell is not published", self.call, symbol)
                return
            cast("BlinkPublisher", self._publisher).add(cell)

    def fail_table(self, error: MassiveError) -> bool:
        """Mark the row `FAILED`, then fail the table, once.

        The failure is recorded first, so the row is on its way to `m.streams()` before anyone
        can see the table failed. When the session's close came first, the table fails with
        the close's error instead, so the table and its `CLOSED` row name the same cause. The
        table counts as failed only once `publish_failure` has returned, so a failure that
        raises can be retried.

        Args:
            error: The failure.

        Returns:
            False when the table had already failed.
        """
        with self._publish_lock:
            if self._failed:
                return False
            self.stop_token.set()
            closed = self.core.fail_stream(self.call, str(error))
            cast("BlinkPublisher", self._publisher).publish_failure(
                error if closed in (None, str(error)) else MassiveError(closed)
            )
            self._failed = True
            return True

    def fail(self, error: MassiveError) -> bool:
        """Stop following and fail the table, once: the session closed, or a thread died.

        The session's hook (`SessionCore.register_worker`), called outside every lock. The
        coordinator is told to stop the result's symbols.

        Args:
            error: The error the table fails with.

        Returns:
            False when the table had already failed.
        """
        self.stop_token.set()
        failed = self.fail_table(error)
        self.core.follows.stop(self)
        return failed

    def release(self) -> None:
        """The result was released: mark the row `RELEASED`; it keeps following.

        Runs on the reaper as the result record's `on_released` callback. A table derived from
        the result may still use it; it stops when the blink table shuts down.
        """
        self.core.release_worker(self.call)

    def stop_watch(self) -> None:
        """Stop the watch's listener: a Deephaven call, so the reaper runs it."""
        cast("SymbolWatch", self._watch).stop()


class _WatchRouter:
    """Hands the watch's reports to the coordinator, the start's replay first, as a whole set.

    The watch replays the symbol table's current rows on the caller thread inside
    `SymbolWatch.start`, and an update-graph thread may report a change before the caller has
    queued that replay. So, until `finish`, the caller's replay is gathered into one set, and
    every other report waits in a list; `finish` queues the set and then the waiting reports,
    in order. Its lock is a leaf held only around list changes and queue puts; the listener
    calls it holding the update graph's lock, and it never waits.
    """

    __slots__ = ("_coordinator", "_lock", "_read", "_replay", "_replaying", "_starter", "_waiting")

    def __init__(self, coordinator: FollowCoordinator, read: FollowedRead) -> None:
        """Gather reports until `finish`.

        Args:
            coordinator: The session's follow coordinator.
            read: The result, held weakly: Java holds the listener, which holds this router.
        """
        self._coordinator = coordinator
        self._read = weakref.ref(read)
        self._lock = threading.Lock()
        self._replaying = True
        self._starter = threading.get_ident()
        self._replay: set[str] = set()
        self._waiting: list[Callable[[FollowedRead], None]] = []

    def _route(self, report: Callable[[FollowedRead], None]) -> None:
        """Queue one report now, or keep it until `finish`.

        Args:
            report: Queues the report for a result.
        """
        with self._lock:
            if self._replaying:
                self._waiting.append(report)
                return
        read = self._read()
        if read is not None:
            report(read)

    def on_change(self, added: frozenset[str], removed: frozenset[str]) -> None:
        """The watch's `on_change`: gather the replay, or queue a change.

        Args:
            added: Symbols gained.
            removed: Symbols lost.
        """
        with self._lock:
            if self._replaying and threading.get_ident() == self._starter:
                self._replay |= added
                return
        self._route(lambda read: self._coordinator.changed(read, added, removed))

    def on_skipped(self, values: tuple[str, ...]) -> None:
        """The watch's `on_skipped`: queue the text of the values skipped now.

        Args:
            values: The skipped values, sorted; empty once none is.
        """
        text = skipped_rows_text(values) if values else None
        self._route(lambda read: self._coordinator.skipped(read, text))

    def on_invalid(self, message: str) -> None:
        """The watch's `on_invalid`: queue the result's failure.

        Args:
            message: Why: a bad row under `RAISE`, or the table's failure.
        """
        self._route(lambda read: self._coordinator.invalid(read, message))

    def finish(self, read: FollowedRead) -> None:
        """Queue the replay's set, then every report that waited, and route later ones at once.

        Args:
            read: The result.
        """
        with self._lock:
            self._coordinator.baseline(read, frozenset(self._replay))
            for report in self._waiting:
                report(read)
            self._waiting.clear()
            self._replaying = False


def _shut_down(
    core_ref: weakref.ref[SessionCore],
    read_ref: weakref.ref[FollowedRead],
    *,
    call: int,
    stop: threading.Event,
) -> None:
    """The result's blink-table shutdown callback: stop following, and tell the session.

    Runs on whichever thread dropped the table's last reference, so it sets tokens, queues
    work, and updates the registry; it makes no Deephaven call. The session is alive: the
    result's record keeps it in the live set until this has run.

    Args:
        core_ref: The session's core, weakly.
        read_ref: The result, weakly.
        call: The result's `StreamId`.
        stop: The result's stop token.
    """
    stop.set()
    core = cast("SessionCore", core_ref())
    # Alive: the session's record of the result holds it until `worker_shut_down` below.
    read = cast("FollowedRead", read_ref())
    read.shut_down_event.set()
    core.follows.stop(read)
    core.defer(read.stop_watch)  # a Deephaven call: never here
    core.worker_shut_down(call)


# A table operation that takes a formula or column names copies its caller's local variables
# into the query scope, through which the JVM holds them until it collects: the helpers below
# keep table operations in frames whose locals are tables and names only, so no result, core,
# or session is held that way.


def _viewed(table: Table, columns: tuple[str, ...]) -> Table:
    """Return a symbol's rows viewed as the dataset's columns, in schema order (no copy).

    Args:
        table: The rows.
        columns: The dataset's columns.

    Returns:
        The view.
    """
    return table.view(list(columns))


def _cells(blink: Table, seed: Table | None) -> Table:
    """Return each symbol's current cell that holds rows: the latest published, else the seed's.

    Args:
        blink: The result's blink table of cells.
        seed: The call-time symbols' cells (`wait=True`), or None.

    Returns:
        One row per symbol with rows: its latest cell, or its seed cell while no cell of it
        was published.
    """
    latest = blink.last_by(SYMBOL)
    held = latest
    if seed is not None:
        masked = (
            seed.natural_join(latest, on=SYMBOL, joins=f"{_OVER}={SEQ}")
            .where(f"isNull({_OVER})")
            .drop_columns(_OVER)
        )
        held = merge([masked, latest])
    return held.where(f"!isNull({CONSTITUENT})")


def _seed(seeded: Mapping[str, Table], columns: tuple[str, ...]) -> Table:
    """Return the static table of the call-time symbols' cells.

    Args:
        seeded: Each symbol's static rows.
        columns: The dataset's columns, in schema order.

    Returns:
        One cell per symbol, numbered 0.
    """
    names = sorted(seeded)
    return new_table(
        [
            string_col(SYMBOL, names),
            long_col(SEQ, [0] * len(names)),
            InputColumn(
                name=CONSTITUENT,
                data_type=TABLE,
                input_data=[_viewed(seeded[name], columns).j_table for name in names],
            ),
        ]
    )


def _build(
    core: SessionCore,
    spec: DatasetSpec,
    read: FollowedRead,
    *,
    symbol_table: Table,
    rule: SymbolRowRule,
    router: _WatchRouter,
    seeded: Mapping[str, Table],
    note: str,
) -> tuple[tuple[Table, BlinkPublisher, SymbolWatch], LivenessScope]:
    """Create a followed read's result and watch in a new result scope (`make_result`).

    Args:
        core: The session's core.
        spec: The dataset.
        read: The result's shared state.
        symbol_table: The user's refreshing symbol table.
        rule: The dataset's rule for the table's rows.
        router: Receives the watch's reports.
        seeded: The call-time symbols' static rows (`wait=True`), or empty.
        note: The sentences the description adds after the dataset's summary.

    Returns:
        The result, its publisher, and the watch (not started), and the result scope.
    """
    built: list[tuple[BlinkPublisher, SymbolWatch]] = []
    schema = core.dataset_schema(spec)

    def shape() -> Table:
        blink, publisher = blink_publisher(
            f"massive.{spec.name}#{read.call}",
            {SYMBOL: dtypes.string, SEQ: dtypes.long, CONSTITUENT: TABLE},
            on_shutdown=functools.partial(
                _shut_down,
                weakref.ref(core),
                weakref.ref(read),
                call=read.call,
                stop=read.stop_token,
            ),
            release_later=core.release_later,
        )
        seed = _seed(seeded, schema.column_names) if seeded else None
        followed = blink_to_append_only(symbol_table) if symbol_table.is_blink else symbol_table
        shown = watched_rows(_cells(blink, seed), followed)
        parts = PartitionedTable.from_partitioned_table(
            shown,
            key_cols=[SYMBOL],
            unique_keys=True,
            constituent_column=CONSTITUENT,
            constituent_table_columns=col_defs(schema),
            constituent_changes_permitted=True,
        )
        result = describe(parts.merge(), spec, schema, note=note)
        watch = SymbolWatch(
            rule,
            followed,
            router.on_change,
            stream_id=read.call,
            on_invalid=router.on_invalid,
            on_skipped=router.on_skipped,
        )
        # A table derived from the result keeps the blink table, and so the watch, alive.
        watch.keep_alive_with(blink)
        built.append((publisher, watch))
        return result

    result, scope = make_result(core.exec_ctx, shape)
    publisher, watch = built[0]
    return (result, publisher, watch), scope


def open_followed_read(
    core: SessionCore,
    *,
    spec: DatasetSpec,
    call: int,
    query: str,
    symbol_table: Table,
    rule: SymbolRowRule,
    reread: Reread,
    plan: FollowPlan,
    note: str,
    first: FirstRead | None,
) -> Table:
    """Create a followed read's result, register it, and start following its symbol table.

    On the caller thread: the result is created inside its own liveness scope (in the
    session's execution context), seeded with the call-time symbols' rows when `first` holds
    them; its row is registered in `m.streams()` as `LOADING`, which keeps the session alive
    while the result is in use; the result is tracked, so releasing it marks the row released;
    the coordinator takes it over; and only then does the watch start, so every change of the
    table reaches the coordinator after the result. If any step raises, even
    `KeyboardInterrupt`, every step already taken is undone and nothing keeps running. Nothing
    here waits, so it works on a thread that holds the update graph's lock.

    Args:
        core: The session's core.
        spec: The dataset.
        call: The call's id (`SessionCore.next_stream_id`), the row's `StreamId`.
        query: The `repr` of the query, for the row's `Query`.
        symbol_table: The user's refreshing table with a `Symbol` column.
        rule: The dataset's rule for the table's rows (`SymbolRowRule.of`).
        reread: Whether a returning symbol is read again.
        plan: How the symbols are read.
        note: The sentences the table description adds after the dataset's summary.
        first: What `history(wait=True)` read of the call-time symbols, or None.

    Returns:
        The refreshing result: every column of the dataset, with the call-time symbols'
        rows when `first` holds them.

    Raises:
        MassiveError: If the session closed before the result could be registered, or after
            (`"MassiveSession #1 is closed"`).
    """
    stop = core.read_stop()
    read = FollowedRead(
        core,
        call=call,
        dataset=spec.name,
        reread=reread,
        plan=plan,
        columns=core.dataset_schema(spec).column_names,
        stop=stop,
    )
    seeded = (
        {}
        if first is None
        else {
            name: outcome.table
            for name, outcome in first.outcomes.items()
            if isinstance(outcome, SymbolRows)
        }
    )
    router = _WatchRouter(core.follows, read)
    with entered(core.exec_ctx):
        (result, publisher, watch), scope = _build(
            core,
            spec,
            read,
            symbol_table=symbol_table,
            rule=rule,
            router=router,
            seeded=seeded,
            note=note,
        )
    read.attach(publisher, watch)
    registered = False
    try:
        core.register_worker(
            StreamRow(
                stream_id=call,
                kind=StreamKind.READ,
                dataset=spec.name,
                query=shown_query(query),
                symbol_count=None,
                symbols="reactive table",
                held_results=1,
                transport=plan.transport,
                state=StreamState.LOADING,
                loading=0,
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
    if core.closed:  # close() failed the table meanwhile; never start following
        core.results.release(result)
        raise MassiveError(f"{core.label} is closed")
    try:
        core.follows.follow(read, first)
        with entered(core.exec_ctx):
            watch.start()
        router.finish(read)
    except BaseException:
        core.results.release(result)
        raise
    logger.debug(
        "call %d follow: %s follows its symbol table (%d symbols seeded)",
        call,
        spec.name,
        len(seeded),
    )
    return result
