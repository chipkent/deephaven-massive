"""Followed reads: a `history()` or `fetch()` result that follows a refreshing symbol table.

When `symbols` is a refreshing table, `history()` and `fetch()` follow it, and `fetch()` follows
a refreshing table passed where a dataset takes one value (an option chain's underlying, a
futures product code), keyed by each value as by a symbol: a symbol added to the table is
downloaded (by `history()` over the call's fixed window, by `fetch()` as Massive answers at that
download) and its rows appear in the result all at once; a symbol removed from the table leaves
the result in the update cycle its row leaves the table. This module builds that result; the
session's follow coordinator (`_tables/follow_coordinator.py`) decides which symbol is
downloaded when and hands each download to `FollowedRead.publish`.

**The design: one partition per symbol.** The result is a `PartitionedTable` with one
partition per symbol, merged into one table. Each partition is a static table holding that
symbol's downloaded rows; Deephaven calls a partition's table its *constituent*.

Deephaven has no call that adds or removes a partition. A `PartitionedTable` is defined by an
ordinary table with one row per partition, a *partition row*: `Symbol`, plus a `Constituent`
column whose value is that symbol's table. Adding a partition means adding a partition row;
removing one means removing its row; changing a row's table replaces the partition. A keyed
input table would be the obvious holder of those rows, but the library never uses input
tables, so it builds them from its own parts (`_build`)::

    blink, publisher = blink_publisher(name, {"Symbol": str, "Seq": long,
                                              "Constituent": TABLE, "DecidedAtStep": long})
    latest_per_symbol = blink.last_by("Symbol").where("!isNull(Constituent)")
    in_watchlist = watched_rows(latest_per_symbol, symbol_table)
    parts = PartitionedTable.from_partitioned_table(in_watchlist, key_cols=["Symbol"],
                unique_keys=True, constituent_column="Constituent", ...)
    result = describe(parts.merge(), spec, schema, note=...)

- The blink publisher lets the coordinator send partition rows, `(Symbol, table)`.
- `last_by("Symbol")` keeps one row per symbol, so a newer partition row of a symbol downloaded
  again replaces its partition.
- `watched_rows` keeps only the symbols in the user's symbol table, as `where_in` would (it is
  a join; its docstring says why), so a symbol's partition leaves in the cycle its row leaves
  that table. A blink symbol table is followed as its append-only form, so every symbol it has
  delivered stays.

A download builds its static table off the update graph's lock, and the one partition row that
points at it lands in one cycle, so no data row is copied in the update cycle. `publisher.add`
never waits for a cycle, so the coordinator can publish while a console command holds the lock.

**The `wait=True` seed.** `history(wait=True)` and `fetch(wait=True)` must return with the
call-time symbols' rows in place, even in a console command, which holds the update graph's
lock, so no published partition row can land before the call returns. Those symbols' partition
rows therefore form a static seed table, merged with the newest published rows
(`_latest_per_symbol`). A
`natural_join` of the seed against them, filtered on `isNull`, drops a seed row once its symbol
has a published row, so no symbol has two partitions. Without the seed, a `wait=True` result
would be empty when the call returns.

**The empty partition row.** `last_by` keeps a removed symbol's newest partition row, and with
it the symbol's table, for the life of the result; the watchlist filter only hides it. So under
`Reread.ALWAYS` a removal publishes a partition row whose table is null, which replaces the old
row and frees its data rows; the `!isNull(Constituent)` filter keeps it from making a
partition. Without it, every symbol ever followed would keep its rows in memory. Under
`Reread.NEVER` the old row is kept on purpose, so a re-added symbol shows its earlier download
at once.

The seed is the exception, on purpose: it keeps every call-time symbol's first download until
the result is released, empty partition row or not, so with `wait=True` those rows stay in
memory even under `Reread.ALWAYS`. Freeing them would take a seed whose rows can be removed,
and Deephaven offers no such table that is also whole when the call returns: a static table's
rows never change, a blink publisher's rows appear only on the next update cycle, which a
console command holding the lock would never let run, and an input table is not for library
code. The cost is bounded by what the call returned in the first place.

**The `DecidedAtStep` check, under `Reread.ALWAYS`.** A re-added symbol must show nothing until
its new download lands. Without a check, its old partition row, still the newest until the
empty one lands, would reappear the moment its row returns to the symbol table, and a download
begun before the removal could land after the re-add and show stale rows. So each partition
row carries `DecidedAtStep`, the latest update cycle of the symbol table the coordinator had
seen when it decided to publish the row, and `_hide_stale_downloads` shows a row only when that
is at or after the cycle its symbol last joined the table.

**`Seq`.** Each partition row carries a number larger than every earlier one of the result; the
seed's are 0. Nothing compares the numbers: the seed's `natural_join` copies `Seq` only because
it is never null, so a non-null copy means "this symbol has a published row". An empty
partition row's `Constituent` is null, so a join that copied it would take an empty row for no
row and keep the seed row, a removed symbol's old rows, behind the watchlist filter.

**Its row in `m.streams()`.** `Kind=read`, keyed by the call's id: `LOADING` while a symbol is
queued, loading, or waiting to retry, and `RUNNING` once every symbol has landed or failed. The
coordinator keeps it current (`SessionCore.update_followed_read`); each symbol's own row is in
`m.symbol_reads()`. The result is a worker table of the session core
(`SessionCore.track_worker`), so its record keeps the session alive while the result is in
use, and `m.close()` fails it.

**Lifetime.** Releasing the result (`m.release`, garbage collection) marks its row `RELEASED`;
it keeps following while a derived table uses it. Once nothing does, the blink table shuts
down. Its callback, which makes no Deephaven call, sets the stop token that every download in
flight listens to, tells the coordinator, queues the watch's stop on the reaper, and tells the
session.

**Failures.** A failure of the whole result (`fail_table`) is recorded first
(`SessionCore.fail_stream`), then published through the blink table's `BlinkPublisher`, both
under the result's publish lock, which also guards every partition row's publish: a failed
result never gets another row. The coordinator fails a result; `m.close()` and a dying session
thread fail it through `fail`, the session's hook.

**Threads.** `open_followed_read` runs on the caller thread, which may hold the update graph's
lock: it creates the tables inside the result's own liveness scope, registers the row, starts
the watch, and waits for nothing. The watch's listener only queues events for the coordinator
(`_WatchRouter`). `publish` and `fail_table` run on the coordinator thread, `fail` on a closing
or dying thread, and the shutdown callback on whichever thread shut the blink table down.
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

from deephaven_massive._enums import Reread
from deephaven_massive._errors import MassiveError
from deephaven_massive._tables.columns import col_defs, describe
from deephaven_massive._tables.diagnostics import StreamKind, StreamRow, StreamState, shown_query
from deephaven_massive._tables.execution_context import entered, update_cycle
from deephaven_massive._tables.liveness import make_result
from deephaven_massive._tables.publisher import BlinkPublisher, blink_publisher
from deephaven_massive._tables.transports import SymbolRows
from deephaven_massive._tables.watchlist import (
    CURRENT_STEP,
    SymbolWatch,
    skipped_rows_text,
    watched_rows,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._massive import Transport
    from deephaven_massive._tables.follow_coordinator import FirstRead, FollowCoordinator
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._tables.transports import SymbolOutcome
    from deephaven_massive._tables.watchlist import SymbolRowRule

__all__ = ["CONSTITUENT", "SEQ", "SYMBOL", "FollowPlan", "FollowedRead", "open_followed_read"]

logger = logging.getLogger(__name__)

SYMBOL: Final = "Symbol"
"""The partition row column that names the symbol, the partition key."""

SEQ: Final = "Seq"
"""The partition row column that numbers a result's partition rows in publish order."""

CONSTITUENT: Final = "Constituent"
"""The partition row column that holds the symbol's static rows; null in an empty row."""

TABLE: Final = dtypes.DType(j_name="io.deephaven.engine.table.Table")
"""The Deephaven type of `CONSTITUENT`: a column whose values are tables."""

_NEWER_SEQ: Final = "__MassiveNewerSeq"
"""The hidden column of a seed row that holds the `SEQ` of its symbol's newest published row,
null while there is none; dataset columns are PascalCase wire names, so none can take it."""

DECIDED_AT_STEP: Final = "DecidedAtStep"
"""The partition row column of the latest update cycle of the symbol table that the coordinator
had applied when it decided to publish the row (the result's build cycle for a seed row)."""

_JOINED_AT_STEP: Final = "__MassiveJoinedAtStep"
"""The hidden column of the update cycle a symbol's row last joined the symbol table."""


@dataclass(frozen=True, slots=True)
class FollowPlan:
    """How a followed read reads its symbols: its source, how many at once, and the read itself.

    Made by `history()` or `fetch()` on the caller thread; immutable.

    Attributes:
        transport: The source every read uses, `Transport.REST` or `Transport.FLAT_FILES`.
        one_scan: True for flat files: the symbols waiting for a read are read together, in one
            scan, and one scan of a result runs at a time. False for REST: the waiting symbols
            are read in groups of `symbols_per_read`, in parallel.
        symbols_per_read: The most symbols one REST read takes: 1 for a path that takes one
            symbol a request, or `DatasetSpec.symbols_per_request` for a `fetch()` whose
            list parameter (`ticker.any_of`) names many symbols in one request. Ignored for
            flat files.
        read: Reads symbols and returns each one's outcome, given the symbols and the result's
            stop token: over the call's window for `history()`
            (`RestTransport.history_by_symbol`, `FlatFileTransport.history_by_symbol`), and as
            Massive answers now for `fetch()` (`RestTransport.fetch_by_symbol`). It raises
            what fails every symbol alike (a rejected key, a closed session).
    """

    transport: Transport
    one_scan: bool
    symbols_per_read: int
    read: Callable[[tuple[str, ...], threading.Event], Mapping[str, SymbolOutcome]]


class FollowedRead:
    """One followed read's result: its publisher, its publish lock, and its stop token.

    Created by `open_followed_read` on the caller thread, before its tables. The coordinator
    holds its per-symbol state elsewhere; this object holds only what other threads share, and
    every method is safe from any thread. It holds the session's core, so the core is alive
    whenever a method runs. That matters on the coordinator thread: once a session that was
    never closed has no result in use, the coordinator still handles the last result's stop
    and writes its `m.symbol_reads()` rows, and only then lets go of the result and the core.
    Nothing in Java holds this object strongly (the watch's router and the shutdown callback
    hold it weakly), so it keeps the session alive no longer than the library's own threads
    need it.
    """

    __slots__ = (
        "__weakref__",
        "_columns",
        "_failed",
        "_publish_lock",
        "_publisher",
        "_watch",
        "built_step",
        "call",
        "core",
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
            core: The session's core.
            call: The call's id: the row's `StreamId` and the log's `call 7`.
            dataset: The dataset's name, such as `"stocks.bars"`.
            reread: Whether a returning symbol is read again.
            plan: How the symbols are read.
            columns: The dataset's columns in schema order, the definition of the merged
                result, which each symbol's rows are viewed as before they are published.
            stop: The result's stop token; every read listens to it.
        """
        self.core: Final = core
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
        self.built_step = 0

    @property
    def shut_down(self) -> bool:
        """True once the result's blink table has shut down: nothing uses the result."""
        return self.shut_down_event.is_set()

    def attach(self, publisher: BlinkPublisher, watch: SymbolWatch, built_step: int) -> None:
        """Keep the result's publisher and watch, once both exist.

        Args:
            publisher: The blink table's publisher.
            watch: The watch of the user's symbol table.
            built_step: The update cycle the result was built at: no cycle ran while it was
                built, so every symbol in the table then joined at this cycle or before.
        """
        self._publisher = publisher
        self._watch = watch
        self.built_step = built_step

    def publish(self, symbol: str, seq: int, table: Table | None, *, decided_at_step: int) -> None:
        """Publish a symbol's partition row: its rows, or an empty row; nothing once stopped.

        Runs on the coordinator thread, in the session's execution context. The rows are viewed
        as the dataset's columns, in schema order, so they match the merged result. The row
        lands on the next update cycle; `add` waits for none.

        Args:
            symbol: The symbol.
            seq: The row's number, larger than every earlier partition row of the result.
            table: The symbol's static rows, or None for an empty row that drops its
                partition and frees its rows.
            decided_at_step: The latest update cycle of the symbol table whose change the
                coordinator had applied when it decided to publish the row; under
                `Reread.ALWAYS` the row shows only if its symbol last joined the table at that
                cycle or before.
        """
        rows = None if table is None else _viewed(table, self._columns).j_table
        partition_row = new_table(
            [
                string_col(SYMBOL, [symbol]),
                long_col(SEQ, [seq]),
                InputColumn(name=CONSTITUENT, data_type=TABLE, input_data=[rows]),
                long_col(DECIDED_AT_STEP, [decided_at_step]),
            ]
        )
        with self._publish_lock:
            # Checked under the lock: a failure sets the token before it publishes.
            if self._failed or self.stop_token.is_set():
                logger.debug(
                    "call %d %s: stopped, so its partition row is not published", self.call, symbol
                )
                return
            cast("BlinkPublisher", self._publisher).add(partition_row)

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

        The session's hook (`SessionCore.track_worker`), called outside every lock. The
        coordinator is told to stop the result's symbols.

        Args:
            error: The error the table fails with.

        Returns:
            False when the table had already failed.
        """
        self.stop_token.set()
        failed = self.fail_table(error)
        self.core.follows.stop_following(self)
        return failed

    def abandon(self) -> None:
        """Stop a result that will never be followed (`SessionCore.track_worker`)."""
        self.stop_token.set()

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
    in order. A change carries the update cycle that made it, and the set the cycle it was
    replayed at, read from the update graph's logical clock (`step`, a read of a counter that
    never waits). Its lock is a leaf held only around list changes and queue puts; the listener
    calls it holding the update graph's lock, and it never waits.
    """

    __slots__ = (
        "_coordinator",
        "_lock",
        "_read",
        "_replay",
        "_replay_step",
        "_replaying",
        "_starter",
        "_step",
        "_waiting",
    )

    def __init__(
        self, coordinator: FollowCoordinator, read: FollowedRead, step: Callable[[], int]
    ) -> None:
        """Gather reports until `finish`.

        Args:
            coordinator: The session's follow coordinator.
            read: The result, held weakly: Java holds the listener, which holds this router.
            step: Returns the update cycle being run, or the last one run between cycles.
        """
        self._coordinator = coordinator
        self._read = weakref.ref(read)
        self._step = step
        self._lock = threading.Lock()
        self._replaying = True
        self._starter = threading.get_ident()
        self._replay: set[str] = set()
        self._replay_step = 0
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
        step = self._step()
        with self._lock:
            if self._replaying and threading.get_ident() == self._starter:
                self._replay |= added
                self._replay_step = step
                return
        self._route(lambda read: self._coordinator.changed(read, added, removed, step=step))

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
            self._coordinator.baseline(read, frozenset(self._replay), self._replay_step)
            for report in self._waiting:
                report(read)
            self._waiting.clear()
            self._replaying = False


def _shut_down(
    core_ref: weakref.ref[SessionCore],
    read_ref: weakref.ref[FollowedRead],
    *,
    worker_shut_down: Callable[[], None],
) -> None:
    """The result's blink-table shutdown callback: stop following, and tell the session.

    Runs on whichever thread dropped the table's last reference, so it sets tokens, queues
    work, and updates the registry; it makes no Deephaven call. The session is alive: the
    result's record keeps it in the live set until this has run.

    Args:
        core_ref: The session's core, weakly.
        read_ref: The result, weakly.
        worker_shut_down: The session's shutdown callback of the result's worker table
            (`SessionCore.worker_shutdown_callback`), run last.
    """
    # Alive: the session's record of the result holds it until `worker_shut_down` below.
    read = cast("FollowedRead", read_ref())
    read.stop_token.set()
    core = cast("SessionCore", core_ref())
    read.shut_down_event.set()
    core.follows.stop_following(read)
    core.defer(read.stop_watch)  # a Deephaven call: never here
    worker_shut_down()


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


def _latest_per_symbol(blink: Table, seed: Table | None) -> Table:
    """Return each symbol's newest partition row that holds rows, the seed's until one lands.

    `last_by` keeps one row per symbol, so a newer published row replaces the symbol's
    partition. With `wait=True`, the seed's row of a symbol stands in until a row of that symbol
    is published: the seed's `natural_join` copies the newest published row's `SEQ`, never
    null, so a symbol never has a seed row and a published one at once, even when the published
    one is empty. An empty row is dropped last, so it drops the symbol's partition.

    Args:
        blink: The result's blink table of partition rows.
        seed: The call-time symbols' partition rows (`wait=True`), or None.

    Returns:
        At most one partition row per symbol, with every column of the blink table.
    """
    latest_per_symbol = blink.last_by(SYMBOL)
    with_seed = latest_per_symbol
    if seed is not None:
        unpublished_seed = (
            seed.natural_join(latest_per_symbol, on=SYMBOL, joins=f"{_NEWER_SEQ}={SEQ}")
            .where(f"isNull({_NEWER_SEQ})")
            .drop_columns(_NEWER_SEQ)
        )
        with_seed = merge([unpublished_seed, latest_per_symbol])
    return with_seed.where(f"!isNull({CONSTITUENT})")


def _hide_stale_downloads(rows: Table, followed: Table) -> Table:
    """Under `Reread.ALWAYS`, hide each partition row decided before its symbol last joined.

    A returning symbol must show no rows until its new download lands, though its earlier
    partition row stays the newest until the empty row that frees it lands. So each symbol is
    stamped with the update cycle its row last joined the table (`_JOINED_AT_STEP`), and a row
    shows only when its `DECIDED_AT_STEP`, the latest cycle of the table whose change the
    coordinator had applied when it decided to publish the row, is that join cycle or later.
    The coordinator hears of a join, with its cycle, before it starts the read, so the new
    download's row always shows. A read of the earlier membership was decided before the
    coordinator heard of the removal, so its row is stamped with an earlier cycle and never
    shows, even when the coordinator publishes it after the symbol has left and come back (the
    removal and re-add were queued behind its outcome). The landing cycle could not tell these
    apart: such a stale row lands after the re-add. Cycle numbers come from one logical clock,
    so, unlike readings of two wall clocks, they never run backwards against each other. A seed
    row, read at the call, is stamped with the build cycle, so it shows while its symbol has not
    left the table since the result was made. The caller holds the update graph's shared lock,
    so no cycle runs while this is built.

    Args:
        rows: The partition rows of the symbols in the symbol table.
        followed: The user's symbol table, made append-only when it is a blink table.

    Returns:
        The rows that are not stale, with the same columns.
    """
    joined = followed.select_distinct(SYMBOL).update(f"{_JOINED_AT_STEP} = {CURRENT_STEP}")
    return (
        rows.natural_join(joined, on=SYMBOL, joins=_JOINED_AT_STEP)
        .where(f"!isNull({_JOINED_AT_STEP}) && {DECIDED_AT_STEP} >= {_JOINED_AT_STEP}")
        .drop_columns(_JOINED_AT_STEP)
    )


def _partition_rows(blink: Table, seed: Table | None, followed: Table, *, reread: Reread) -> Table:
    """Return the table that defines the result's partitions: one row per symbol it shows.

    Args:
        blink: The result's blink table of partition rows.
        seed: The call-time symbols' partition rows (`wait=True`), or None.
        followed: The user's symbol table, made append-only when it is a blink table.
        reread: Whether a returning symbol is read again. Under `Reread.NEVER` a returning
            symbol shows its kept row at once, so no row is stale.

    Returns:
        The partition rows: `Symbol`, `Seq`, and `Constituent`.
    """
    # A symbol whose row leaves the symbol table loses its partition in that cycle.
    in_watchlist = watched_rows(_latest_per_symbol(blink, seed), followed)
    if reread is Reread.ALWAYS:
        in_watchlist = _hide_stale_downloads(in_watchlist, followed)
    return in_watchlist.drop_columns(DECIDED_AT_STEP)


def _seed(seeded: Mapping[str, Table], columns: tuple[str, ...], built_step: int) -> Table:
    """Return the static table of the call-time symbols' partition rows.

    Args:
        seeded: Each symbol's static rows.
        columns: The dataset's columns, in schema order.
        built_step: The update cycle the result is built at, each row's `DECIDED_AT_STEP`.

    Returns:
        One partition row per symbol, numbered 0.
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
            long_col(DECIDED_AT_STEP, [built_step] * len(names)),
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
) -> tuple[tuple[Table, BlinkPublisher, SymbolWatch, int], LivenessScope]:
    """Create a followed read's result and watch in a new result scope (`make_result`).

    The caller holds the update graph's shared lock (`open_followed_read`).

    Args:
        core: The session's core.
        spec: The dataset.
        read: The result's shared state.
        symbol_table: The user's refreshing symbol table, or table of values.
        rule: The dataset's rule for the table's rows, which names the column read.
        router: Receives the watch's reports.
        seeded: The call-time symbols' static rows (`wait=True`), or empty.
        note: The sentences the description adds after the dataset's summary.

    Returns:
        The result, its publisher, the watch (not started), and the update cycle it was built
        at; and the result scope.
    """
    built: list[tuple[BlinkPublisher, SymbolWatch, int]] = []
    schema = core.dataset_schema(spec)

    def shape() -> Table:
        # The result is a PartitionedTable with one partition per symbol, merged into one table.
        # Deephaven has no call that adds or removes a partition: the rows of the partition
        # table define them, one row per symbol holding its static table. A blink publisher
        # plus `last_by` stands in for a keyed input table, which the library never uses, and
        # `watched_rows` (a `where_in` against the user's table) removes a symbol's partition.
        blink, publisher = blink_publisher(
            f"massive.{spec.name}#{read.call}",
            {
                SYMBOL: dtypes.string,
                SEQ: dtypes.long,
                CONSTITUENT: TABLE,
                DECIDED_AT_STEP: dtypes.long,
            },
            on_shutdown=functools.partial(
                _shut_down,
                weakref.ref(core),
                weakref.ref(read),
                worker_shut_down=core.worker_shutdown_callback(read.call, read.stop_token),
            ),
            release_later=core.release_later,
        )
        # No cycle runs: every symbol in the table now joined at this cycle or before.
        built_step = update_cycle(core.exec_ctx)
        seed = _seed(seeded, schema.column_names, built_step) if seeded else None
        followed = blink_to_append_only(symbol_table) if symbol_table.is_blink else symbol_table
        # A table of product codes is matched by its `ProductCode`, as by a partition's `Symbol`.
        keyed = followed if rule.column == SYMBOL else followed.view([f"{SYMBOL} = {rule.column}"])
        partition_rows = _partition_rows(blink, seed, keyed, reread=read.reread)
        parts = PartitionedTable.from_partitioned_table(
            partition_rows,
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
        built.append((publisher, watch, built_step))
        return result

    result, scope = make_result(core.exec_ctx, shape)
    publisher, watch, built_step = built[0]
    return (result, publisher, watch, built_step), scope


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
    `KeyboardInterrupt`, every step already taken is undone and nothing keeps running. It holds
    the update graph's shared lock from the build until the watch has started, so it waits for
    an update cycle that is running; nothing else here waits, so it works on a thread that
    holds the update graph's lock.

    Args:
        core: The session's core.
        spec: The dataset.
        call: The call's id (`SessionCore.next_stream_id`), the row's `StreamId`.
        query: The `repr` of the query, for the row's `Query`.
        symbol_table: The user's refreshing table with a `Symbol` column, or, for a table
            passed where the dataset takes one value, with the rule's column.
        rule: The dataset's rule for the table's rows (`SymbolRowRule.of`, or
            `SymbolRowRule.for_values`).
        reread: Whether a returning symbol is read again.
        plan: How the symbols are read.
        note: The sentences the table description adds after the dataset's summary.
        first: What `history(wait=True)` or `fetch(wait=True)` read of the call-time symbols,
            or None.

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
    router = _WatchRouter(core.follows, read, functools.partial(update_cycle, core.exec_ctx))
    # The update graph's shared lock is held from the build until the watch has started, so no
    # update cycle runs between them. A symbol that left the table and came back in such a
    # cycle would stay hidden for good: its seed row, decided at the build's cycle, is older
    # than its return (`_hide_stale_downloads`), and the watch, started after both, would see
    # no change, so the coordinator would never read it again. The lock also keeps a cycle from
    # refreshing a new table in the step that stamped it. A thread that already holds the lock,
    # as a console command does, takes it again at once, and nothing here waits on a thread.
    lock = core.exec_ctx.update_graph.j_update_graph.sharedLock()
    lock.lock()
    try:
        with entered(core.exec_ctx):
            (result, publisher, watch, built_step), scope = _build(
                core,
                spec,
                read,
                symbol_table=symbol_table,
                rule=rule,
                router=router,
                seeded=seeded,
                note=note,
            )
        read.attach(publisher, watch, built_step)

        def make() -> tuple[FollowedRead, StreamRow]:
            row = StreamRow(
                stream_id=call,
                kind=StreamKind.READ,
                dataset=spec.name,
                query=shown_query(query),
                symbol_count=0,
                symbols="",
                held_results=1,
                transport=plan.transport,
                state=StreamState.LOADING,
                loading=0,
                created_at=time.time_ns(),
            )
            return read, row

        core.track_worker(result, scope, make)
        try:
            core.follows.follow(read, first)
            with entered(core.exec_ctx):
                watch.start()
            router.finish(read)
        except BaseException:
            core.results.release(result)
            raise
    finally:
        lock.unlock()
    logger.debug(
        "call %d follow: %s follows its symbol table (%d symbols seeded)",
        call,
        spec.name,
        len(seeded),
    )
    return result
