"""Followed reads: a `history()` or `fetch()` result that follows a refreshing symbol table.

When `symbols` is a refreshing table, `history()` and `fetch()` follow it, and `fetch()` follows
a refreshing table passed where a dataset takes one value (an option chain's underlying, a
futures product code), keyed by each value as by a symbol: a symbol added to the table is
downloaded (by `history()` over the call's fixed window, by `fetch()` as Massive answers at that
download) and its rows appear in the result all at once; a symbol removed from the table leaves
the result in the update cycle its row leaves the table. This module builds that result; the
session's follow coordinator (`_tables/follow_coordinator.py`) decides which symbol is
downloaded when and hands each download to `FollowedRead.publish`.

**The design: a table of row groups, ungrouped.** Each symbol's downloaded rows are a static
table; the result shows them through *row groups*, rows that hold a symbol's columns as the
vectors `group_by()` makes, which view the static table's columns without copying them. A
symbol's rows make one row group per `GROUP_ROWS` rows (its *parts*, numbered from 0), because
Deephaven 42.8's `ungroup` returns wrong values in every row once any vector is longer than
2^31 (`GROUP_ROWS` says more). The result is the `ungroup` of the row groups of the symbols it
shows. A keyed input table would be the obvious holder of those row groups, but the library
never uses input tables, so it builds them from its own parts (`_build`)::

    blink, publisher = blink_publisher(name, <Key, Seq, Part, DecidedAtStep, then each
                                              dataset column as a vector>)
    latest = blink.last_by([Key, Part])
    in_watchlist = watched_rows(latest, symbol_table, key=Key)
    result = describe(in_watchlist.drop_columns(<Key, Seq, Part, DecidedAtStep>)
                      .ungroup(<the dataset's columns>), spec, schema, note=...)

- The blink publisher lets the coordinator send a symbol's row groups, all its parts in one
  `add`, so they land in one cycle.
- `last_by([Key, Part])` keeps one row group per part, so a newer download of a symbol replaces
  each part. A download of fewer parts than the symbol had also sends an *empty row group*
  (null vectors, which `ungroup` expands to no rows) for each part beyond its own, so no earlier
  part outlives it. Each download sends at least part 0, so a symbol with no rows is one empty
  row group.
- `watched_rows` keeps only the symbols in the user's symbol table, as `where_in` would (it is
  a join; its docstring says why), so a symbol's rows leave in the cycle its row leaves that
  table. A blink symbol table is followed as its append-only form, so every symbol it has
  delivered stays.
- The bookkeeping columns have hidden names (`_KEY` and the others), since a dataset has its
  own `Symbol` column, here a vector.

Not a `PartitionedTable` with one partition per symbol, merged: while a partitioned table may
gain constituents, Deephaven counts its merge's columns as stateful, and its `view` and
`update_view` refuse stateful columns, so a user could not call them on the result.
`ungroup`'s columns are stateless.

A download builds its row groups off the update graph's lock, and they land in one cycle, so no
data row is copied in the update cycle. `publisher.add` never waits for a cycle, so the
coordinator can publish while a console command holds the lock.

**The cost of a new largest symbol.** `ungroup` gives each row group a block of 2^b row keys:
at least 2^10, grown to cover the longest part since the result last had no rows, and never
shrunk while it has any. A part longer than the block widens every block, which gives every
row of the result a new row key in that cycle: cheap for the result itself, but a derived table
that tracks row keys (a `sort`, for one) does work in proportion to the result's rows, several
times what an add that widens nothing costs it. It happens at most once per doubling of the
longest part, so a result meets it a few times, mostly while its first symbols land.

**The `wait=True` seed.** `history(wait=True)` and `fetch(wait=True)` must return with the
call-time symbols' rows in place, even in a console command, which holds the update graph's
lock, so no published row group can land before the call returns. Those symbols' row groups
therefore form a static seed table, merged with the newest published ones
(`_latest_row_groups`). A `natural_join` of the seed against each symbol's newest part 0,
filtered on `isNull`, drops every seed row group of a symbol once it has a published one, so no
symbol shows two downloads. Without the seed, a `wait=True` result would be empty when the call
returns.

**The empty download.** `last_by` keeps a removed symbol's newest row groups, and with them the
symbol's rows, for the life of the result; the watchlist filter only hides them. So under
`Reread.ALWAYS` a removal publishes an empty download, whose empty row groups replace every part
and free its data rows. Without it, every symbol ever followed would keep its rows in memory.
Under `Reread.NEVER` the old row groups are kept on purpose, so a re-added symbol shows its
earlier download at once.

The seed is the exception, on purpose: it keeps every call-time symbol's first download until
the result is released, empty download or not, so with `wait=True` those rows stay in memory
even under `Reread.ALWAYS`. Freeing them would take a seed whose rows can be removed, and
Deephaven offers no such table that is also whole when the call returns: a static table's rows
never change, a blink publisher's rows appear only on the next update cycle, which a console
command holding the lock would never let run, and an input table is not for library code. The
cost is bounded by what the call returned in the first place.

**The `DecidedAtStep` check, under `Reread.ALWAYS`.** A re-added symbol must show nothing until
its new download lands. Without a check, its old row groups, still the newest until the empty
download lands, would reappear the moment its row returns to the symbol table, and a download
begun before the removal could land after the re-add and show stale rows. So each row group
carries `DecidedAtStep`, the latest update cycle of the symbol table the coordinator had seen
when it decided to publish the download, and `_hide_stale_downloads` shows a row group only
when that is at or after the cycle its symbol last joined the table.

**`Seq`.** Each download's row groups carry a number larger than every earlier download's of the
result; the seed's are 0. Nothing compares the numbers: the seed's `natural_join` copies `Seq`
only because it is never null, so a non-null copy means "this symbol has a published download",
even an empty one.

**Its row in `m.streams()`.** `Kind=read`, keyed by the call's id: `LOADING` while a symbol is
queued, loading, or waiting to retry, and `RUNNING` once every symbol has landed or failed. The
coordinator keeps it current (`WorkerTables.update_followed_read`); each symbol's own row is in
`m.symbol_reads()`. The result is a worker table of the session core
(`WorkerTables.track`), so its record keeps the session alive while the result is in
use, and `m.close()` fails it.

**Lifetime.** Releasing the result (`m.release`, garbage collection) marks its row `RELEASED`;
it keeps following while a derived table uses it. Once nothing does, the blink table shuts
down. Its callback, which makes no Deephaven call, sets the stop token that every download in
flight listens to, tells the coordinator, queues the watch's stop on the reaper, and tells the
session.

**Failures.** A failure of the whole result (`fail_table`) is recorded first
(`SessionCore.fail_stream`), then published through the blink table's `BlinkPublisher`, both
under the result's publish lock, which also guards every download's publish: a failed result
never gets another row group. The coordinator fails a result; `m.close()` and a dying session
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
from typing import TYPE_CHECKING, Final, Protocol

from deephaven import dtypes, merge, new_table
from deephaven.column import InputColumn, int_col, long_col, string_col
from deephaven.stream import blink_to_append_only
from deephaven.table import TableDefinition

from deephaven_massive._enums import Reread
from deephaven_massive._errors import MassiveError, SessionClosedError, required
from deephaven_massive._tables.background_read import GROUP_ROWS
from deephaven_massive._tables.columns import col_defs, describe
from deephaven_massive._tables.diagnostics import StreamKind, StreamRow, StreamState, shown_query
from deephaven_massive._tables.execution_context import entered, update_cycle
from deephaven_massive._tables.follow_coordinator import CoordinatedRead
from deephaven_massive._tables.liveness import make_result
from deephaven_massive._tables.publisher import BlinkPublisher, blink_publisher
from deephaven_massive._tables.refused_values import refused_values_of
from deephaven_massive._tables.transports import SymbolRows
from deephaven_massive._tables.watchlist import (
    CURRENT_STEP,
    SkippedValues,
    SymbolWatch,
    watched_rows,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping, Sequence

    from deephaven.liveness_scope import LivenessScope
    from deephaven.table import Table

    from deephaven_massive._datasets.catalog import DatasetSpec
    from deephaven_massive._datasets.schema import Schema
    from deephaven_massive._massive import Transport
    from deephaven_massive._tables.follow_coordinator import FirstRead
    from deephaven_massive._tables.problem_events import Problem
    from deephaven_massive._tables.session_core import SessionCore
    from deephaven_massive._tables.transports import SymbolOutcome
    from deephaven_massive._tables.watchlist import SymbolRowRule

__all__ = ["FollowPlan", "FollowedRead", "open_followed_read"]

logger = logging.getLogger(__name__)

# Dataset columns are PascalCase wire names, so none can take a `__Massive` name; a dataset has
# its own `Symbol` column, which a row group holds as a vector.

_SYMBOL: Final = "Symbol"
"""The symbol table's column of symbols, which a row group's `_KEY` is matched against."""

_KEY: Final = "__MassiveKey"
"""The row group column that names its symbol."""

_SEQ: Final = "__MassiveSeq"
"""The row group column that numbers a result's downloads in publish order; 0 in the seed."""

_PART: Final = "__MassivePart"
"""The row group column that numbers a symbol's row groups, each of `GROUP_ROWS` rows at most."""

_DECIDED_AT_STEP: Final = "__MassiveDecidedAtStep"
"""The row group column of the latest update cycle of the symbol table that the coordinator
had applied when it decided to publish the download (the result's build cycle for the seed)."""

_BOOKKEEPING: Final = (_KEY, _SEQ, _PART, _DECIDED_AT_STEP)
"""The row group columns that are not the dataset's, in definition order."""

_NEWER_SEQ: Final = "__MassiveNewerSeq"
"""The hidden column of a seed row group that holds the `_SEQ` of its symbol's newest published
download, null while there is none."""

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
            stop token, or None for the read of a call that waits for it, whose rate-limit
            pauses are then waited out as its caller's (a stop token marks a read no caller
            waits for, which waits out `MassiveConfig.max_outage`): over the call's window for
            `history()`
            (`RestTransport.history_by_symbol`, `FlatFileTransport.history_by_symbol`), and as
            Massive answers now for `fetch()` (`RestTransport.fetch_by_symbol`). It raises
            what fails every symbol alike (a rejected key, a closed session).
    """

    transport: Transport
    one_scan: bool
    symbols_per_read: int
    read: Callable[[tuple[str, ...], threading.Event | None], Mapping[str, SymbolOutcome]]


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
        "_empty",
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
            columns: The dataset's columns in schema order, the definition of the result,
                which each symbol's rows are viewed as before they are grouped.
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
        self._empty: Table | None = None
        self.built_step = 0

    @property
    def shut_down(self) -> bool:
        """True once the result's blink table has shut down: nothing uses the result."""
        return self.shut_down_event.is_set()

    def attach(
        self, publisher: BlinkPublisher, watch: SymbolWatch, *, built_step: int, empty: Table
    ) -> None:
        """Keep the result's publisher and watch, once both exist.

        Args:
            publisher: The blink table's publisher.
            watch: The watch of the user's symbol table.
            built_step: The update cycle the result was built at: no cycle ran while it was
                built, so every symbol in the table then joined at this cycle or before.
            empty: The static template of an empty row group (`_empty_row_group`).
        """
        self._publisher = publisher
        self._watch = watch
        self.built_step = built_step
        self._empty = empty

    def publish(
        self,
        symbol: str,
        seq: int,
        table: Table | None,
        *,
        decided_at_step: int,
        parts_before: int,
    ) -> int:
        """Publish a symbol's download as row groups, or an empty download; nothing once stopped.

        Runs on the coordinator thread, in the session's execution context. The rows are viewed
        as the dataset's columns, in schema order, and grouped into one row group per
        `GROUP_ROWS` rows (`_row_groups`), with an empty row group for each part the symbol's
        previous download had beyond this one's, so no earlier part outlives it; none of it
        copies data. Every row group lands in one `add`, on the next update cycle; `add` waits
        for none.

        Args:
            symbol: The symbol.
            seq: The download's number, larger than every earlier one of the result.
            table: The symbol's static rows, or None for an empty download that frees its rows.
            decided_at_step: The latest update cycle of the symbol table whose change the
                coordinator had applied when it decided to publish the download; under
                `Reread.ALWAYS` it shows only if its symbol last joined the table at that cycle
                or before.
            parts_before: How many row groups of the symbol's previous published download
                hold rows (the coordinator's count; 0 for none).

        Returns:
            How many row groups of the symbol's newest published download hold rows:
            `parts_before` when nothing was published.
        """
        rows = None if table is None else _viewed(table, self._columns)
        groups, parts = _row_groups(
            symbol,
            seq,
            rows,
            decided_at_step=decided_at_step,
            parts_before=parts_before,
            empty=required(self._empty, f"call {self.call}'s empty row group"),
        )
        with self._publish_lock:
            # Checked under the lock: a failure sets the token before it publishes.
            if self._failed or self.stop_token.is_set():
                logger.debug(
                    "call %d %s: stopped, so its download is not published", self.call, symbol
                )
                return parts_before
            required(self._publisher, f"call {self.call}'s publisher").add(groups)
        return parts

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
            closed = self.core.fail_stream(self.call, error)
            required(self._publisher, f"call {self.call}'s publisher").publish_failure(
                error if closed in (None, str(error)) else SessionClosedError(closed)
            )
            self._failed = True
            return True

    def fail(self, error: MassiveError) -> bool:
        """Stop following and fail the table, once: the session closed, or a thread died.

        The session's hook (`WorkerTables.track`), called outside every lock. The
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
        """Stop a result that will never be followed (`WorkerTables.track`)."""
        self.stop_token.set()

    def release(self) -> None:
        """The result was released: mark the row `RELEASED`; it keeps following.

        Runs on the reaper as the result record's `on_released` callback. A table derived from
        the result may still use it; it stops when the blink table shuts down.
        """
        self.core.workers.release(self.call)

    def stop_watch(self) -> None:
        """Stop the watch's listener: a Deephaven call, so the reaper runs it.

        A result whose build was undone after its blink table was made (an exception, even a
        `KeyboardInterrupt`, in `open_followed_read` before `attach`) shuts down with no watch:
        there is nothing to stop, and that is logged at DEBUG.
        """
        if self._watch is None:
            logger.debug(
                "call %d follow: the result shut down before its watch was made; no watch to stop",
                self.call,
            )
            return
        self._watch.stop()


class _WatchReports(Protocol):
    """Where a watch router hands a result's reports: the session's `FollowCoordinator`."""

    def baseline(self, read: CoordinatedRead, symbols: frozenset[str], step: int, /) -> None:
        """Queue the symbols a result follows from the start (`FollowCoordinator.baseline`).

        Args:
            read: The result.
            symbols: The symbol table's symbols.
            step: The update cycle they were read at.
        """
        ...

    def changed(
        self,
        read: CoordinatedRead,
        added: frozenset[str],
        removed: frozenset[str],
        /,
        *,
        step: int,
    ) -> None:
        """Queue a change of the symbol table (`FollowCoordinator.changed`).

        Args:
            read: The result.
            added: Symbols gained.
            removed: Symbols lost.
            step: The update cycle that made the change.
        """
        ...

    def invalid(self, read: CoordinatedRead, message: str, /) -> None:
        """Queue the result's failure (`FollowCoordinator.invalid`).

        Args:
            read: The result.
            message: Why it fails.
        """
        ...


class _ShutDownRead(CoordinatedRead, Protocol):
    """A followed read, as its blink table's shutdown callback stops it: a `FollowedRead`."""

    @property
    def shut_down_event(self) -> threading.Event:
        """Set once the result's blink table has shut down."""
        ...

    def stop_watch(self) -> None:
        """Stop the watch's listener (`FollowedRead.stop_watch`)."""
        ...


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
        self, coordinator: _WatchReports, read: CoordinatedRead, step: Callable[[], int]
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
        self._waiting: list[Callable[[CoordinatedRead], None]] = []

    def _route(self, report: Callable[[CoordinatedRead], None]) -> None:
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

    def on_invalid(self, message: str) -> None:
        """The watch's `on_invalid`: queue the result's failure.

        Args:
            message: Why: a bad row under `RAISE`, or the table's failure.
        """
        self._route(lambda read: self._coordinator.invalid(read, message))

    def finish(self, read: CoordinatedRead) -> None:
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
    read_ref: Callable[[], _ShutDownRead | None],
    *,
    worker_shut_down: Callable[[], None],
) -> None:
    """The result's blink-table shutdown callback: stop following, and tell the session.

    Runs on whichever thread dropped the table's last reference, so it sets tokens, queues
    work, and updates the registry; it makes no Deephaven call. The session is alive: the
    result's record keeps it in the live set until this has run.

    Args:
        core_ref: The session's core, weakly.
        read_ref: Returns the result, or None once it is freed: a weak reference.
        worker_shut_down: The session's shutdown callback of the result's worker table
            (`WorkerTables.shutdown_callback`), run last.
    """
    # Alive: the session's record of the result holds it until `worker_shut_down` below.
    read = required(read_ref(), "the followed read")
    read.stop_token.set()
    core = required(core_ref(), "the session")
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


def _key_row(symbol: str, seq: int, *, part: int, decided_at_step: int) -> Table:
    """Return the static one-row table of a row group's bookkeeping columns.

    Built from column data, not a formula, so nothing is compiled for a publish and no symbol is
    written into formula text.

    Args:
        symbol: The symbol.
        seq: The download's number.
        part: The row group's part.
        decided_at_step: The download's `_DECIDED_AT_STEP`.

    Returns:
        One row of `_BOOKKEEPING`, in that order.
    """
    return new_table(
        [
            string_col(_KEY, [symbol]),
            long_col(_SEQ, [seq]),
            int_col(_PART, [part]),
            long_col(_DECIDED_AT_STEP, [decided_at_step]),
        ]
    )


def _empty_row_group(schema: Schema) -> Table:
    """Return the static template of an empty row group: `_KEY`, then null vectors, no rows.

    `_KEY` and every dataset column as the vector `group_by()` makes, element types included, so
    a key row's `natural_join` against it (`_row_groups`) makes an empty row group whose
    definition matches a full one exactly; an aggregation of an empty static table, so nothing
    is compiled or waited for.

    Args:
        schema: The dataset's schema.

    Returns:
        The template, with no rows.
    """
    columns = [InputColumn(_KEY, dtypes.string)]
    columns += [InputColumn(name, kind) for name, kind in col_defs(schema).items()]
    return new_table(columns).group_by(_KEY)


def _row_groups(
    symbol: str,
    seq: int,
    rows: Table | None,
    *,
    decided_at_step: int,
    parts_before: int,
    empty: Table,
) -> tuple[Table, int]:
    """Return a download's row groups, part 0 first, and how many of them hold rows.

    Each part holds at most `GROUP_ROWS` rows, as the vectors of `group_by()` with no keys on a
    slice, which view the static table's columns without copying them. An empty row group
    stands in for each part from the download's own count up to the symbol's previous count,
    so no earlier part outlives the download, and for part 0 when the download has no rows, so
    every download has a part 0 (the seed's mask and `published` read it).

    Args:
        symbol: The symbol.
        seq: The download's number.
        rows: The symbol's static rows, viewed as the dataset's columns, or None for an empty
            download.
        decided_at_step: The download's `_DECIDED_AT_STEP`.
        parts_before: How many parts held rows in the symbol's previous published download, 0
            for none: every part from that one up is empty or was never sent, because each
            download sends an empty row group for each part it lacks below this count.
        empty: The template of an empty row group (`_empty_row_group`).

    Returns:
        The static row groups, with `_BOOKKEEPING` then the dataset's columns as vectors; and
        how many of them hold rows.
    """
    groups: list[Table] = []
    if rows is not None:
        groups = [
            _key_row(symbol, seq, part=part, decided_at_step=decided_at_step).join(
                rows.slice(start, min(rows.size, start + GROUP_ROWS)).group_by()
            )
            for part, start in enumerate(range(0, rows.size, GROUP_ROWS))
        ]
    parts = len(groups)
    groups += [
        _key_row(symbol, seq, part=part, decided_at_step=decided_at_step).natural_join(
            empty, on=_KEY
        )
        for part in range(parts, max(parts_before, 1))
    ]
    return merge(groups), parts


def _latest_row_groups(blink: Table, seed: Table | None) -> Table:
    """Return each symbol's newest row group of each part, the seed's until a download lands.

    `last_by` keeps one row group per symbol and part, so a newer download replaces each part,
    its empty row groups included. With `wait=True`, the seed's row groups of a symbol stand in
    until a download of that symbol is published: the seed's `natural_join` copies `_SEQ` from
    the newest part 0, which every download has, so a symbol never shows the seed's rows and a
    published download at once, even an empty one.

    Args:
        blink: The result's blink table of row groups.
        seed: The call-time symbols' row groups (`wait=True`), or None.

    Returns:
        The row groups, with every column of the blink table.
    """
    latest = blink.last_by([_KEY, _PART])
    if seed is None:
        return latest
    published = latest.where(f"{_PART} == 0").view([_KEY, _SEQ])
    unpublished_seed = (
        seed.natural_join(published, on=_KEY, joins=f"{_NEWER_SEQ}={_SEQ}")
        .where(f"isNull({_NEWER_SEQ})")
        .drop_columns(_NEWER_SEQ)
    )
    return merge([unpublished_seed, latest])


def _hide_stale_downloads(rows: Table, followed: Table) -> Table:
    """Under `Reread.ALWAYS`, hide each row group decided before its symbol last joined.

    A returning symbol must show no rows until its new download lands, though its earlier row
    groups stay the newest until the empty download that frees them lands. So each symbol is
    stamped with the update cycle its row last joined the table (`_JOINED_AT_STEP`), and a row
    group shows only when its `_DECIDED_AT_STEP`, the latest cycle of the table whose change the
    coordinator had applied when it decided to publish the download, is that join cycle or
    later. The coordinator hears of a join, with its cycle, before it starts the read, so the
    new download always shows. A read of the earlier membership was decided before the
    coordinator heard of the removal, so its row groups are stamped with an earlier cycle and
    never show, even when the coordinator publishes them after the symbol has left and come back
    (the removal and re-add were queued behind its outcome). The landing cycle could not tell
    these apart: such a stale download lands after the re-add. Cycle numbers come from one
    logical clock, so, unlike readings of two wall clocks, they never run backwards against
    each other. A seed row group, read at the call, is stamped with the build cycle, so it shows
    while its symbol has not left the table since the result was made. Every part of a download
    carries one stamp, so a download shows whole or not at all. The caller holds the update
    graph's shared lock, so no cycle runs while this is built.

    Args:
        rows: The row groups of the symbols in the symbol table.
        followed: The user's symbol table, made append-only when it is a blink table.

    Returns:
        The row groups that are not stale, with the same columns.
    """
    joined = followed.select_distinct(_SYMBOL).update(f"{_JOINED_AT_STEP} = {CURRENT_STEP}")
    return (
        rows.natural_join(joined, on=f"{_KEY}={_SYMBOL}", joins=_JOINED_AT_STEP)
        .where(f"!isNull({_JOINED_AT_STEP}) && {_DECIDED_AT_STEP} >= {_JOINED_AT_STEP}")
        .drop_columns(_JOINED_AT_STEP)
    )


def _shown_row_groups(
    blink: Table, seed: Table | None, followed: Table, *, reread: Reread
) -> Table:
    """Return the row groups the result shows: those of the symbols in the symbol table.

    Args:
        blink: The result's blink table of row groups.
        seed: The call-time symbols' row groups (`wait=True`), or None.
        followed: The user's symbol table, made append-only when it is a blink table.
        reread: Whether a returning symbol is read again. Under `Reread.NEVER` a returning
            symbol shows its kept row groups at once, so none is stale.

    Returns:
        The row groups, with only the dataset's columns, as vectors.
    """
    # A symbol whose row leaves the symbol table loses its rows in that cycle.
    in_watchlist = watched_rows(_latest_row_groups(blink, seed), followed, key=_KEY)
    if reread is Reread.ALWAYS:
        in_watchlist = _hide_stale_downloads(in_watchlist, followed)
    return in_watchlist.drop_columns(list(_BOOKKEEPING))


def _seed(
    seeded: Mapping[str, Table], columns: tuple[str, ...], *, built_step: int, empty: Table
) -> Table:
    """Return the static table of the call-time symbols' row groups.

    Flattened, so its row keys are dense: `ungroup` shifts each row key of the row groups left
    by up to 30 bits (`GROUP_ROWS`), and the shifted key must fit in a long, while a merge
    spaces its tables' keys 2^16 apart.

    Args:
        seeded: Each symbol's static rows.
        columns: The dataset's columns, in schema order.
        built_step: The update cycle the result is built at, each row group's
            `_DECIDED_AT_STEP`.
        empty: The template of an empty row group (`_empty_row_group`).

    Returns:
        Each symbol's row groups, numbered 0.
    """
    return merge(
        [
            _row_groups(
                name,
                0,
                _viewed(seeded[name], columns),
                decided_at_step=built_step,
                parts_before=0,
                empty=empty,
            )[0]
            for name in sorted(seeded)
        ]
    ).flatten()


def _build(
    core: SessionCore,
    spec: DatasetSpec,
    read: FollowedRead,
    *,
    symbol_table: Table,
    rule: SymbolRowRule,
    values_column: str | None,
    router: _WatchRouter,
    seeded: Mapping[str, Table],
    note: str,
) -> tuple[tuple[Table, BlinkPublisher, SymbolWatch, int, Table], LivenessScope]:
    """Create a followed read's result and watch in a new result scope (`make_result`).

    The caller holds the update graph's shared lock (`open_followed_read`).

    Args:
        core: The session's core.
        spec: The dataset.
        read: The result's shared state.
        symbol_table: The user's refreshing symbol table, or table of values.
        rule: The dataset's rule for the table's rows, which names the column read.
        values_column: The column of a table of values, which its problems name in `Field`;
            None for a symbol table.
        router: Receives the watch's reports.
        seeded: The call-time symbols' static rows (`wait=True`), or empty.
        note: The sentences the description adds after the dataset's summary.

    Returns:
        The result, its publisher, the watch (not started), the update cycle it was built at,
        and the template of an empty row group; and the result scope.
    """
    built: list[tuple[BlinkPublisher, SymbolWatch, int, Table]] = []
    schema = core.dataset_schema(spec)

    def shape() -> Table:
        # The result ungroups row groups, each holding up to GROUP_ROWS of a symbol's rows as
        # vectors. A blink publisher plus `last_by` stands in for a keyed input table, which
        # the library never uses, and `watched_rows` (a `where_in` against the user's table)
        # removes a symbol's rows. `ungroup`'s columns are stateless, so `view` works on them.
        empty = _empty_row_group(schema)
        blink, publisher = blink_publisher(
            f"massive.{spec.name}#{read.call}",
            TableDefinition(
                _key_row("", 0, part=0, decided_at_step=0)
                .natural_join(empty, on=_KEY)
                .j_table.getDefinition()
            ),
            on_shutdown=functools.partial(
                _shut_down,
                weakref.ref(core),
                weakref.ref(read),
                worker_shut_down=core.workers.shutdown_callback(read.call, read.stop_token),
            ),
            release_later=core.release_later,
        )
        # No cycle runs: every symbol in the table now joined at this cycle or before.
        built_step = update_cycle(core.exec_ctx)
        seed = (
            _seed(seeded, schema.column_names, built_step=built_step, empty=empty)
            if seeded
            else None
        )
        followed = blink_to_append_only(symbol_table) if symbol_table.is_blink else symbol_table
        # A table of product codes is matched by its `ProductCode` as a watchlist by its `Symbol`.
        keyed = (
            followed if rule.column == _SYMBOL else followed.view([f"{_SYMBOL} = {rule.column}"])
        )
        groups = _shown_row_groups(blink, seed, keyed, reread=read.reread)
        result = describe(groups.ungroup(list(schema.column_names)), spec, schema, note=note)
        # The problems of a table of values name its column, as a `poll()`'s do.
        skipped = SkippedValues(
            rule,
            stream_id=read.call,
            report=functools.partial(_report_problems, weakref.ref(core)),
            values=None if values_column is None else rule,
        )
        watch = SymbolWatch(
            rule,
            followed,
            router.on_change,
            stream_id=read.call,
            on_invalid=router.on_invalid,
            on_skipped=skipped.update if values_column is None else skipped.update_values,
        )
        refused = refused_values_of(symbol_table, rule.column)
        if refused is not None:  # a table `canonicalize_symbols` returned
            refused.follow(skipped, field=values_column)
        # A table derived from the result keeps the blink table, and so the watch, alive.
        watch.keep_alive_with(blink)
        built.append((publisher, watch, built_step, empty))
        return result

    result, scope = make_result(core.exec_ctx, shape)
    publisher, watch, built_step, empty = built[0]
    return (result, publisher, watch, built_step, empty), scope


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
    values_column: str | None = None,
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
        values_column: For a table passed where the dataset takes one value, its column,
            which its problems name in `Field`; None for a symbol table.

    Returns:
        The refreshing result: every column of the dataset, with the call-time symbols'
        rows when `first` holds them.

    Raises:
        MassiveError: If the session closed before the result could be registered, or after
            (`"MassiveSession #1 is closed; ..."`).
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
            (result, publisher, watch, built_step, empty), scope = _build(
                core,
                spec,
                read,
                symbol_table=symbol_table,
                rule=rule,
                values_column=values_column,
                router=router,
                seeded=seeded,
                note=note,
            )
        read.attach(publisher, watch, built_step=built_step, empty=empty)

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

        core.workers.track(result, scope, make)
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


def _report_problems(
    core_ref: Callable[[], SessionCore | None], problems: Sequence[Problem]
) -> None:
    """Hand problems of a followed read's symbol table to its session, if it still exists.

    Its `SkippedValues` calls it from the watch's listener, on the caller thread for the
    start's replay, and from a converted watchlist's listener: the session's problem sink logs
    and queues, and never blocks. The session is held weakly: Java holds the listener.

    Args:
        core_ref: Returns the session's core, or None once it is freed.
        problems: The problems.
    """
    core = core_ref()
    if core is not None:
        core.problems(problems)
