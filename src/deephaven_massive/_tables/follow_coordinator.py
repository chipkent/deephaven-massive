"""The follow coordinator: the one owner of every followed read's per-symbol state in a session.

A `history()` or `fetch()` whose `symbols` is a refreshing table follows it
(`_tables/followed_read.py`): each symbol added to the table is read (over `history()`'s fixed
window, or as Massive answers at the read) and lands in the result whole, and each symbol
removed leaves it. This module decides, for every such result of a
session, which symbol is read when, what its read's outcome does, and what the diagnostics
show. It owns:

- **One coordinator thread per session** (`massive #1 follow`), started by the session's first
  followed read and kept until the session closes or is collected (a `QueueWorker`), so one
  thread only ever owns the state. It takes every event from
  one queue, in order: the symbol table's changes (from the watch's listener, which only
  queues), each read's start and outcome (from the read pool), and each result's stop (from its
  blink table's shutdown). It is the only thread that reads or changes per-symbol state, so that
  state needs no lock. It makes no request and waits on nothing: it publishes each symbol's
  cell (`FollowedRead.publish`, which never waits for an update cycle), times the retries, and
  queues the `m.streams()` and `m.symbol_reads()` rows.
- **One read pool per session**, a `ThreadPoolExecutor` of `MassiveConfig.max_concurrency`
  threads (`massive #1 follow-read_0`), made and ended with the coordinator thread.
  A read's outcome that arrives once its result's stop token is set is dropped: the result was
  released and is unused, or failed. Each task
  reads some symbols of one result (`FollowPlan.read`) in the session's execution context,
  listening to that result's stop token, and queues the outcome. A REST read reads up to
  `FollowPlan.symbols_per_read` symbols (one, or a `fetch()` list parameter's batch); a
  flat-file read reads every symbol waiting at once, in one scan, and a result has at most
  one scan in flight, since a scan downloads the same files whatever the number of symbols.

**A symbol's life.** Added, it is `QUEUED`; a read of it makes it `LOADING`; its rows make it
`DONE`. A failure that may pass (`FailureKind.TRANSIENT`) makes it `RETRYING`, read again 1, 5,
and 15 minutes after the first, second, and third failure (`RETRY_DELAYS`), and `FAILED` after
the fourth; any other failure makes it `FAILED` at once. Removed, it is `REMOVED`: under
`Reread.ALWAYS` its cell is replaced by an empty one (a *tombstone*) so its rows are freed, and
a re-add reads it again; under `Reread.NEVER` its cell is kept, so a re-add shows the earlier
download at once, with no request. A `FAILED` symbol is read again on every re-add. Each add
and removal moves the symbol to a new *generation*: the outcome of a read of an older one is
dropped, so no removed or overtaken read ever lands, and a symbol is never read twice at once
(a re-add during a read waits for it to end). When the result stops (released and unused, or
failed), every symbol still following is `STOPPED`.

**Whole-table failures.** A read that raises (`AuthenticationError`, a session closed during
the read, or a defect) and a symbol table that fails or, under
`noncanonical_symbols=RAISE`, gains a bad row fail the result (`FollowedRead.fail_table`):
the coordinator records the failure and publishes it, never the listener. So does a crypto pair
Massive's crypto ticker list does not hold, under `unlisted_symbols=RAISE`.

**Time.** The coordinator reads `CLOCK`: the wall clock for the diagnostics' instants and the
monotonic clock for the retries. Its queue wait ends at the next retry; tests replace `CLOCK`
with a virtual one and wake the coordinator (`wake`) after moving it.

The module is one subject although it runs past 1,000 lines: every event of a followed read
(a change of its symbol table, a read's start or outcome, a retry coming due, a stop) changes
the same per-symbol state on the one coordinator thread, so the state machine and the rows it
shows read whole only here.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, TypeAlias, cast

from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._enums import OnError, Reread
from deephaven_massive._errors import (
    LIBRARY_BUG,
    ErrorCause,
    MassiveError,
    UnlistedPairError,
    failure_text,
    library_bug,
)
from deephaven_massive._log_limits import bounded_list, log_failure, mark_failure_logged
from deephaven_massive._tables import poll
from deephaven_massive._tables.diagnostics import (
    FLAT_FILES_AND_REST,
    NotePart,
    StreamUpdate,
    describe_symbols,
)
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import QueueWorker
from deephaven_massive._tables.poll import SymbolRowError
from deephaven_massive._tables.symbol_read_rows import SymbolReadRow, SymbolReadState
from deephaven_massive._tables.transports import FailureKind, SymbolFailure, SymbolRows

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

    from deephaven.execution_context import ExecutionContext

    from deephaven_massive._tables.followed_read import FollowedRead
    from deephaven_massive._tables.poll import PollClock
    from deephaven_massive._tables.transports import SymbolOutcome

__all__ = ["CLOCK", "RETRY_DELAYS", "FirstRead", "FollowCoordinator"]

logger = logging.getLogger(__name__)

CLOCK: PollClock = poll.CLOCK
"""The coordinator's time source; tests replace it with a virtual clock."""

RETRY_DELAYS: Final = (60.0, 300.0, 900.0)
"""Seconds after a symbol's first, second, and third transient failure before it is read again;
the fourth failure is final."""

_FOLLOWING: Final = frozenset(
    {SymbolReadState.QUEUED, SymbolReadState.LOADING, SymbolReadState.RETRYING}
)
"""The states of a symbol still waiting for its rows: the `Loading` count of `m.streams()`."""

_ENDED: Final = frozenset({SymbolReadState.FAILED, SymbolReadState.REMOVED, SymbolReadState.CLOSED})
"""The states a stop leaves as they are; every other becomes `STOPPED`."""

_CAUSES_SHOWN: Final = 3
"""The most causes one `Error` part of `m.streams()` names."""

_SYMBOLS_SHOWN: Final = 5
"""The most symbols one cause of an `Error` part lists."""

_CAUSE_SHOWN: Final = 200
"""The longest text of one cause in `Error`, in characters."""


@dataclass(frozen=True, slots=True)
class FirstRead:
    """The outcome of the read `history(wait=True)` makes of its call-time symbols.

    Made on the caller thread before the result exists, and handed to the coordinator with
    the result (`FollowCoordinator.follow`), which records each symbol's outcome as if its
    own pool had read it. Immutable.

    Attributes:
        outcomes: Each call-time symbol's rows (already in the result's seed) or failure.
        began_at: When the read began, epoch nanoseconds: each `DONE` symbol's `ReadAt`.
        seconds: How long the read took, for the log.
    """

    outcomes: Mapping[str, SymbolOutcome]
    began_at: int
    seconds: float


@dataclass(frozen=True, slots=True)
class _Follow:
    """A new followed read, with what its caller already read."""

    read: FollowedRead
    first: FirstRead | None


@dataclass(frozen=True, slots=True)
class _Baseline:
    """The symbol table's whole set of symbols when its watch started.

    Every later change is relative to it. `step` is the update cycle the watch replayed the
    table at, or 0 when the table held no symbol then.
    """

    read: FollowedRead
    symbols: frozenset[str]
    step: int


@dataclass(frozen=True, slots=True)
class _Changed:
    """Symbols the table gained and lost in one update cycle, the cycle `step`."""

    read: FollowedRead
    added: frozenset[str]
    removed: frozenset[str]
    step: int


@dataclass(frozen=True, slots=True)
class _Skipped:
    """The table's values the result skips now, because they are not Massive's tickers."""

    read: FollowedRead
    text: str | None


@dataclass(frozen=True, slots=True)
class _Invalid:
    """A bad row under `noncanonical_symbols=RAISE`, or the symbol table's own failure."""

    read: FollowedRead
    message: str


@dataclass(frozen=True, slots=True)
class _Began:
    """A pool thread began reading some symbols of one result, at these generations."""

    read: FollowedRead
    generations: Mapping[str, int]


@dataclass(frozen=True, slots=True)
class _Outcome:
    """A read's outcome: each symbol's rows or failure, or the error that failed the table."""

    read: FollowedRead
    generations: Mapping[str, int]
    outcomes: Mapping[str, SymbolOutcome]
    began_at: int
    seconds: float
    table_error: MassiveError | None = None


@dataclass(frozen=True, slots=True)
class _Stop:
    """The result stopped: released and unused, failed, or closed."""

    read: FollowedRead


@dataclass(frozen=True, slots=True)
class _Wake:
    """Nothing but a wake-up: the coordinator checks the retries that are due."""


_Message: TypeAlias = (
    "_Follow | _Baseline | _Changed | _Skipped | _Invalid | _Began | _Outcome | _Stop | _Wake"
)


@dataclass(slots=True)
class _Symbol:
    """One symbol of one followed read; touched only by the coordinator thread.

    Attributes:
        state: Its `m.symbol_reads()` state.
        in_table: Whether the symbol table holds it now.
        generation: Moves on every add and removal; a read's outcome counts only at the
            generation it was started for.
        reading: The generation a read in flight was started for, or None.
        waiting: Whether it waits for a read slot (its result's queue).
        attempts: Reads begun since it was last added with a read queued.
        due: When its next read is due (monotonic seconds), while `RETRYING`.
        next_attempt_at: The same instant, epoch nanoseconds.
        added_at: When its row last entered the table, epoch nanoseconds.
        read_at: When the download whose rows the result holds began, or None.
        rows: The rows of its cell in the result (0 for none).
        kept: Whether the result's latest cell for it holds rows (not a tombstone).
        error: Its latest failure's text, until a read succeeds.
        cause: Its latest failure's cause, which groups it in `Error`.
        transport: The source of its latest read, as `m.symbol_reads()` shows it.
    """

    state: SymbolReadState
    in_table: bool
    generation: int
    added_at: int
    reading: int | None = None
    waiting: bool = False
    attempts: int = 0
    due: float | None = None
    next_attempt_at: int | None = None
    read_at: int | None = None
    rows: int = 0
    kept: bool = False
    error: str | None = None
    cause: str | None = None
    transport: str | None = None


@dataclass(slots=True)
class _Result:
    """The coordinator's state of one followed read.

    Attributes:
        read: The result's handle.
        symbols: Every symbol it has followed, by name.
        queue: The symbols waiting for a read, in the order they began to wait.
        scanning: Whether a flat-file scan of the result is in flight.
        seq: The number of the result's latest cell.
        ended: Whether it failed or stopped: no read starts, and no cell is published.
        dirty: The symbols whose `m.symbol_reads()` row changed since it was last written.
        warned: The causes of failure already logged at WARNING for the result.
        transports: The sources its reads have used.
        skipped: The text of its skipped symbol-table values, or None.
        shown: The `m.streams()` progress last written, to skip an unchanged write.
        heard: The latest update cycle of the symbol table whose change the coordinator has
            applied (the result's build cycle until the first), stamped on every cell it
            publishes; see `FollowedRead.publish`.
    """

    read: FollowedRead
    heard: int
    symbols: dict[str, _Symbol] = field(default_factory=dict)
    queue: list[str] = field(default_factory=list)
    scanning: bool = False
    seq: int = 0
    ended: bool = False
    dirty: set[str] = field(default_factory=set)
    warned: set[str] = field(default_factory=set)
    transports: set[str] = field(default_factory=set)
    skipped: str | None = None
    shown: object = None


def _now_ns() -> int:
    """Return the wall-clock time of `CLOCK` in epoch nanoseconds."""
    return int(CLOCK.time() * 1e9)


def _cause(error: MassiveError) -> str:
    """Return the key that groups failures of one cause: the error's type and HTTP status.

    Args:
        error: A symbol's failure.

    Returns:
        Such as `"EntitlementError 403"`, or `"MassiveError"` for a failure without a status.
    """
    name = type(error).__name__
    return name if error.status is None else f"{name} {error.status}"


def _transport(read: FollowedRead, outcome: SymbolOutcome) -> str:
    """Return the source a symbol's read used, as `m.symbol_reads()` shows it.

    Args:
        read: The result, whose plan names its source.
        outcome: The symbol's outcome.

    Returns:
        The source the rows name (`SymbolRows.source`), such as `"flat files and REST"`, or
        else the plan's source, such as `"REST"`.
    """
    if isinstance(outcome, SymbolRows) and outcome.source:
        return outcome.source
    return TRANSPORT_LABELS[read.plan.transport]


def _shown_transport(transports: Iterable[str]) -> str | None:
    """Return the `Transport` of a followed read's `m.streams()` row: the sources used so far.

    Args:
        transports: The sources its reads used, as `_transport` names them.

    Returns:
        The one source, `FLAT_FILES_AND_REST` when both REST and flat files served rows, or
        None before any read.
    """
    used = set(transports)
    if len(used) > 1:
        return FLAT_FILES_AND_REST
    return used.pop() if used else None


class FollowCoordinator:
    """The session's follow coordinator thread and read pool; see the module docstring.

    Created with the session's core, which holds it; it starts no thread until the session's
    first followed read (`follow`), and keeps its thread and pool until `stop` (the session's
    close, or its collection), so one thread owns the per-symbol state for the session's life.
    It never refers to the core. `follow`, `post`, `wake`, and `stop` are safe from any thread
    and never block: they take its lock only around a queue put. Every handler runs on the
    coordinator thread.
    """

    __slots__ = (
        "_closed",
        "_exec_ctx",
        "_following",
        "_lock",
        "_max_workers",
        "_pool",
        "_results",
        "_thread_prefix",
        "_worker",
    )

    def __init__(self, exec_ctx: ExecutionContext, thread_prefix: str, max_workers: int) -> None:
        """Prepare the coordinator; no thread starts yet.

        Args:
            exec_ctx: The session's execution context, entered by the coordinator thread and by
                each pool thread.
            thread_prefix: The session's thread prefix, such as `"massive #1"`.
            max_workers: The read pool's size, the session's `max_concurrency`.
        """
        self._exec_ctx = exec_ctx
        self._thread_prefix = thread_prefix
        self._max_workers = max_workers
        # Guards the thread, the pool, the followed reads, and the stop. Held only
        # around a queue put or a pool submit, which never waits on another thread's lock.
        self._lock = threading.Lock()
        self._worker: QueueWorker[_Message] | None = None
        self._pool: ThreadPoolExecutor | None = None
        # Every followed read handed over (`follow`) and not yet forgotten, by `id`: what a
        # dying thread fails, including a read whose start never reached it.
        self._following: dict[int, FollowedRead] = {}
        self._closed = False
        # Touched only by the coordinator thread.
        self._results: dict[int, _Result] = {}

    @property
    def thread(self) -> threading.Thread | None:
        """The coordinator thread, or None before the session's first followed read."""
        with self._lock:
            return None if self._worker is None else self._worker.thread

    @property
    def following(self) -> int:
        """How many followed reads the coordinator serves: counted until each one stops."""
        with self._lock:
            return len(self._following)

    def follow(self, read: FollowedRead, first: FirstRead | None) -> None:
        """Start coordinating a new followed read, starting the thread and pool if needed.

        Called on the caller thread once the result is registered, before its watch starts,
        so the result's own events queue behind it.

        Args:
            read: The result.
            first: The outcome of `history(wait=True)`'s read of the call-time symbols, or None
                for `wait=False`.
        """
        with self._lock:
            if self._closed:  # the session closed: the result's own check refuses it
                return
            if self._worker is None:
                self._pool = ThreadPoolExecutor(
                    self._max_workers, thread_name_prefix=f"{self._thread_prefix} follow-read"
                )
                self._worker = QueueWorker(
                    self._exec_ctx,
                    f"{self._thread_prefix} follow",
                    handle=self._handle,
                    timeout=self._next_due,
                    on_timeout=self._retry_due,
                    on_exit=self._exited,
                    on_death=self._died,
                )
            started = not self._following
            self._following[id(read)] = read
            self._worker.put(_Follow(read, first))
        if started:
            logger.debug("%s follow: a followed read starts", self._thread_prefix)

    def stop(self) -> None:
        """End the coordinator thread and its pool, without waiting; idempotent.

        Called by `SessionCore.close` and, through the reaper, by the collection of a session
        never closed. The events queued before it are handled first; later ones are dropped.
        """
        with self._lock:
            self._closed = True
            worker, pool = self._worker, self._pool
            self._worker = self._pool = None
        if worker is not None:
            worker.stop()
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)

    def post(self, message: _Message) -> None:
        """Queue an event for the coordinator; never blocks. Dropped when no result follows.

        Args:
            message: The event.
        """
        with self._lock:
            worker = self._worker
            if worker is not None:
                worker.put(message)

    def wake(self) -> None:
        """Make the coordinator check the retries that are due now (after `CLOCK` moved)."""
        self.post(_Wake())

    # --- Events from other threads, for a result -------------------------------------------

    def baseline(self, read: FollowedRead, symbols: frozenset[str], step: int) -> None:
        """Queue the symbol table's set of symbols when its watch started.

        Args:
            read: The result.
            symbols: The set.
            step: The update cycle the watch replayed the table at, or 0 for an empty set.
        """
        self.post(_Baseline(read, symbols, step))

    def changed(
        self, read: FollowedRead, added: frozenset[str], removed: frozenset[str], *, step: int
    ) -> None:
        """Queue one change of the symbol table's set (the watch's listener calls it).

        Args:
            read: The result.
            added: The symbols gained.
            removed: The symbols lost.
            step: The update cycle that made the change.
        """
        self.post(_Changed(read, added, removed, step))

    def skipped(self, read: FollowedRead, text: str | None) -> None:
        """Queue the text of the table's skipped values, or None once none is skipped.

        Args:
            read: The result.
            text: As `skipped_rows_text` writes it, or None.
        """
        self.post(_Skipped(read, text))

    def invalid(self, read: FollowedRead, message: str) -> None:
        """Queue a bad row under `RAISE`, or the symbol table's failure: the result fails.

        Args:
            read: The result.
            message: Why, as the watch says it.
        """
        self.post(_Invalid(read, message))

    def stop_following(self, read: FollowedRead) -> None:
        """Queue the result's stop (its blink table shut down, or it failed or closed).

        Args:
            read: The result.
        """
        self.post(_Stop(read))

    # --- The coordinator thread -----------------------------------------------------------

    def _handle(self, batch: list[_Message]) -> None:
        """Handle one batch of events in order, then start reads and write the rows.

        Args:
            batch: The events.
        """
        touched: dict[int, _Result] = {}
        for message in batch:
            if isinstance(message, _Wake):
                continue
            if isinstance(message, _Follow):
                result = self._start(message)
            else:
                found = self._results.get(id(message.read))
                if found is None:  # an event of a result already stopped
                    logger.debug(
                        "follow: an event for call %d after it stopped: %s",
                        message.read.call,
                        type(message).__name__,
                    )
                    continue
                result = found
                try:
                    self._apply(result, message)
                except Exception as error:  # one result's defect; the others go on
                    logger.exception(
                        "call %d follow: handling %s failed. %s",
                        result.read.call,
                        type(message).__name__,
                        LIBRARY_BUG,
                    )
                    self._defect(result, message, error)
            touched[id(result.read)] = result
        self._retry_due(touched)
        for result in touched.values():
            self._schedule(result)
            self._show(result)
        self._forget_stopped(touched)

    def _apply(self, result: _Result, message: _Message) -> None:
        """Apply one event of a known result.

        Args:
            result: The result's state.
            message: The event.
        """
        if isinstance(message, (_Baseline, _Changed)):
            self._membership(result, message)
        elif isinstance(message, _Skipped):
            result.skipped = message.text
        elif isinstance(message, _Invalid):
            self._fail(result, SymbolRowError(message.message))
        elif isinstance(message, _Began):
            for name, generation in message.generations.items():
                symbol = result.symbols[name]
                if symbol.generation == generation and symbol.in_table:
                    symbol.state = SymbolReadState.LOADING
                    symbol.attempts += 1
                    result.dirty.add(name)
        elif isinstance(message, _Outcome):
            self._landed(result, message)
        else:  # _Stop
            self._stopped(result)

    def _defect(self, result: _Result, message: _Message, error: Exception) -> None:
        """Fail a result whose event raised: its state may no longer be whole.

        The caller logged the defect with its traceback; this fails the result's table with a
        `MassiveError` that names it (`ErrorCause.LIBRARY_DEFECT`), marked as logged.

        Args:
            result: The result's state.
            message: The event that raised.
            error: What it raised.
        """
        failure = MassiveError(
            library_bug(
                f"{result.read.dataset}: the followed read failed handling "
                f"{type(message).__name__}: {type(error).__name__}: {error}"
            ),
            cause=ErrorCause.LIBRARY_DEFECT,
        )
        mark_failure_logged(failure)  # the caller logged it, with its trace
        self._fail(result, failure)

    def _membership(self, result: _Result, message: _Baseline | _Changed) -> None:
        """Apply a change of the symbol table's set: removals first, then adds.

        Args:
            result: The result's state.
            message: The whole set when the watch started, or one change.
        """
        if isinstance(message, _Baseline):
            held = {name for name, symbol in result.symbols.items() if symbol.in_table}
            added, removed = message.symbols - held, frozenset(held - message.symbols)
        else:
            added, removed = message.added, message.removed
        result.heard = max(result.heard, message.step)
        now = _now_ns()
        for name in sorted(removed):
            self._remove(result, name)
        for name in sorted(added):
            self._add(result, name, now)
        logger.debug(
            "call %d follow: the symbol table gained %s and lost %s",
            result.read.call,
            bounded_list(sorted(added)),
            bounded_list(sorted(removed)),
        )

    def _start(self, message: _Follow) -> _Result:
        """Start following a result: record what its caller read, if anything.

        Args:
            message: The new result and its first read.

        Returns:
            Its state.
        """
        read = message.read
        result = _Result(read, heard=read.built_step)
        self._results[id(read)] = result
        if read.core.closed:  # the close failed the table while it was handed over
            result.ended = True
            logger.debug("call %d follow: the session closed before it started", read.call)
            return result
        first = message.first
        if first is not None:
            # The call warned of its failed symbols already: once per result and cause.
            result.warned.update(
                _cause(outcome.error)
                for outcome in first.outcomes.values()
                if isinstance(outcome, SymbolFailure)
            )
            for name, outcome in first.outcomes.items():
                symbol = _Symbol(
                    state=SymbolReadState.LOADING,
                    in_table=True,
                    generation=1,
                    added_at=first.began_at,
                    attempts=1,
                )
                result.symbols[name] = symbol
                if isinstance(outcome, SymbolRows):  # its cell is in the result's seed
                    symbol.kept = True
                self._record(result, name, outcome, began_at=first.began_at, seconds=first.seconds)
        logger.debug(
            "call %d follow: started (%s, reread %s, %d symbols read by the call)",
            read.call,
            TRANSPORT_LABELS[read.plan.transport],
            read.reread.name,
            0 if first is None else len(first.outcomes),
        )
        return result

    def _add(self, result: _Result, name: str, now: int) -> None:
        """A symbol entered the table: reuse its kept download under `NEVER`, or queue a read.

        Args:
            result: The result's state.
            name: The symbol.
            now: The instant of the add, epoch nanoseconds.
        """
        symbol = result.symbols.get(name)
        if symbol is None:
            symbol = result.symbols[name] = _Symbol(
                state=SymbolReadState.QUEUED, in_table=True, generation=0, added_at=now
            )
        symbol.in_table = True
        symbol.generation += 1
        symbol.added_at = now
        result.dirty.add(name)
        if result.read.reread is Reread.NEVER and symbol.kept:
            symbol.state = SymbolReadState.DONE
            logger.debug(
                "call %d follow %s: back in the table; its download of %s is kept (NEVER), "
                "no request",
                result.read.call,
                name,
                _ns_text(cast("int", symbol.read_at)),  # a kept download has begun
            )
            return
        symbol.state = SymbolReadState.QUEUED
        symbol.attempts = 0
        symbol.due = symbol.next_attempt_at = None
        self._enqueue(result, name)

    def _remove(self, result: _Result, name: str) -> None:
        """A symbol left the table: drop its pending read and retry, and its rows under `ALWAYS`.

        Its rows leave the result in the cycle its row left the table (the result joins the
        table), so the tombstone only frees them.

        Args:
            result: The result's state.
            name: The symbol.
        """
        symbol = result.symbols[name]
        symbol.in_table = False
        symbol.generation += 1
        symbol.state = SymbolReadState.REMOVED
        symbol.due = symbol.next_attempt_at = None
        result.dirty.add(name)
        if symbol.waiting:
            symbol.waiting = False
            result.queue.remove(name)
        if result.read.reread is Reread.ALWAYS and symbol.kept:
            result.seq += 1
            result.read.publish(name, result.seq, None, heard=result.heard)
            symbol.kept = False
            symbol.read_at = None
            symbol.rows = 0

    def _enqueue(self, result: _Result, name: str) -> None:
        """Put a symbol at the end of its result's queue of symbols waiting for a read.

        Only a symbol that was not waiting comes here: a new add, or a retry come due.

        Args:
            result: The result's state.
            name: The symbol.
        """
        result.symbols[name].waiting = True
        result.queue.append(name)

    def _landed(self, result: _Result, message: _Outcome) -> None:
        """Apply a read's outcome: each symbol whose generation still matches takes it.

        Args:
            result: The result's state.
            message: The outcome.
        """
        if result.read.plan.one_scan:
            result.scanning = False
        for name in message.generations:
            result.symbols[name].reading = None  # its only read in flight has ended
        if result.read.stop_token.is_set():  # released and unused, or failed: drop it
            logger.debug("call %d follow: a read ended after the stop; dropped", result.read.call)
            return
        if message.table_error is not None:
            self._fail(result, message.table_error)
            return
        for name, generation in message.generations.items():
            symbol = result.symbols[name]
            if symbol.generation != generation or not symbol.in_table or result.ended:
                logger.debug(
                    "call %d follow %s: a read overtaken by a later add or removal ended; "
                    "its outcome is dropped",
                    result.read.call,
                    name,
                )
                continue
            self._record(
                result,
                name,
                message.outcomes[name],
                began_at=message.began_at,
                seconds=message.seconds,
                publish=True,
            )

    def _record(
        self,
        result: _Result,
        name: str,
        outcome: SymbolOutcome,
        *,
        began_at: int,
        seconds: float,
        publish: bool = False,
    ) -> None:
        """Record one symbol's outcome: its rows land, or it retries, or it fails.

        Args:
            result: The result's state.
            name: The symbol.
            outcome: Its rows or failure.
            began_at: When the read began, epoch nanoseconds.
            seconds: How long it took.
            publish: True to publish its cell; False for a symbol already in the seed.
        """
        symbol = result.symbols[name]
        read = result.read
        transport = _transport(read, outcome)
        symbol.transport = transport
        result.transports.add(transport)
        result.dirty.add(name)
        if isinstance(outcome, SymbolRows):
            if publish:
                result.seq += 1
                read.publish(name, result.seq, outcome.table, heard=result.heard)
            symbol.state = SymbolReadState.DONE
            symbol.kept = True
            symbol.rows = outcome.table.size
            symbol.read_at = began_at
            symbol.error = symbol.cause = None
            symbol.due = symbol.next_attempt_at = None
            logger.debug(
                "call %d %s: %d rows in %.1f s from %s (cell %d, heard cycle %d)",
                read.call,
                name,
                symbol.rows,
                seconds,
                transport,
                result.seq,
                result.heard,
            )
            return
        self._failed(result, name, outcome)

    def _failed(self, result: _Result, name: str, outcome: SymbolFailure) -> None:
        """Make a failed symbol `RETRYING` or `FAILED`, and log it.

        A crypto pair Massive's crypto ticker list does not hold (`UnlistedPairError`) follows
        `MassiveConfig.unlisted_symbols`: `FAILED` with a WARNING under `LOG`, with only a
        DEBUG line under `IGNORE`, and under `RAISE` it fails the whole result.

        Args:
            result: The result's state.
            name: The symbol.
            outcome: Its failure.
        """
        symbol = result.symbols[name]
        error = outcome.error
        unlisted = (
            result.read.core.config.unlisted_symbols
            if isinstance(error, UnlistedPairError)
            else None
        )
        if unlisted is OnError.RAISE:
            logger.debug(
                "call %d follow %s: fails the result, under unlisted_symbols=RAISE: %s",
                result.read.call,
                name,
                error,
            )
            self._fail(result, error)
            return
        symbol.error = failure_text(error)
        symbol.cause = _cause(error)
        attempts = max(symbol.attempts, 1)
        if outcome.kind is FailureKind.TRANSIENT and attempts <= len(RETRY_DELAYS):
            delay = RETRY_DELAYS[attempts - 1]
            symbol.state = SymbolReadState.RETRYING
            symbol.due = CLOCK.monotonic() + delay
            symbol.next_attempt_at = _now_ns() + int(delay * 1e9)
            logger.debug(
                "call %d follow %s: attempt %d failed in a way that may pass; read again in "
                "%.0f s: %s",
                result.read.call,
                name,
                attempts,
                delay,
                symbol.error,
            )
            return
        symbol.state = SymbolReadState.FAILED
        symbol.due = symbol.next_attempt_at = None
        first = symbol.cause not in result.warned
        result.warned.add(symbol.cause)
        mark_failure_logged(error)
        level = logging.WARNING if first else logging.DEBUG
        if unlisted is not None:
            level = logging.WARNING if first and unlisted is OnError.LOG else logging.DEBUG
        elif error.cause is not ErrorCause.EXPECTED and first:
            level = logging.ERROR
        logger.log(
            level,
            "%s call %d: %s failed after %d %s (%s), and is left out of the result; "
            "delete and re-add its row to read it again: %s",
            result.read.dataset,
            result.read.call,
            name,
            attempts,
            "attempt" if attempts == 1 else "attempts",
            outcome.kind.value,
            symbol.error,
        )

    def _retry_due(self, touched: dict[int, _Result] | None = None) -> None:
        """Queue every retry whose time has come.

        Args:
            touched: Receives each result with a retry queued; None when the coordinator woke
                for the retries alone, which then starts their reads itself.
        """
        now = CLOCK.monotonic()
        woke = touched is None
        touched = {} if touched is None else touched
        for key, result in self._results.items():
            if result.ended:
                continue
            for name, symbol in result.symbols.items():
                if symbol.state is SymbolReadState.RETRYING and (symbol.due or 0.0) <= now:
                    symbol.state = SymbolReadState.QUEUED
                    symbol.due = symbol.next_attempt_at = None
                    result.dirty.add(name)
                    self._enqueue(result, name)
                    touched[key] = result
        if woke:
            for result in touched.values():
                self._schedule(result)
                self._show(result)

    def _next_due(self) -> float | None:
        """Return the seconds until the next retry is due, for the queue wait.

        Returns:
            The seconds (0 or less: at once), or None when no retry waits.
        """
        dues = [
            symbol.due
            for result in self._results.values()
            if not result.ended
            for symbol in result.symbols.values()
            if symbol.due is not None
        ]
        return None if not dues else min(dues) - CLOCK.monotonic()

    def _schedule(self, result: _Result) -> None:
        """Start the reads the result's queue allows: REST groups, or one flat-file scan.

        A REST read takes up to `FollowPlan.symbols_per_read` waiting symbols, in queue order.
        A symbol whose earlier read is still in flight waits for it.

        Args:
            result: The result's state.
        """
        if result.ended or (result.read.plan.one_scan and result.scanning):
            return
        ready = [name for name in result.queue if result.symbols[name].reading is None]
        if not ready:
            return
        for name in ready:
            result.queue.remove(name)
            result.symbols[name].waiting = False
        size = len(ready) if result.read.plan.one_scan else result.read.plan.symbols_per_read
        batches = [ready[i : i + size] for i in range(0, len(ready), size)]
        if result.read.plan.one_scan:
            result.scanning = True
        for batch in batches:
            generations = {name: result.symbols[name].generation for name in batch}
            for name, generation in generations.items():
                result.symbols[name].reading = generation
            self._submit(result.read, generations)

    def _submit(self, read: FollowedRead, generations: Mapping[str, int]) -> None:
        """Hand one read to the pool, or drop it once `stop` has ended the pool.

        `stop` may end the pool while this thread still handles the events queued before it;
        a read of those is dropped, since every result is failed or about to fail. The submit
        runs under the lock, so `stop` never shuts the pool down between the check and the
        submit; a submit only queues the task and at most starts a pool thread.

        Args:
            read: The result.
            generations: The symbols to read, at their generations.
        """
        with self._lock:
            if self._pool is not None:
                self._pool.submit(self._read, read, dict(generations))
                return
        logger.debug(
            "call %d follow: the coordinator stopped; the read of %s is dropped",
            read.call,
            bounded_list(sorted(generations)),
        )

    def _read(self, read: FollowedRead, generations: Mapping[str, int]) -> None:
        """A pool task: read some symbols of one result and queue the outcome.

        Runs in the session's execution context. An error the read raises fails the result:
        a `MassiveError` as it is (a rejected key, a session closed during the read), anything
        else as a defect of the library, logged with its traceback.

        Args:
            read: The result.
            generations: The symbols, at the generations the read is for.
        """
        names = tuple(generations)
        self.post(_Began(read, generations))
        began_at = _now_ns()
        started = time.monotonic()
        outcomes: Mapping[str, SymbolOutcome] = {}
        table_error: MassiveError | None = None
        with entered(self._exec_ctx):
            try:
                outcomes = read.plan.read(names, read.stop_token)
            except MassiveError as error:
                table_error = error
            except BaseException as error:
                table_error = MassiveError(
                    library_bug(
                        f"{read.dataset}: the read of {', '.join(names)} failed: "
                        f"{type(error).__name__}: {error}"
                    ),
                    cause=ErrorCause.LIBRARY_DEFECT,
                )
                mark_failure_logged(table_error)  # logged here, with its trace
                logger.exception("call %d follow: a read failed. %s", read.call, LIBRARY_BUG)
        self.post(
            _Outcome(
                read,
                generations,
                outcomes,
                began_at,
                time.monotonic() - started,
                table_error,
            )
        )

    def _fail(self, result: _Result, error: MassiveError) -> None:
        """Fail a whole result once, and stop following its symbols.

        Args:
            result: The result's state.
            error: The failure.
        """
        if result.ended:
            return
        result.ended = True
        log_failure(
            logger,
            error,
            "%s call %d: the followed read failed, and its table with it: %s",
            result.read.dataset,
            result.read.call,
        )
        result.read.fail_table(error)

    def _stopped(self, result: _Result) -> None:
        """The result stopped: every symbol still following becomes `STOPPED`.

        Args:
            result: The result's state.
        """
        result.ended = True
        for name, symbol in result.symbols.items():
            if symbol.state not in _ENDED:
                symbol.state = SymbolReadState.STOPPED
                symbol.due = symbol.next_attempt_at = None
                result.dirty.add(name)
        result.queue.clear()
        logger.debug("call %d follow: stopped", result.read.call)

    def _show(self, result: _Result) -> None:
        """Write the result's changed `m.symbol_reads()` rows and its `m.streams()` progress.

        Args:
            result: The result's state.
        """
        read = result.read
        core = read.core
        if result.dirty:
            now = _now_ns()
            rows = [self._row(result, name, now) for name in sorted(result.dirty)]
            result.dirty.clear()
            core.write_symbol_reads(rows)
        current = sorted(name for name, symbol in result.symbols.items() if symbol.in_table)
        loading = sum(1 for name in current if result.symbols[name].state in _FOLLOWING)
        rows_now = sum(result.symbols[name].rows for name in current)
        notes = {
            NotePart.RETRYING_SYMBOLS: self._note(result, current, SymbolReadState.RETRYING),
            NotePart.FAILED_SYMBOLS: self._note(result, current, SymbolReadState.FAILED),
        }
        count, text = describe_symbols(tuple(current))
        shown = (tuple(current), loading, rows_now, tuple(notes.items()), result.skipped)
        if shown == result.shown:
            return
        result.shown = shown
        transport = _shown_transport(result.transports)
        core.update_followed_read(
            read.call,
            StreamUpdate(
                rows_published=rows_now,
                symbol_count=count,
                symbols=text,
                notes=notes,
                skipped=result.skipped,
                clear_skipped=result.skipped is None,
                loading=loading,
                transport=transport,
            ),
            loading=bool(loading),
        )

    @staticmethod
    def _row(result: _Result, name: str, now: int) -> SymbolReadRow:
        """Return a symbol's `m.symbol_reads()` row.

        Args:
            result: The result's state.
            name: The symbol.
            now: The row's `UpdatedAt`, epoch nanoseconds.

        Returns:
            The row.
        """
        symbol = result.symbols[name]
        return SymbolReadRow(
            stream_id=result.read.call,
            symbol=name,
            dataset=result.read.dataset,
            state=symbol.state,
            transport=symbol.transport,
            added_at=symbol.added_at,
            read_at=symbol.read_at if symbol.in_table or symbol.kept else None,
            rows=symbol.rows if symbol.in_table else 0,
            attempts=symbol.attempts,
            next_attempt_at=symbol.next_attempt_at,
            error=symbol.error,
            updated_at=now,
        )

    @staticmethod
    def _note(result: _Result, current: Sequence[str], state: SymbolReadState) -> str | None:
        """Return the `Error` part of the current symbols in one state, grouped by cause.

        Args:
            result: The result's state.
            current: The symbols the table holds now, sorted.
            state: `RETRYING` or `FAILED`.

        Returns:
            Such as `"2 symbols failed (delete and re-add a row to read one again):
            EntitlementError: HTTP 403 ...: AAPL, MSFT"`, or None when no symbol is in `state`.
        """
        groups: dict[str, list[str]] = {}
        texts: dict[str, str] = {}
        for name in current:
            symbol = result.symbols[name]
            if symbol.state is state:
                cause = symbol.cause or ""
                groups.setdefault(cause, []).append(name)
                texts.setdefault(cause, symbol.error or "")
        if not groups:
            return None
        total = sum(len(names) for names in groups.values())
        parts = []
        for cause in sorted(groups)[:_CAUSES_SHOWN]:
            names = groups[cause]
            shown = ", ".join(names[:_SYMBOLS_SHOWN])
            more = f" (+{len(names) - _SYMBOLS_SHOWN} more)" if len(names) > _SYMBOLS_SHOWN else ""
            text = texts[cause]
            text = text if len(text) <= _CAUSE_SHOWN else text[: _CAUSE_SHOWN - 1] + "…"
            parts.append(f"{text}: {shown}{more}")
        if len(groups) > _CAUSES_SHOWN:
            parts.append(f"{len(groups) - _CAUSES_SHOWN} more causes")
        noun = "symbol" if total == 1 else "symbols"
        if state is SymbolReadState.FAILED:
            head = f"{total} {noun} failed (delete and re-add a row to read one again)"
        else:
            head = f"{total} {noun} retrying after a failure that may pass"
        return f"{head}: {'; '.join(parts)}"

    def _forget_stopped(self, touched: Mapping[int, _Result]) -> None:
        """Forget each result whose blink table has shut down: no event of it matters any more.

        Args:
            touched: The results this batch touched.
        """
        for key, result in touched.items():
            if result.ended and result.read.shut_down and key in self._results:
                del self._results[key]
                with self._lock:
                    self._following.pop(key, None)

    def _exited(self) -> None:
        """The coordinator thread ends (`stop`): forget every result, so none outlives it.

        Their tables are failed or about to fail: `stop` runs at the session's close, after its
        live tables failed, or once a collected session has no result left.
        """
        logger.debug("follow: the coordinator ends with %d results", len(self._results))
        self._results.clear()
        with self._lock:
            self._following.clear()

    def _died(self, death: BaseException) -> None:
        """The coordinator thread died: fail every followed read it served or was handed.

        Runs on the dying thread. Its results would stop following without an error otherwise.
        A `follow` that came before the thread was cleared queued its start to the dead thread,
        so every read handed over is failed, not only those the thread had started; a `follow`
        after it starts a new thread.

        Args:
            death: What ended the thread.
        """
        error = MassiveError(
            library_bug(f"the follow coordinator died ({death!r}); this table no longer updates"),
            cause=ErrorCause.LIBRARY_DEFECT,
        )
        for result in self._results.values():
            result.ended = True
        self._results.clear()  # before the next thread can start: never two owners
        with self._lock:
            reads = list(self._following.values())
            self._following.clear()
            pool = self._pool  # None when a stop came first
            self._worker = self._pool = None
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)
        logger.debug("follow: the coordinator died; failing %d followed reads", len(reads))
        for read in reads:
            read.fail_table(error)


def _ns_text(value: int) -> str:
    """Return an instant in epoch nanoseconds as UTC text for a log line.

    Args:
        value: The instant.

    Returns:
        Such as `"2026-11-02T14:05:12Z"`.
    """
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(value / 1e9))
