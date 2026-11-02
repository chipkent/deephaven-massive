"""The follow coordinator: the one owner of every followed read's per-symbol state in a session.

A `history()` whose `symbols` is a refreshing table follows it (`_tables/followed_read.py`):
each symbol added to the table is read over the call's fixed window and lands in the result
whole, and each symbol removed leaves it. This module decides, for every such result of a
session, which symbol is read when, what its read's outcome does, and what the diagnostics
show. It owns:

- **One coordinator thread per session** (`massive #1 follow`), started by the session's first
  followed read and ended once the session has none (a `QueueWorker`). It takes every event from
  one queue, in order: the symbol table's changes (from the watch's listener, which only
  queues), each read's start and outcome (from the read pool), and each result's stop (from its
  blink table's shutdown). It is the only thread that reads or changes per-symbol state, so that
  state needs no lock. It makes no request and waits on nothing: it publishes each symbol's
  cell (`FollowedRead.publish`, which never waits for an update cycle), times the retries, and
  queues the `m.streams()` and `m.symbol_reads()` rows.
- **One read pool per session**, a `ThreadPoolExecutor` of `MassiveConfig.max_concurrency`
  threads (`massive #1 follow-read_0`), made and ended with the coordinator thread. Each task
  reads some symbols of one result (`FollowPlan.read`) in the session's execution context,
  listening to that result's stop token, and queues the outcome. A REST read reads one symbol;
  a flat-file read reads every symbol waiting at once, in one scan, and a result has at most
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
the coordinator records the failure and publishes it, never the listener.

**Time.** The coordinator reads `CLOCK`: the wall clock for the diagnostics' instants and the
monotonic clock for the retries. Its queue wait ends at the next retry; tests replace `CLOCK`
with a virtual one and wake the coordinator (`wake`) after moving it.
"""

from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Final, TypeAlias

from deephaven_massive._datasets.column_descriptions import TRANSPORT_LABELS
from deephaven_massive._enums import Reread
from deephaven_massive._errors import (
    LIBRARY_BUG,
    ErrorCause,
    MassiveError,
    failure_text,
    library_bug,
)
from deephaven_massive._log_limits import bounded_list, log_failure, mark_failure_logged
from deephaven_massive._massive import Transport
from deephaven_massive._tables import poll
from deephaven_massive._tables.diagnostics import (
    FLAT_FILES_AND_REST,
    NotePart,
    StreamUpdate,
    SymbolReadRow,
    SymbolReadState,
    describe_symbols,
)
from deephaven_massive._tables.execution_context import entered
from deephaven_massive._tables.liveness import QueueWorker
from deephaven_massive._tables.poll import SymbolRowError
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

    Every later change is relative to it.
    """

    read: FollowedRead
    symbols: frozenset[str]


@dataclass(frozen=True, slots=True)
class _Changed:
    """Symbols the table gained and lost in one update cycle."""

    read: FollowedRead
    added: frozenset[str]
    removed: frozenset[str]


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
    transport: Transport | str | None = None


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
    """

    read: FollowedRead
    symbols: dict[str, _Symbol] = field(default_factory=dict)
    queue: list[str] = field(default_factory=list)
    scanning: bool = False
    seq: int = 0
    ended: bool = False
    dirty: set[str] = field(default_factory=set)
    warned: set[str] = field(default_factory=set)
    transports: set[Transport | str] = field(default_factory=set)
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


def _transport(read: FollowedRead, outcome: SymbolOutcome) -> Transport | str:
    """Return the source a symbol's read used, as `m.symbol_reads()` shows it.

    Args:
        read: The result, whose plan names its source.
        outcome: The symbol's outcome.

    Returns:
        The plan's source, or `FLAT_FILES_AND_REST` for flat-file rows some of which REST read.
    """
    if isinstance(outcome, SymbolRows) and outcome.with_rest_days:
        return FLAT_FILES_AND_REST
    return read.plan.transport


def _shown_transport(transports: Iterable[Transport | str]) -> Transport | str | None:
    """Return the `Transport` of a followed read's `m.streams()` row: the sources used so far.

    Args:
        transports: The sources its reads used.

    Returns:
        The one source, `FLAT_FILES_AND_REST` when REST filled some flat-file days, or None
        before any read.
    """
    used = set(transports)
    if FLAT_FILES_AND_REST in used:
        return FLAT_FILES_AND_REST
    return used.pop() if len(used) == 1 else None


class FollowCoordinator:
    """The session's follow coordinator thread and read pool; see the module docstring.

    Created with the session's core, which holds it; it starts no thread until the session's
    first followed read (`follow`) and ends its thread and pool once the session has none, so
    an idle session owns neither. It refers to the core weakly. `follow`, `post`, and `wake`
    are safe from any thread and never block: they take a leaf lock only around a queue put.
    Every handler runs on the coordinator thread.
    """

    __slots__ = (
        "_count",
        "_exec_ctx",
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
        # A leaf lock: guards the thread, the pool, and the count of followed reads.
        self._lock = threading.Lock()
        self._worker: QueueWorker[_Message] | None = None
        self._pool: ThreadPoolExecutor | None = None
        self._count = 0
        # Touched only by the coordinator thread.
        self._results: dict[int, _Result] = {}

    @property
    def thread(self) -> threading.Thread | None:
        """The coordinator thread, or None while the session has no followed read."""
        with self._lock:
            return None if self._worker is None else self._worker.thread

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
                    on_death=self._died,
                )
            started = self._count == 0
            self._count += 1
            self._worker.put(_Follow(read, first))
        if started:
            logger.debug("%s follow: the coordinator started", self._thread_prefix)

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

    def baseline(self, read: FollowedRead, symbols: frozenset[str]) -> None:
        """Queue the symbol table's set of symbols when its watch started.

        Args:
            read: The result.
            symbols: The set.
        """
        self.post(_Baseline(read, symbols))

    def changed(self, read: FollowedRead, added: frozenset[str], removed: frozenset[str]) -> None:
        """Queue one change of the symbol table's set (the watch's listener calls it).

        Args:
            read: The result.
            added: The symbols gained.
            removed: The symbols lost.
        """
        self.post(_Changed(read, added, removed))

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

    def stop(self, read: FollowedRead) -> None:
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
                self._apply(result, message)
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
        result = _Result(read)
        self._results[id(read)] = result
        first = message.first
        if first is not None:
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
                _ns_text(symbol.read_at),
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
            result.read.publish(name, result.seq, None)
            symbol.kept = False
            symbol.read_at = None
            symbol.rows = 0

    def _enqueue(self, result: _Result, name: str) -> None:
        """Put a symbol in its result's queue of symbols waiting for a read, once.

        Args:
            result: The result's state.
            name: The symbol.
        """
        symbol = result.symbols[name]
        if not symbol.waiting:
            symbol.waiting = True
            result.queue.append(name)

    def _landed(self, result: _Result, message: _Outcome) -> None:
        """Apply a read's outcome: each symbol whose generation still matches takes it.

        Args:
            result: The result's state.
            message: The outcome.
        """
        if result.read.plan.one_scan:
            result.scanning = False
        for name, generation in message.generations.items():
            symbol = result.symbols[name]
            if symbol.reading == generation:
                symbol.reading = None
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
        label = TRANSPORT_LABELS[transport] if isinstance(transport, Transport) else transport
        if isinstance(outcome, SymbolRows):
            if publish:
                result.seq += 1
                read.publish(name, result.seq, outcome.table)
            symbol.state = SymbolReadState.DONE
            symbol.kept = True
            symbol.rows = outcome.table.size
            symbol.read_at = began_at
            symbol.error = symbol.cause = None
            symbol.due = symbol.next_attempt_at = None
            logger.debug(
                "call %d %s: %d rows in %.1f s from %s",
                read.call,
                name,
                symbol.rows,
                seconds,
                label,
            )
            return
        self._failed(result, name, outcome)

    def _failed(self, result: _Result, name: str, outcome: SymbolFailure) -> None:
        """Make a failed symbol `RETRYING` or `FAILED`, and log it.

        Args:
            result: The result's state.
            name: The symbol.
            outcome: Its failure.
        """
        symbol = result.symbols[name]
        error = outcome.error
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
        if error.cause is not ErrorCause.EXPECTED and first:
            level = logging.ERROR
        logger.log(
            level,
            "%s call %d: %s failed after %d attempts (%s), and is left out of the result; "
            "delete and re-add its row to read it again: %s",
            result.read.dataset,
            result.read.call,
            name,
            attempts,
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
        """Start the reads the result's queue allows: each REST symbol, or one flat-file scan.

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
        batches = [ready] if result.read.plan.one_scan else [[name] for name in ready]
        if result.read.plan.one_scan:
            result.scanning = True
        for batch in batches:
            generations = {name: result.symbols[name].generation for name in batch}
            for name, generation in generations.items():
                result.symbols[name].reading = generation
            self._submit(result.read, generations)

    def _submit(self, read: FollowedRead, generations: Mapping[str, int]) -> None:
        """Hand one read to the pool.

        Args:
            read: The result.
            generations: The symbols to read, at their generations.
        """
        with self._lock:
            pool = self._pool
        if pool is not None:  # the coordinator thread runs only while its pool does
            pool.submit(self._read, read, dict(generations))

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
        """Forget each stopped result, and end the thread and pool once none is left.

        Args:
            touched: The results this batch touched.
        """
        for key, result in touched.items():
            if result.ended and result.read.shut_down and key in self._results:
                del self._results[key]
                self._release_one()

    def _release_one(self) -> None:
        """Count one followed read less; end the thread and pool with the last."""
        with self._lock:
            self._count -= 1
            if self._count:
                return
            worker, pool = self._worker, self._pool
            self._worker = self._pool = None
        if worker is not None:
            worker.stop()
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)
        logger.debug("%s follow: the coordinator ended: no followed read left", self._thread_prefix)

    def _died(self, death: BaseException) -> None:
        """The coordinator thread died: fail every followed read it served.

        Runs on the dying thread. Its results would stop following without an error otherwise.

        Args:
            death: What ended the thread.
        """
        error = MassiveError(
            library_bug(f"the follow coordinator died ({death!r}); this table no longer updates"),
            cause=ErrorCause.LIBRARY_DEFECT,
        )
        results = list(self._results.values())
        self._results.clear()
        with self._lock:
            self._count = 0
            pool = self._pool
            self._worker = self._pool = None
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)
        for result in results:
            result.ended = True
            result.read.fail_table(error)


def _ns_text(value: int | None) -> str:
    """Return an instant in epoch nanoseconds as UTC text for a log line, or `"none"`.

    Args:
        value: The instant, or None.

    Returns:
        Such as `"2026-11-02T14:05:12Z"`.
    """
    if value is None:
        return "none"
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(value / 1e9))
